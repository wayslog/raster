//! 原生子进程中断与独立的丢弃未同步状态模型，分别验证恢复结果。
use super::*;
#[path = "checkpoint_power_model.rs"]
mod power;
use power::{Change, DurableModel};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
struct CrashState {
    armed: bool,
    stop: usize,
    files: BTreeMap<u64, String>,
    pending: BTreeMap<u64, (String, Option<String>, Change)>,
    root: PathBuf,
    power_root: PathBuf,
    durability: DurableModel,
    events: Vec<String>,
}
struct CrashDevice {
    inner: Box<dyn Device>,
    state: Arc<Mutex<CrashState>>,
}
struct CrashFactory(Arc<Mutex<CrashState>>);
impl DeviceFactory for CrashFactory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(CrashDevice {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }
            .open(options)?,
            state: self.0.clone(),
        }))
    }
}
fn directory_name(path: &std::path::Path) -> String {
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "根".into());
    if name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
        "令牌目录".into()
    } else {
        name
    }
}
impl Device for CrashDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let mut state = self.state.lock().unwrap();
        let name = |file: &FileId| state.files.get(&file.slot).cloned().unwrap_or_default();
        let (event, opened) = match &request.operation {
            IoOperation::Open { path, .. } => {
                let name = if path.starts_with("checkpoints") {
                    path.file_name().unwrap().to_string_lossy().into_owned()
                } else {
                    String::new()
                };
                (
                    if name.is_empty() {
                        String::new()
                    } else {
                        format!("打开:{name}")
                    },
                    Some(name),
                )
            }
            IoOperation::Write { file, .. } => (name(file).into_event("写"), None),
            IoOperation::Read { file, .. } => (name(file).into_event("读"), None),
            IoOperation::SyncFile { file, .. } => (name(file).into_event("同步文件"), None),
            IoOperation::Close(file) => (name(file).into_event("关闭"), None),
            IoOperation::CreateDirectory(path) if path.starts_with("checkpoints") => {
                (format!("创建目录:{}", directory_name(path)), None)
            }
            IoOperation::SyncDirectory(path)
                if path.as_os_str().is_empty() || path.starts_with("checkpoints") =>
            {
                (format!("同步目录:{}", directory_name(path)), None)
            }
            IoOperation::Rename { destination, .. } if destination.starts_with("checkpoints") => {
                ("发布提交".into(), None)
            }
            _ => (String::new(), None),
        };
        let change = Change::from_operation(&request.operation);
        let id = self.inner.submit(request)?;
        state.pending.insert(id.0, (event, opened, change));
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
        let start = out.len();
        self.inner.poll(budget, out)?;
        let mut state = self.state.lock().unwrap();
        for completion in &out[start..] {
            let (event, opened, change) =
                state.pending.remove(&completion.id.0).expect("完成已登记");
            if let (Some(name), Ok(IoOutcome::Opened(file))) = (opened, &completion.result) {
                state.files.insert(file.slot, name);
            }
            assert!(completion.result.is_ok(), "原生设备完成失败");
            let root = state.root.clone();
            state.durability.apply(&root, change, completion);
            if state.armed && !event.is_empty() {
                state.events.push(event);
                if state.events.len() == state.stop {
                    state.durability.materialize(&state.power_root);
                    std::process::exit(77);
                }
            }
        }
        Ok(())
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}
trait EventName {
    fn into_event(self, prefix: &str) -> String;
}
impl EventName for String {
    fn into_event(self, prefix: &str) -> String {
        if self.is_empty() {
            self
        } else {
            format!("{prefix}:{self}")
        }
    }
}
fn config(root: PathBuf) -> Config {
    let mut config = Config::default();
    config.storage.root = root;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config
}
#[test]
fn 检查点崩溃子进程入口() {
    let Some(root) = std::env::var_os("RASTER_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let stop: usize = std::env::var("RASTER_CRASH_STOP").unwrap().parse().unwrap();
    let state = Arc::new(Mutex::new(CrashState {
        root: root.clone(),
        power_root: root.with_extension("掉电"),
        ..Default::default()
    }));
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config(root.clone()))
        .device(Box::new(CrashFactory(state.clone())))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 10, 7);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let first = wait(&mut session, &ticket);
    // 测试协调信息只供父进程选择已知恢复集，不作为引擎提交证据。
    let seed: Vec<_> = store
        .id()
        .0
        .into_iter()
        .chain(session.id().0)
        .chain(first.token.0)
        .collect();
    std::fs::write(root.join("测试身份"), seed).unwrap();
    put(&mut session, 30, 9);
    {
        let mut state = state.lock().unwrap();
        state.armed = true;
        state.stop = stop;
    }
    if stop == 0 {
        let state = state.lock().unwrap();
        state.durability.materialize(&state.power_root);
        std::process::exit(77);
    }
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    wait(&mut session, &ticket);
    {
        let mut state = state.lock().unwrap();
        state.armed = false;
        state.durability.materialize(&state.power_root);
        std::fs::write(root.join("测试事件"), state.events.join("\n")).unwrap();
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    println!("检查点崩溃基线完成");
}
fn child(root: &std::path::Path, stop: usize) -> std::process::Output {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "engine::checkpoint_tests::safety::crash::检查点崩溃子进程入口",
            "--nocapture",
        ])
        .env("RASTER_CRASH_ROOT", root)
        .env("RASTER_CRASH_STOP", stop.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if process.try_wait().unwrap().is_some() {
            return process.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            process.kill().unwrap();
            let output = process.wait_with_output().unwrap();
            panic!(
                "中断测试子进程超时：{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn verify(root: &std::path::Path) -> (usize, usize) {
    let seed = std::fs::read(root.join("测试身份")).unwrap();
    assert_eq!(seed.len(), 48);
    let store = StoreId(seed[..16].try_into().unwrap());
    let session = SessionId(seed[16..32].try_into().unwrap());
    let first = CheckpointToken(seed[32..].try_into().unwrap());
    let mut accepted = 0;
    let mut rejected = 0;
    let mut old_recovered = false;
    for entry in std::fs::read_dir(root.join("checkpoints")).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_dir() {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let token = CheckpointToken(std::array::from_fn(|i| {
            u8::from_str_radix(&name[i * 2..i * 2 + 2], 16).unwrap()
        }));
        let committed = path.join("commit").exists();
        let result = recover_store(
            config(root.to_path_buf()),
            crate::api::maintenance::RecoverySet {
                store,
                index: token,
                log: token,
            },
        );
        if !committed {
            assert!(result.is_err(), "未提交目录不能恢复");
            rejected += 1;
            continue;
        }
        let (restored, report) = result.unwrap();
        let old = token == first;
        old_recovered |= old;
        assert_eq!(report.sessions[0].serial, Serial(if old { 10 } else { 30 }));
        let mut session = restored.continue_session(session).unwrap().session;
        assert_eq!(read_value(&mut session, 40, 7), Some(7));
        assert_eq!(
            read_value(&mut session, 50, 9),
            if old { None } else { Some(9) }
        );
        session.close(deadline()).unwrap();
        drop(session);
        restored.shutdown(deadline()).unwrap();
        accepted += 1;
    }
    assert!(old_recovered, "每个中断点必须仍能恢复旧代");
    (accepted, rejected)
}
#[test]
fn 每个检查点完成步骤中断进程后只接受完整提交且旧代可恢复() {
    let parent = Directory(std::env::temp_dir().join(format!(
        "raster-crash-{:x?}",
        StoreId::generate().unwrap().0
    )));
    std::fs::create_dir(&parent.0).unwrap();
    let baseline = parent.0.join("基线");
    let output = child(&baseline, usize::MAX);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("检查点崩溃基线完成"));
    let events = std::fs::read_to_string(baseline.join("测试事件")).unwrap();
    for required in [
        "写:owner",
        "同步文件:owner",
        "写:manifest",
        "同步文件:manifest",
        "写:commit.pending",
        "同步文件:commit.pending",
        "发布提交",
        "同步目录:令牌目录",
    ] {
        assert!(
            events.lines().any(|line| line == required),
            "缺少阶段 {required}"
        );
    }
    assert!(events.contains(".material"));
    let count = events.lines().count();
    assert_eq!(verify(&baseline), (2, 0));
    assert_eq!(verify_power(&baseline), (2, 0));
    let mut rejected = 0;
    let mut published = 0;
    for step in 0..=count {
        let root = parent.0.join(format!("中断-{step}"));
        let output = child(&root, step);
        assert_eq!(
            output.status.code(),
            Some(77),
            "未命中步骤 {step}：{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let (accepted, failed) = verify(&root);
        let (power_accepted, _) = verify_power(&root);
        assert_eq!(
            power_accepted,
            if step == count { 2 } else { 1 },
            "掉电只保留最终目录同步过的提交，步骤 {step}"
        );
        rejected += failed;
        published += usize::from(accepted == 2);
    }
    assert!(rejected > 0 && published > 0, "必须覆盖提交之前与发布之后");
    println!("检查点进程中断覆盖 {count} 个 I/O 完成步骤及启动前边界");
}

fn verify_power(root: &std::path::Path) -> (usize, usize) {
    let image = root.with_extension("掉电");
    std::fs::copy(root.join("测试身份"), image.join("测试身份")).unwrap();
    verify(&image)
}
