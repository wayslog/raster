//! 每个释放变更完成后中断进程，并用丢弃未同步目录状态的模型复查恢复安全。
use super::*;
use crate::engine::checkpoint_tests::power::{Change, DurableModel};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct State {
    root: PathBuf,
    image: PathBuf,
    target: Option<PathBuf>,
    stop: usize,
    events: Vec<String>,
    pending: std::collections::BTreeMap<IoId, (Change, Option<String>)>,
    model: DurableModel,
}
struct Factory(Arc<Mutex<State>>);
struct Crash {
    inner: Box<dyn Device>,
    state: Arc<Mutex<State>>,
}
impl DeviceFactory for Factory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(Crash {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }
            .open(options)?,
            state: self.0.clone(),
        }))
    }
}
impl Device for Crash {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let mut state = self.state.lock().unwrap();
        let event = state
            .target
            .as_ref()
            .and_then(|target| match &request.operation {
                IoOperation::Rename {
                    source,
                    destination,
                } if source == &target.join("commit")
                    && destination == &target.join("commit.released") =>
                {
                    Some("失效重命名".to_owned())
                }
                IoOperation::SyncDirectory(path) if path == target => {
                    Some("同步失效或删除目录".to_owned())
                }
                IoOperation::RemoveFile(path) if path.parent() == Some(target.as_path()) => Some(
                    format!("删除材料:{}", path.file_name().unwrap().to_str().unwrap()),
                ),
                _ => None,
            });
        let change = Change::from_operation(&request.operation);
        let id = self.inner.submit(request)?;
        state.pending.insert(id, (change, event));
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        self.inner.poll(budget, output)?;
        let mut state = self.state.lock().unwrap();
        for completion in output.iter() {
            let (change, event) = state.pending.remove(&completion.id).unwrap();
            assert!(
                completion.result.is_ok(),
                "原生释放完成失败：{:?}",
                completion.result
            );
            let root = state.root.clone();
            state.model.apply(&root, change, completion);
            if let Some(event) = event {
                state.events.push(event);
                if state.events.len() == state.stop {
                    state.model.materialize(&state.image);
                    std::process::exit(81);
                }
            }
        }
        Ok(())
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
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
fn 释放崩溃子进程入口() {
    let Some(root) = std::env::var_os("RASTER_RELEASE_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let stop = std::env::var("RASTER_RELEASE_CRASH_STOP")
        .unwrap()
        .parse()
        .unwrap();
    let state = Arc::new(Mutex::new(State {
        root: root.clone(),
        image: root.with_extension("掉电"),
        stop,
        ..Default::default()
    }));
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config(root.clone()))
        .device(Box::new(Factory(state.clone())))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let old = checkpoint(&store, &mut session, CheckpointKind::Full);
    put(&mut session, 1, 9);
    let newest = checkpoint(&store, &mut session, CheckpointKind::Full);
    let identity: Vec<_> = store
        .id()
        .0
        .into_iter()
        .chain(old.token.0)
        .chain(newest.token.0)
        .collect();
    std::fs::write(root.join("测试身份"), identity).unwrap();
    {
        let mut state = state.lock().unwrap();
        state.target = Some(
            store
                .inner
                .storage
                .checkpoint_path(old.token, "commit")
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        );
        if stop == 0 {
            state.model.materialize(&state.image);
            std::process::exit(81);
        }
    }
    release(&store, &mut session, old.token)
        .as_ref()
        .as_ref()
        .unwrap();
    {
        let state = state.lock().unwrap();
        state.model.materialize(&state.image);
        std::fs::write(root.join("释放事件"), state.events.join("\n")).unwrap();
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
fn child(root: &std::path::Path, stop: usize) -> std::process::Output {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "engine::checkpoint_tests::checkpoint_release::crash::释放崩溃子进程入口",
            "--nocapture",
        ])
        .env("RASTER_RELEASE_CRASH_ROOT", root)
        .env("RASTER_RELEASE_CRASH_STOP", stop.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= until {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "释放中断子进程超时：{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn verify(root: &std::path::Path, identity: &[u8]) -> bool {
    assert_eq!(identity.len(), 48);
    let store_id = StoreId(identity[..16].try_into().unwrap());
    let old = CheckpointToken(identity[16..32].try_into().unwrap());
    let newest = CheckpointToken(identity[32..].try_into().unwrap());
    let old_dir = root
        .join("checkpoints")
        .join(old.0.iter().map(|b| format!("{b:02x}")).collect::<String>());
    let committed = old_dir.join("commit").exists();
    let result = recover_store(
        config(root.to_path_buf()),
        RecoverySet {
            store: store_id,
            index: old,
            log: old,
        },
    );
    if committed {
        let (store, _) = result.expect("可见 commit 对应的材料必须完整可恢复");
        let mut session = store.start_session(Default::default()).unwrap();
        assert_eq!(read_value(&mut session, 2, 7), Some(7));
        assert_eq!(read_value(&mut session, 3, 9), None);
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    } else {
        assert!(result.is_err());
        assert!(old_dir.join("commit.released").exists());
    }
    let newest_set = RecoverySet {
        store: store_id,
        index: newest,
        log: newest,
    };
    let (store, _) = recover_store(config(root.to_path_buf()), newest_set.clone()).unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert_eq!(read_value(&mut session, 2, 7), Some(7));
    assert_eq!(read_value(&mut session, 3, 9), Some(9));
    let report = release(&store, &mut session, old);
    assert_eq!(
        report.as_ref().as_ref().unwrap().retirement,
        CheckpointRetirement::Retired
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    let (reader, _) = recover_store(config(root.to_path_buf()), newest_set).unwrap();
    reader.shutdown(deadline()).unwrap();
    committed
}
#[test]
fn 每个失效和删除步骤中断后保留集合可恢复且释放可跨重启接续() {
    let parent = Directory(std::env::temp_dir().join(format!(
        "raster-release-crash-{:x?}",
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
    let events = std::fs::read_to_string(baseline.join("释放事件")).unwrap();
    let events: Vec<_> = events.lines().collect();
    assert_eq!(events[0], "失效重命名");
    assert_eq!(events[1], "同步失效或删除目录");
    assert!(events.iter().any(|event| event.starts_with("删除材料:")));
    let (pairs, remainder) = events[2..].as_chunks::<2>();
    assert!(remainder.is_empty());
    assert!(pairs.iter().all(|pair| {
        pair[0].starts_with("删除材料:") && pair[1] == "同步失效或删除目录"
    }));
    let identity = std::fs::read(baseline.join("测试身份")).unwrap();
    assert!(!verify(&baseline, &identity));
    assert!(!verify(&baseline.with_extension("掉电"), &identity));
    for step in 0..=events.len() {
        let root = parent.0.join(format!("中断-{step}"));
        let output = child(&root, step);
        assert_eq!(
            output.status.code(),
            Some(81),
            "步骤 {step}：{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let identity = std::fs::read(root.join("测试身份")).unwrap();
        let native_committed = verify(&root, &identity);
        let power_committed = verify(&root.with_extension("掉电"), &identity);
        assert_eq!(native_committed, step == 0);
        assert_eq!(
            power_committed,
            step < 2,
            "失效目录同步前不能删除可见提交的材料"
        );
    }
}
