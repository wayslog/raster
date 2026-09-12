//! Native child process interruption and independent discarding unsynchronized state model,Verify recovery results individually.
use super::*;
use crate::engine::checkpoint_tests::power::{Change, DurableModel};
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
        .unwrap_or_else(|| "root".into());
    if name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
        "token directory".into()
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
                        format!("open:{name}")
                    },
                    Some(name),
                )
            }
            IoOperation::Write { file, .. } => (name(file).into_event("write"), None),
            IoOperation::Read { file, .. } => (name(file).into_event("read"), None),
            IoOperation::SyncFile { file, .. } => (name(file).into_event("sync_file"), None),
            IoOperation::Close(file) => (name(file).into_event("close"), None),
            IoOperation::CreateDirectory(path) if path.starts_with("checkpoints") => {
                (format!("Create directory:{}", directory_name(path)), None)
            }
            IoOperation::SyncDirectory(path)
                if path.as_os_str().is_empty() || path.starts_with("checkpoints") =>
            {
                (
                    format!("Synchronize directories:{}", directory_name(path)),
                    None,
                )
            }
            IoOperation::Rename { destination, .. } if destination.starts_with("checkpoints") => {
                ("post commit".into(), None)
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
            let (event, opened, change) = state
                .pending
                .remove(&completion.id.0)
                .expect("Completed registration");
            if let (Some(name), Ok(IoOutcome::Opened(file))) = (opened, &completion.result) {
                state.files.insert(file.slot, name);
            }
            assert!(completion.result.is_ok(), "Native device completion failed");
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
fn checkpoint_crash_child_process_entry() {
    let Some(root) = std::env::var_os("RASTER_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let stop: usize = std::env::var("RASTER_CRASH_STOP").unwrap().parse().unwrap();
    let state = Arc::new(Mutex::new(CrashState {
        root: root.clone(),
        power_root: root.with_extension("power_loss"),
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
    // Test coordination information is only available to the parent process to select a known recovery set,Not submitting evidence as an engine.
    let seed: Vec<_> = store
        .id()
        .0
        .into_iter()
        .chain(session.id().0)
        .chain(first.token.0)
        .collect();
    std::fs::write(root.join("test_identity"), seed).unwrap();
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
    let kind = match std::env::var("RASTER_CRASH_KIND").unwrap().as_str() {
        "Full" => CheckpointKind::Full,
        "Index" => CheckpointKind::Index,
        "Log" => CheckpointKind::Log,
        _ => panic!("Unknown checkpoint test type"),
    };
    let ticket = store.maintenance().checkpoint(kind).unwrap();
    wait(&mut session, &ticket);
    {
        let mut state = state.lock().unwrap();
        state.armed = false;
        state.durability.materialize(&state.power_root);
        std::fs::write(root.join("test event"), state.events.join("\n")).unwrap();
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    println!("Checkpoint crash baseline completed");
}
fn child(root: &std::path::Path, stop: usize, kind: &str) -> std::process::Output {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "engine::checkpoint_tests::safety::crash::checkpoint_crash_child_process_entry",
            "--nocapture",
        ])
        .env("RASTER_CRASH_ROOT", root)
        .env("RASTER_CRASH_STOP", stop.to_string())
        .env("RASTER_CRASH_KIND", kind)
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
                "Interrupt test child process timeout:{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn verify(root: &std::path::Path) -> (usize, usize) {
    let seed = std::fs::read(root.join("test_identity")).unwrap();
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
        let manifest = committed.then(|| {
            Commit::decode(&std::fs::read(path.join("commit")).unwrap())
                .unwrap()
                .verify(&std::fs::read(path.join("manifest")).unwrap())
                .unwrap()
        });
        let result = recover_store(
            config(root.to_path_buf()),
            crate::api::maintenance::RecoverySet {
                store,
                index: manifest.as_ref().map_or(token, |m| m.base_index),
                log: token,
            },
        );
        if !committed {
            assert!(
                result.is_err(),
                "Uncommitted directories cannot be restored"
            );
            rejected += 1;
            continue;
        }
        let manifest = manifest.unwrap();
        if manifest.kind == Kind::Index {
            assert!(
                result.is_err(),
                "Index-only commits may not be independently restored"
            );
            assert!(manifest.session_progress.is_empty());
            assert_eq!(manifest.materials.len(), 1);
            let material = &manifest.materials[0];
            assert_eq!(material.kind, Kind::Index);
            let name = crate::storage::SegmentedStorage::checkpoint_material_name(
                material.id,
                material.generation,
            );
            let bytes = std::fs::read(path.join(name)).unwrap();
            material.verify(&bytes).unwrap();
            IndexSnapshot::decode(&bytes).unwrap();
            accepted += 1;
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
    assert!(
        old_recovered,
        "Each interruption point must still be able to restore the old generation"
    );
    (accepted, rejected)
}
#[test]
fn only_full_commits_are_accepted_after_each_checkpoint_completion_step_interrupts_the_process_and_the_old_generation_is_recoverable()
 {
    matrix("Full");
}
#[test]
fn only_index_checkpoint_interrupt_matrix_does_not_generate_session_persistence_commitment() {
    matrix("Index");
}
#[test]
fn only_log_checkpoint_interrupt_matrix_always_binds_existing_index_recovery() {
    matrix("Log");
}
fn matrix(kind: &str) {
    let parent = Directory(std::env::temp_dir().join(format!(
        "raster-crash-{:x?}",
        StoreId::generate().unwrap().0
    )));
    std::fs::create_dir(&parent.0).unwrap();
    let baseline = parent.0.join("baseline");
    let output = child(&baseline, usize::MAX, kind);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Checkpoint crash baseline completed")
    );
    let events = std::fs::read_to_string(baseline.join("test event")).unwrap();
    for required in [
        "write:owner",
        "sync_file:owner",
        "write:manifest",
        "sync_file:manifest",
        "write:commit.pending",
        "sync_file:commit.pending",
        "post commit",
        "Synchronize directories:token directory",
    ] {
        assert!(
            events.lines().any(|line| line == required),
            "missing stage {required}"
        );
    }
    assert!(events.contains(".material"));
    let count = events.lines().count();
    assert_eq!(verify(&baseline), (2, 0));
    assert_eq!(verify_power(&baseline), (2, 0));
    let mut rejected = 0;
    let mut published = 0;
    for step in 0..=count {
        let root = parent.0.join(format!("interrupt-{step}"));
        let output = child(&root, step, kind);
        assert_eq!(
            output.status.code(),
            Some(77),
            "miss step {step}:{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let (accepted, failed) = verify(&root);
        let (power_accepted, _) = verify_power(&root);
        assert_eq!(
            power_accepted,
            if step == count { 2 } else { 1 },
            "Only the commits that have been synchronized in the final directory will be retained after a power outage.,step {step}"
        );
        rejected += failed;
        published += usize::from(accepted == 2);
    }
    assert!(
        rejected > 0 && published > 0,
        "Must cover before submission and after publishing"
    );
    println!(
        "{kind} Checkpoint process interruption coverage {count} a I/O Completion steps and pre-launch boundaries"
    );
}

fn verify_power(root: &std::path::Path) -> (usize, usize) {
    let image = root.with_extension("power_loss");
    std::fs::copy(root.join("test_identity"), image.join("test_identity")).unwrap();
    verify(&image)
}
