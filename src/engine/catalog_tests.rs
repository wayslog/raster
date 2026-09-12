//! Checkpoint release and recovery are coordinated through the same directory lock;Use native device at pause point,Does not replace disk semantics.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
#[derive(Default)]
struct Control {
    attempts: AtomicUsize,
    pause_namespace: AtomicBool,
    namespace_blocked: AtomicBool,
    pause_reads: AtomicBool,
    read_blocked: AtomicBool,
    pause_publish: AtomicBool,
    publish_blocked: AtomicBool,
}
struct Factory(Arc<Control>);
struct Controlled {
    inner: Box<dyn Device>,
    control: Arc<Control>,
}
impl DeviceFactory for Factory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(Controlled {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }
            .open(options)?,
            control: self.0.clone(),
        }))
    }
}
impl Device for Controlled {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let lock = matches!(request.operation, IoOperation::TryLock { .. });
        if self.control.pause_namespace.load(Ordering::SeqCst)
            && matches!(&request.operation, IoOperation::CreateDirectory(path) if path.starts_with("checkpoints"))
        {
            self.control.namespace_blocked.store(true, Ordering::SeqCst);
            return Err(RejectedIo {
                request,
                reason: Error::Busy,
            });
        }

        if self.control.pause_reads.load(Ordering::SeqCst)
            && matches!(request.operation, IoOperation::Read { .. })
        {
            self.control.read_blocked.store(true, Ordering::SeqCst);
            return Err(RejectedIo {
                request,
                reason: Error::Busy,
            });
        }
        if self.control.pause_publish.load(Ordering::SeqCst)
            && matches!(request.operation, IoOperation::Rename { .. })
        {
            self.control.publish_blocked.store(true, Ordering::SeqCst);
            return Err(RejectedIo {
                request,
                reason: Error::Busy,
            });
        }
        let id = self.inner.submit(request)?;
        if lock {
            self.control.attempts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        self.inner.poll(budget, output)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}
fn external(store: &RasterKV<Schema>) -> Box<dyn Device> {
    device::thread_pool::ThreadPoolDeviceFactory {
        workers: 1,
        queue_capacity: 8,
    }
    .open(DeviceOpenOptions {
        root: store.inner.config.storage.root.clone(),
        create_new: false,
    })
    .unwrap()
}
fn execute(device: &dyn Device, operation: IoOperation) -> Result<IoOutcome, Error> {
    let route = CompletionRoute(71);
    let id = device.submit(IoRequest { route, operation }).unwrap();
    let until = deadline();
    loop {
        assert!(!until.expired(), "Directory quorum device wait timeout");
        let mut output = Vec::new();
        device.poll(PollBudget::default(), &mut output).unwrap();
        if let Some(done) = output.pop() {
            assert!(output.is_empty());
            assert_eq!(done.id, id);
            assert_eq!(done.route, route);
            assert!(done.buffer.is_none());
            return done.result;
        }
        std::thread::yield_now();
    }
}
fn try_lock(device: &dyn Device, mode: FileLockMode) -> Result<FileId, Error> {
    match execute(
        device,
        IoOperation::TryLock {
            path: "checkpoint.lock".into(),
            mode,
        },
    )? {
        IoOutcome::Locked(file) => Ok(file),
        _ => panic!("directory lock completion error"),
    }
}
fn lock(device: &dyn Device, mode: FileLockMode) -> FileId {
    let until = deadline();
    loop {
        match try_lock(device, mode) {
            Ok(file) => return file,
            Err(Error::Busy) => {
                assert!(
                    !until.expired(),
                    "Expected available directory lock not released"
                );
                std::thread::yield_now();
            }
            Err(error) => panic!("Lock failed:{error}"),
        }
    }
}
fn close(device: &dyn Device, file: FileId) {
    assert!(matches!(
        execute(device, IoOperation::Close(file)).unwrap(),
        IoOutcome::Done
    ));
}
fn progress_until(
    store: &RasterKV<Schema>,
    session: &mut Session<Schema>,
    ready: impl Fn() -> bool,
) {
    let until = deadline();
    while !ready() {
        assert!(
            !until.expired(),
            "Directory arbitration pause point not reached"
        );
        session.poll(PollBudget::default()).unwrap();
        store.maintenance().poll(PollBudget::default()).unwrap();
        std::thread::yield_now();
    }
}
#[test]
fn exclusive_directory_lock_blocks_checkpoint_and_issuing_shared_lock_overwrites_commit_rename() {
    let control = Arc::new(Control::default());
    let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let guard = external(&store);
    let held = lock(&*guard, FileLockMode::Exclusive);
    control.pause_publish.store(true, Ordering::SeqCst);
    control.pause_namespace.store(true, Ordering::SeqCst);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    progress_until(&store, &mut session, || {
        control.attempts.load(Ordering::SeqCst) >= 2
    });
    assert!(ticket.try_report().unwrap().is_none());
    assert_eq!(
        store.inner.coordinator.snapshot().unwrap().phase,
        crate::coordination::Phase::WaitFlush
    );
    close(&*guard, held);
    progress_until(&store, &mut session, || {
        control.namespace_blocked.load(Ordering::SeqCst)
    });
    assert!(
        matches!(try_lock(&*guard, FileLockMode::Exclusive), Err(Error::Busy)),
        "Shared locks must be obtained before creating a namespace"
    );
    control.pause_namespace.store(false, Ordering::SeqCst);
    progress_until(&store, &mut session, || {
        control.publish_blocked.load(Ordering::SeqCst)
    });
    assert!(matches!(
        try_lock(&*guard, FileLockMode::Exclusive),
        Err(Error::Busy)
    ));
    let concurrent_reader = lock(&*guard, FileLockMode::Shared);
    close(&*guard, concurrent_reader);
    control.pause_publish.store(false, Ordering::SeqCst);
    wait(&mut session, &ticket);
    let available = lock(&*guard, FileLockMode::Exclusive);
    close(&*guard, available);
    guard.shutdown(deadline()).unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn resume_reading_holds_a_shared_directory_lock_and_releases_it_before_the_instance_is_released() {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    };
    session.close(deadline()).unwrap();
    let config = store.inner.config.clone();
    let control = Arc::new(Control::default());
    control.pause_reads.store(true, Ordering::SeqCst);
    let child_control = control.clone();
    let child = std::thread::spawn(move || {
        RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(Factory(child_control)))
            .recover(set)
    });
    let until = deadline();
    while !control.read_blocked.load(Ordering::SeqCst) {
        assert!(
            !until.expired() && !child.is_finished(),
            "Resume has not reached the material reading pause point"
        );
        std::thread::yield_now();
    }
    let guard = external(&store);
    assert!(matches!(
        try_lock(&*guard, FileLockMode::Exclusive),
        Err(Error::Busy)
    ));
    let shared = lock(&*guard, FileLockMode::Shared);
    close(&*guard, shared);
    let mut releaser = store.start_session(Default::default()).unwrap();
    let blocked = store
        .maintenance()
        .release_checkpoint(report.token)
        .unwrap();
    let result = releaser.wait_maintenance(&blocked, deadline()).unwrap();
    assert!(
        matches!(&*result, Err(Error::CheckpointReleaseFailed { cause, .. }) if matches!(&**cause, Error::Busy))
    );
    control.pause_reads.store(false, Ordering::SeqCst);
    let (restored, _) = child.join().unwrap().unwrap();
    let release = store
        .maintenance()
        .release_checkpoint(report.token)
        .unwrap();
    let result = releaser.wait_maintenance(&release, deadline()).unwrap();
    assert_eq!(
        result.as_ref().as_ref().unwrap().retirement,
        CheckpointRetirement::Retired
    );
    releaser.close(deadline()).unwrap();
    let available = lock(&*guard, FileLockMode::Exclusive);
    let mut session = restored.start_session(Default::default()).unwrap();
    assert_eq!(read_value(&mut session, 0, 7), Some(7));
    session.close(deadline()).unwrap();
    close(&*guard, available);
    guard.shutdown(deadline()).unwrap();
    restored.shutdown(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn if_the_base_index_fails_while_waiting_for_the_directory_lock_the_log_checkpoint_refuses_to_be_released()
 {
    let control = Arc::new(Control::default());
    let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let anchor = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let base = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Index)
            .unwrap(),
    );
    let release_control = Arc::new(Control::default());
    let (other, _) = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(store.inner.config.clone())
        .device(Box::new(Factory(release_control.clone())))
        .recover(crate::api::maintenance::RecoverySet {
            store: store.id(),
            index: anchor.token,
            log: anchor.token,
        })
        .unwrap();
    let mut releaser = other.start_session(Default::default()).unwrap();
    release_control.pause_reads.store(true, Ordering::SeqCst);
    let release = other.maintenance().release_checkpoint(base.token).unwrap();
    progress_until(&other, &mut releaser, || {
        release_control.read_blocked.load(Ordering::SeqCst)
    });
    let before = control.attempts.load(Ordering::SeqCst);
    let ticket = store.maintenance().checkpoint(CheckpointKind::Log).unwrap();
    progress_until(&store, &mut session, || {
        control.attempts.load(Ordering::SeqCst) >= before + 2
    });
    release_control.pause_reads.store(false, Ordering::SeqCst);
    let result = releaser.wait_maintenance(&release, deadline()).unwrap();
    assert_eq!(
        result.as_ref().as_ref().unwrap().retirement,
        CheckpointRetirement::Retired
    );
    let result = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*result, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound)
    );
    let committed = std::fs::read_dir(store.inner.config.storage.root.join("checkpoints"))
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().join("commit").exists())
        .count();
    assert_eq!(
        committed, 1,
        "Keep only the original complete set,Cannot publish log checkpoint missing base index"
    );
    let guard = external(&store);
    assert!(matches!(
        try_lock(&*guard, FileLockMode::Exclusive),
        Err(Error::Busy)
    ));
    let _ = session.close(deadline());
    drop(session);
    let _ = store.shutdown(deadline());
    let available = lock(&*guard, FileLockMode::Exclusive);
    close(&*guard, available);
    guard.shutdown(deadline()).unwrap();
    assert_eq!(read_value(&mut releaser, 0, 7), Some(7));
    wait(
        &mut releaser,
        &other
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    releaser.close(deadline()).unwrap();
    other.shutdown(deadline()).unwrap();
}
