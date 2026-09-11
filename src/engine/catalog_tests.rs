//! 检查点发布与恢复通过同一目录锁协调；暂停点使用原生设备，不替代磁盘语义。
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
#[derive(Default)]
struct Control {
    attempts: AtomicUsize,
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
        assert!(!until.expired(), "目录仲裁设备等待超时");
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
        _ => panic!("目录锁完成错误"),
    }
}
fn lock(device: &dyn Device, mode: FileLockMode) -> FileId {
    let until = deadline();
    loop {
        match try_lock(device, mode) {
            Ok(file) => return file,
            Err(Error::Busy) => {
                assert!(!until.expired(), "预期可用的目录锁没有释放");
                std::thread::yield_now();
            }
            Err(error) => panic!("加锁失败：{error}"),
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
        assert!(!until.expired(), "未到达目录仲裁暂停点");
        session.poll(PollBudget::default()).unwrap();
        store.maintenance().poll(PollBudget::default()).unwrap();
        std::thread::yield_now();
    }
}
#[test]
fn 独占目录锁阻挡检查点且发布共享锁覆盖提交重命名() {
    let control = Arc::new(Control::default());
    let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let guard = external(&store);
    let held = lock(&*guard, FileLockMode::Exclusive);
    control.pause_publish.store(true, Ordering::SeqCst);
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
fn 恢复读取持有共享目录锁且实例发布前释放() {
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
            "恢复未到达材料读取暂停点"
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
    control.pause_reads.store(false, Ordering::SeqCst);
    let (restored, _) = child.join().unwrap().unwrap();
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
fn 等待目录锁期间基准索引失效则日志检查点拒绝发布() {
    let control = Arc::new(Control::default());
    let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let base = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Index)
            .unwrap(),
    );
    let guard = external(&store);
    let held = lock(&*guard, FileLockMode::Exclusive);
    let before = control.attempts.load(Ordering::SeqCst);
    let ticket = store.maintenance().checkpoint(CheckpointKind::Log).unwrap();
    progress_until(&store, &mut session, || {
        control.attempts.load(Ordering::SeqCst) >= before + 2
    });
    // 测试在独占仲裁内模拟提交失效；公开释放及删除重试由后续任务接入。
    let source = store
        .inner
        .storage
        .checkpoint_path(base.token, "commit")
        .unwrap();
    let destination = store
        .inner
        .storage
        .checkpoint_path(base.token, "commit.retired")
        .unwrap();
    execute(
        &*guard,
        IoOperation::Rename {
            source: source.clone(),
            destination,
        },
    )
    .unwrap();
    execute(
        &*guard,
        IoOperation::SyncDirectory(source.parent().unwrap().to_path_buf()),
    )
    .unwrap();
    close(&*guard, held);
    let result = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*result, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound)
    );
    for entry in std::fs::read_dir(store.inner.config.storage.root.join("checkpoints")).unwrap() {
        assert!(
            !entry.unwrap().path().join("commit").exists(),
            "不发布缺少基准索引的日志检查点"
        );
    }
    // 检查点失败沿用失败关闭协议；未确认关闭的目录句柄由设备关闭释放。
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
}
