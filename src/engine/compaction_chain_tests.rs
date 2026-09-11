//! 真实维护子任务串接验证；内部票据不暴露，不把计划步骤冒充已完成输出。
use super::*;
use crate::api::maintenance::{
    CompactionAlgorithm, CompactionOptions, PhysicalReclamation, RecoverySet,
};

fn options(
    algorithm: CompactionAlgorithm,
    until: LogAddress,
    checkpoint: bool,
    shift_begin: bool,
) -> CompactionOptions {
    CompactionOptions {
        algorithm,
        until,
        workers: 1,
        checkpoint,
        shift_begin,
    }
}
#[test]
fn 两算法后续检查点先于截断且各选项保留数据墓碑和会话进度() {
    for algorithm in [CompactionAlgorithm::Lookup, CompactionAlgorithm::ScanDedup] {
        for (checkpoint, shift) in [(true, false), (false, true), (true, true)] {
            let (_root, store) = setup(None);
            let config = store.inner.config.clone();
            let mut session = store.start_session(Default::default()).unwrap();
            for key in 0..96 {
                put(&mut session, key, key);
            }
            let submission = session
                .delete(
                    Serial(96),
                    Delete(5),
                    crate::api::operation::DeleteOptions {
                        force_tombstone: true,
                    },
                )
                .unwrap();
            match submission {
                Submission::Ready(result) => {
                    result.unwrap();
                }
                Submission::Pending(mut ticket) => {
                    session.wait(&mut ticket, deadline()).unwrap().unwrap();
                }
            }
            let before = store.inner.log.frontiers().unwrap();
            let ticket = store
                .maintenance()
                .compact(options(algorithm, before.tail, checkpoint, shift))
                .unwrap();
            assert!(matches!(
                session.wait_maintenance(&ticket, Deadline(Instant::now())),
                Err(Error::DeadlineExceeded)
            ));
            let result = session.wait_maintenance(&ticket, deadline()).unwrap();
            let report = result.as_ref().as_ref().unwrap();
            assert_eq!(report.copied, 96);
            assert_eq!(report.checkpoint.is_some(), checkpoint);
            assert_eq!(report.gc.is_some(), shift);
            assert_eq!(
                store.inner.log.frontiers().unwrap().begin,
                if shift { before.tail } else { before.begin }
            );
            if let Some(gc) = &report.gc {
                assert_eq!(gc.begin, before.tail);
                assert!(gc.index_cleaned);
                assert!(matches!(gc.physical, PhysicalReclamation::Completed));
            }
            let session_id = session.id();
            if let Some(checkpoint) = &report.checkpoint {
                assert_eq!(checkpoint.begin, before.begin, "先持久化检查点再截断");
                assert_eq!(
                    checkpoint
                        .sessions
                        .iter()
                        .find(|cut| cut.session == session_id)
                        .unwrap()
                        .serial,
                    Serial(96)
                );
                let (reader, _) = recover_store(
                    config.clone(),
                    RecoverySet {
                        store: store.id(),
                        index: checkpoint.token,
                        log: checkpoint.token,
                    },
                )
                .unwrap();
                let mut read = reader.continue_session(session_id).unwrap().session;
                for key in 0..96 {
                    assert_eq!(
                        read_value(&mut read, 97 + key, key),
                        if key == 5 { None } else { Some(key) }
                    );
                }
                read.close(deadline()).unwrap();
                reader.shutdown(deadline()).unwrap();
            }
            for key in 0..96 {
                assert_eq!(
                    read_value(&mut session, 97 + key, key),
                    if key == 5 { None } else { Some(key) }
                );
            }
            assert!(std::sync::Arc::ptr_eq(
                &result,
                &ticket.try_report().unwrap().unwrap()
            ));
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
        }
    }
}
#[test]
fn 复合任务动作间隙允许其他维护但关闭不能越过未完成票据() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let before = store.inner.log.frontiers().unwrap();
    let ticket = store
        .maintenance()
        .compact(options(
            CompactionAlgorithm::Lookup,
            before.tail,
            false,
            true,
        ))
        .unwrap();
    session.close(deadline()).unwrap();
    for step in 0..1000 {
        assert!(step < 999, "未到达复制结束间隙");
        store.inner.progress_compaction().unwrap();
        if store.inner.coordinator.snapshot().unwrap().id.is_none() {
            break;
        }
    }
    assert!(ticket.try_report().unwrap().is_none());
    assert!(matches!(store.shutdown(deadline()), Err(Error::Busy)));
    let other = store.maintenance().shift_begin(before.begin).unwrap();
    assert_eq!(
        store.inner.progress_compaction().unwrap(),
        (false, false),
        "不占用动作等待自己，其他动作竞争只是 Busy"
    );
    for step in 0..10000 {
        assert!(step < 9999, "无会话驱动没有完成后续动作");
        store.maintenance().poll(PollBudget::default()).unwrap();
        if ticket.try_report().unwrap().is_some() {
            break;
        }
    }
    other
        .try_report()
        .unwrap()
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
    let result = ticket.try_report().unwrap().unwrap();
    assert_eq!(
        result.as_ref().as_ref().unwrap().gc.as_ref().unwrap().begin,
        before.tail
    );
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 不支持检查点的设备在复合压缩接受前拒绝且没有迁移() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let before = store.inner.log.frontiers().unwrap();
    assert!(matches!(
        store.maintenance().compact(options(
            CompactionAlgorithm::Lookup,
            before.tail,
            true,
            true
        )),
        Err(Error::UnsupportedDurability)
    ));
    assert_eq!(store.inner.log.frontiers().unwrap().tail, before.tail);
    assert_eq!(store.inner.log.frontiers().unwrap().begin, before.begin);
    assert!(store.inner.coordinator.snapshot().unwrap().id.is_none());
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

mod failure {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    #[derive(Clone, Copy, Debug)]
    enum Point {
        Checkpoint,
        Gc,
    }
    struct Control {
        point: Point,
        armed: AtomicBool,
        completions: AtomicUsize,
        pending: Mutex<std::collections::BTreeSet<IoId>>,
    }
    struct Factory(Arc<Control>);
    struct Fault {
        inner: Box<dyn Device>,
        control: Arc<Control>,
    }
    impl DeviceFactory for Factory {
        fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
            Ok(Box::new(Fault {
                inner: device::thread_pool::ThreadPoolDeviceFactory {
                    workers: 2,
                    queue_capacity: 64,
                }
                .open(options)?,
                control: self.0.clone(),
            }))
        }
    }
    impl Device for Fault {
        fn capabilities(&self) -> DeviceCapabilities {
            self.inner.capabilities()
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            let selected = match (&self.control.point, &request.operation) {
                (Point::Checkpoint, IoOperation::Rename { destination, .. }) => {
                    destination.file_name().is_some_and(|name| name == "commit")
                }
                (Point::Gc, IoOperation::RemoveFile(path)) => path.starts_with("segments"),
                _ => false,
            };
            let id = self.inner.submit(request)?;
            if selected && self.control.armed.swap(false, Ordering::SeqCst) {
                self.control.pending.lock().unwrap().insert(id);
            }
            Ok(id)
        }
        fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
            self.inner.poll(budget, output)?;
            self.control
                .completions
                .fetch_add(output.len(), Ordering::SeqCst);
            for completion in output {
                if self.control.pending.lock().unwrap().remove(&completion.id) {
                    assert!(matches!(completion.result, Ok(IoOutcome::Done)));
                    completion.result = Err(Error::Io(std::io::Error::from_raw_os_error(5)));
                }
            }
            Ok(())
        }
        fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
            self.inner.shutdown(deadline)
        }
    }
    #[test]
    fn 后续检查点或回收实际生效后失败不重放复制且保留完整错误与已完成报告() {
        for point in [Point::Checkpoint, Point::Gc] {
            let root = Directory(std::env::temp_dir().join(format!(
                "raster-chain-fault-{:x?}",
                StoreId::generate().unwrap().0
            )));
            let mut config = Config::default();
            config.storage.root = root.0.clone();
            config.storage.segment_bytes = 4096;
            config.log.page_bytes = 4096;
            config.index.buckets = 16;
            let control = Arc::new(Control {
                point,
                armed: true.into(),
                completions: AtomicUsize::new(0),
                pending: Mutex::new(Default::default()),
            });
            let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
                .config(config.clone())
                .device(Box::new(Factory(control.clone())))
                .create()
                .unwrap();
            let mut session = store.start_session(Default::default()).unwrap();
            for key in 0..400 {
                put(&mut session, key, key);
            }
            let before = store.inner.log.frontiers().unwrap();
            let ticket = store
                .maintenance()
                .compact(options(
                    CompactionAlgorithm::Lookup,
                    before.tail,
                    true,
                    true,
                ))
                .unwrap();
            // 这里包含 400 条复制、完整检查点和原生目录同步；共享 CI 的总预算独立于 API 的短等待测试。
            // 切片超时始终复用原票据，不重新提交。总预算耗尽时保留阶段和实际 I/O 进度以诊断停滞。
            let started = Instant::now();
            let limit = Deadline(started + Duration::from_secs(60));
            let result = loop {
                let slice = Deadline((Instant::now() + Duration::from_secs(10)).min(limit.0));
                match session.wait_maintenance(&ticket, slice) {
                    Ok(result) => break result,
                    Err(Error::DeadlineExceeded) if !limit.expired() => {
                        eprintln!(
                            "复合故障 {point:?} 等待切片结束：阶段 {:?}，边界 {:?}，完成 I/O {}，故障待触发 {}",
                            store.inner.coordinator.snapshot(),
                            store.inner.log.frontiers(),
                            control.completions.load(Ordering::SeqCst),
                            control.armed.load(Ordering::SeqCst)
                        );
                    }
                    Err(error) => panic!(
                        "复合故障 {point:?} 未终结：{error:?}，阶段 {:?}，边界 {:?}，完成 I/O {}，故障待触发 {}",
                        store.inner.coordinator.snapshot(),
                        store.inner.log.frontiers(),
                        control.completions.load(Ordering::SeqCst),
                        control.armed.load(Ordering::SeqCst)
                    ),
                }
            };
            eprintln!(
                "复合故障 {point:?} 已终结：耗时 {:?}，完成 I/O {}",
                started.elapsed(),
                control.completions.load(Ordering::SeqCst)
            );
            let Err(Error::CompactionFailed {
                until,
                copied,
                checkpoint,
                gc,
                cause,
            }) = &*result
            else {
                panic!("应返回带部分效果的复合错误：{result:?}");
            };
            assert_eq!(*until, before.tail);
            assert_eq!(*copied, 400);
            assert!(gc.is_none());
            assert!(
                !control.armed.load(Ordering::SeqCst),
                "未命中故障点 {point:?}，实际结果：{result:?}"
            );
            match point {
                Point::Checkpoint => {
                    assert!(checkpoint.is_none());
                    assert!(matches!(&**cause,Error::Io(error) if error.raw_os_error()==Some(5)));
                    assert_eq!(
                        store.inner.log.frontiers().unwrap().begin,
                        before.begin,
                        "检查点失败后不截断"
                    );
                    assert!(store.inner.failed.load(Ordering::SeqCst));
                }
                Point::Gc => {
                    let checkpoint = checkpoint
                        .as_ref()
                        .expect("已持久化检查点必须保留在错误报告中");
                    assert_eq!(checkpoint.begin, before.begin);
                    assert!(
                        matches!(&**cause,Error::GcFailed {begin,deleted_segments:0,cause,..} if *begin==before.tail && matches!(&**cause,Error::Io(error) if error.raw_os_error()==Some(5)))
                    );
                    assert_eq!(store.inner.log.frontiers().unwrap().begin, before.tail);
                    assert!(!store.inner.failed.load(Ordering::SeqCst));
                    let tail = store.inner.log.frontiers().unwrap().tail;
                    for _ in 0..16 {
                        store.maintenance().poll(PollBudget::default()).unwrap();
                    }
                    assert_eq!(
                        store.inner.log.frontiers().unwrap().tail,
                        tail,
                        "失败不自动重放复制"
                    );
                    let retry = store.maintenance().shift_begin(before.tail).unwrap();
                    session
                        .wait_maintenance(&retry, deadline())
                        .unwrap()
                        .as_ref()
                        .as_ref()
                        .unwrap();
                    let (reader, _) = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
                        .config(config)
                        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
                            workers: 2,
                            queue_capacity: 64,
                        }))
                        .recover(RecoverySet {
                            store: store.id(),
                            index: checkpoint.token,
                            log: checkpoint.token,
                        })
                        .unwrap();
                    let mut read = reader.start_session(Default::default()).unwrap();
                    assert_eq!(read_value(&mut read, 0, 399), Some(399));
                    read.close(deadline()).unwrap();
                    reader.shutdown(deadline()).unwrap();
                }
            }
            assert!(Arc::ptr_eq(&result, &ticket.try_report().unwrap().unwrap()));
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
        }
    }
}

#[derive(Debug)]
struct Panicking(u64);
impl Keyed<Schema> for Panicking {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Panicking {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        panic!("测试业务计算恐慌")
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        panic!("测试原地计算恐慌")
    }
}
#[test]
fn 回收期间业务恐慌的失败关闭仍报告已完成检查点和已推进begin() {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..64 {
        put(&mut session, key, key);
    }
    let before = store.inner.log.frontiers().unwrap();
    let ticket = store
        .maintenance()
        .compact(options(
            CompactionAlgorithm::Lookup,
            before.tail,
            true,
            true,
        ))
        .unwrap();
    let until = deadline();
    loop {
        assert!(!until.expired(), "未进入 GC 索引阶段");
        session
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
        store
            .maintenance()
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
        if store.inner.coordinator.snapshot().unwrap().phase == crate::coordination::Phase::GcIndex
        {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(store.inner.log.frontiers().unwrap().begin, before.tail);
    assert!(matches!(
        session.upsert(Serial(64), Panicking(1000)).unwrap(),
        Submission::Ready(Err(_))
    ));
    assert!(store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    let result = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*result,Err(Error::CompactionFailed {copied:64,checkpoint:Some(_),cause,..}) if matches!(&**cause,Error::GcFailed {begin,..} if *begin==before.tail)),
        "失败报告：{result:?}"
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
