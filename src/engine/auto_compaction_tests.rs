//! 自动调度的实际文件 I/O、停止排空、错误结果与恢复；不手动触发压缩。
use super::*;
use crate::api::maintenance::{
    AutoCompactionPhase as AutoPhase, AutoCompactionStatus, PhysicalReclamation,
};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
struct IoState {
    selected: BTreeSet<IoId>,
    held: Vec<IoCompletion>,
    ready: VecDeque<IoCompletion>,
    reads: usize,
    failed: bool,
}
#[derive(Default)]
struct Gate {
    state: Mutex<IoState>,
    hold: AtomicBool,
    fail: AtomicBool,
    fatal: AtomicBool,
    panic_poll: AtomicBool,
    panic_accept: AtomicBool,
}
struct Factory(Arc<Gate>);
struct GatedDevice {
    inner: Box<dyn Device>,
    gate: Arc<Gate>,
}
impl DeviceFactory for Factory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(GatedDevice {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 128,
            }
            .open(options)?,
            gate: self.0.clone(),
        }))
    }
}
impl Device for GatedDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        if std::thread::current().name() == Some("raster自动压缩")
            && self.gate.panic_accept.swap(false, Ordering::SeqCst)
        {
            panic!("注入自动接受前的设备能力查询恐慌");
        }
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        // 填充阶段只有 Upsert；所有冷读均来自自动任务，也要拦住 Session 协助推进时提交的读取。
        let select = matches!(request.operation, IoOperation::Read { .. });
        let mut state = self.gate.state.lock().unwrap();
        let id = self.inner.submit(request)?;
        if select {
            state.selected.insert(id);
            state.reads += 1;
        }
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        if self.gate.panic_poll.swap(false, Ordering::SeqCst) {
            panic!("注入自动线程的设备轮询恐慌");
        }
        let mut incoming = Vec::new();
        self.inner.poll(budget, &mut incoming)?;
        let mut state = self.gate.state.lock().unwrap();
        for completion in incoming {
            if state.selected.remove(&completion.id) {
                state.held.push(completion);
            } else {
                state.ready.push_back(completion);
            }
        }
        if !self.gate.hold.load(Ordering::SeqCst) {
            let released = std::mem::take(&mut state.held);
            for mut completion in released {
                if self.gate.fail.load(Ordering::SeqCst) && !state.failed {
                    assert!(matches!(completion.result, Ok(IoOutcome::Transferred(_))));
                    completion.result = if self.gate.fatal.load(Ordering::SeqCst) {
                        Err(Error::InvalidState("注入自动读取协议错误"))
                    } else {
                        Err(Error::Io(std::io::Error::from_raw_os_error(5)))
                    };
                    state.failed = true;
                }
                state.ready.push_back(completion);
            }
        }
        for _ in 0..budget.0.get() {
            let Some(completion) = state.ready.pop_front() else {
                break;
            };
            output.push(completion);
        }
        Ok(())
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)?;
        // 底层已停止，适配器保留的完成缓冲也在设备结束边界释放。
        let mut state = self.gate.state.lock().unwrap();
        state.selected.clear();
        state.held.clear();
        state.ready.clear();
        Ok(())
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.hold.store(false, Ordering::SeqCst);
    }
}
fn config(root: &Directory) -> Config {
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.storage.segment_bytes = 8192;
    config.index.buckets = 16;
    config.maintenance.auto_compaction = true;
    config.maintenance.workers = 2;
    let policy = &mut config.maintenance.auto_compaction_policy;
    policy.check_interval = Duration::from_millis(5);
    policy.log_size_budget = 32 * 1024;
    // 触发时已写满八页，四页驻留预算之外至少有两个完整冷页，首轮目标能覆盖整段。
    policy.trigger_fraction = 1.0;
    policy.compact_fraction = 0.4;
    policy.max_compacted_bytes = 8192;
    config
}
fn fixture(gate: &Arc<Gate>) -> (Directory, RasterKV<Schema>, Config) {
    let root = Directory(
        std::env::temp_dir().join(format!("raster-auto-{:x?}", StoreId::generate().unwrap().0)),
    );
    let config = config(&root);
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config.clone())
        .device(Box::new(Factory(gate.clone())))
        .create()
        .unwrap();
    (root, store, config)
}
fn until(mut condition: impl FnMut() -> bool) {
    let end = deadline();
    while !condition() {
        assert!(!end.expired(), "自动维护未达到指定状态");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn submit<O, T: 'static>(
    session: &mut Session<Schema>,
    serial: u64,
    mut request: O,
    mut call: impl FnMut(&mut Session<Schema>, Serial, O) -> Result<Submission<T>, Rejected<O>>,
) -> crate::api::Outcome<T> {
    let end = deadline();
    loop {
        match call(session, Serial(serial), request) {
            Ok(Submission::Ready(result)) => return result.unwrap(),
            Ok(Submission::Pending(mut ticket)) => {
                return session.wait(&mut ticket, end).unwrap().unwrap();
            }
            Err(rejected) if matches!(rejected.reason, Error::Busy) => {
                assert!(!end.expired(), "接受前 Busy 未解除");
                assert_ne!(
                    session
                        .engine
                        .coordinator
                        .last_accepted(session.id())
                        .unwrap(),
                    Some(Serial(serial))
                );
                request = rejected.request;
                session.poll(PollBudget::default()).unwrap();
                std::thread::yield_now();
            }
            Err(rejected) => panic!("{:?}", rejected.reason),
        }
    }
}
fn put(session: &mut Session<Schema>, serial: u64, key: u64) {
    submit(session, serial, Put(key), |s, n, o| s.upsert(n, o));
}
fn populate(store: &RasterKV<Schema>) -> Session<Schema> {
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..600 {
        put(&mut session, key, key);
    }
    session
}
fn stopped(store: &RasterKV<Schema>) -> AutoCompactionStatus {
    let status = store
        .maintenance()
        .wait_auto_compaction(deadline())
        .unwrap();
    assert_eq!(status.phase, AutoPhase::Stopped);
    assert!(status.active.is_none() && status.failure.is_none());
    status
}
#[test]
fn 自动维护真实读取在途时停止幂等且截止时间保留任务() {
    let gate = Arc::new(Gate::default());
    gate.hold.store(true, Ordering::SeqCst);
    let _release = Release(gate.clone());
    let (_root, store, _) = fixture(&gate);
    let mut session = populate(&store);
    session.close(deadline()).unwrap();
    drop(session);
    until(|| !gate.state.lock().unwrap().held.is_empty());
    let before = store.maintenance().auto_compaction_status().unwrap();
    assert_eq!(before.phase, AutoPhase::Compacting);
    assert!(before.active.is_some() && before.budget_reached);
    let maintenance = store.maintenance();
    maintenance.stop_auto_compaction().unwrap();
    maintenance.stop_auto_compaction().unwrap();
    assert_eq!(
        maintenance.auto_compaction_status().unwrap().phase,
        AutoPhase::Stopping
    );
    assert!(matches!(
        maintenance.wait_auto_compaction(Deadline(Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert!(matches!(
        store.shutdown(Deadline(Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(
        maintenance.auto_compaction_status().unwrap().active,
        before.active
    );
    gate.hold.store(false, Ordering::SeqCst);
    let after = stopped(&store);
    assert_eq!(after.completed_compactions, 1);
    let report = after
        .last_compaction
        .as_ref()
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
    assert!(report.copied > 0 && report.gc.is_some() && report.checkpoint.is_none());
    assert_eq!(store.inner.log.frontiers().unwrap().begin, report.until);
    assert!(gate.state.lock().unwrap().held.is_empty());
    store.shutdown(deadline()).unwrap();
    assert_eq!(stopped(&store).completed_compactions, 1);
}
#[test]
fn 关闭自动排空已接受压缩且线程不持有存储环() {
    let gate = Arc::new(Gate::default());
    gate.hold.store(true, Ordering::SeqCst);
    let release = Release(gate.clone());
    let (_root, store, _) = fixture(&gate);
    let mut session = populate(&store);
    session.close(deadline()).unwrap();
    drop(session);
    until(|| !gate.state.lock().unwrap().held.is_empty());
    let weak = Arc::downgrade(&store.inner);
    std::thread::scope(|scope| {
        let closer = scope.spawn(|| store.shutdown(deadline()));
        until(|| {
            store.maintenance().auto_compaction_status().unwrap().phase == AutoPhase::Stopping
        });
        assert!(!closer.is_finished(), "I/O 未归还，关闭不能提前成功");
        drop(release);
        assert!(closer.join().unwrap().unwrap().device_drained);
    });
    assert_eq!(stopped(&store).completed_compactions, 1);
    drop(store);
    assert!(weak.upgrade().is_none());
}
#[test]
fn 自动压缩普通错误保留原始结果并停止而不重放() {
    let gate = Arc::new(Gate::default());
    gate.hold.store(true, Ordering::SeqCst);
    let _release = Release(gate.clone());
    let (_root, store, _) = fixture(&gate);
    let mut session = populate(&store);
    until(|| !gate.state.lock().unwrap().held.is_empty());
    gate.fail.store(true, Ordering::SeqCst);
    gate.hold.store(false, Ordering::SeqCst);
    until(|| store.maintenance().auto_compaction_status().unwrap().phase == AutoPhase::Failed);
    let status = store
        .maintenance()
        .wait_auto_compaction(deadline())
        .unwrap();
    assert_eq!(status.completed_compactions, 1);
    let report = status.last_compaction.unwrap();
    assert!(
        matches!(report.as_ref(), Err(Error::CompactionFailed { cause, gc: None, .. })
        if matches!(cause.as_ref(), Error::Io(e) if e.raw_os_error()==Some(5)))
    );
    assert!(!store.inner.failed.load(Ordering::SeqCst));
    let reads = gate.state.lock().unwrap().reads;
    for _ in 0..30 {
        store.maintenance().poll(PollBudget::default()).unwrap();
    }
    assert_eq!(gate.state.lock().unwrap().reads, reads);
    assert!(Arc::ptr_eq(
        &report,
        &store
            .maintenance()
            .auto_compaction_status()
            .unwrap()
            .last_compaction
            .unwrap()
    ));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 自动调度等待手动屏障时可停止且不取消手动任务() {
    let gate = Arc::new(Gate::default());
    let (_root, store, _) = fixture(&gate);
    let mut session = store.start_session(Default::default()).unwrap();
    let blocker = crate::engine::session_actor::session(&store);
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    // 第二个会话未刷新屏障，自动任务只能等待全局动作；写会话仍可正常写入和刷页。
    for key in 0..600 {
        put(&mut session, key, key);
    }
    until(|| store.maintenance().auto_compaction_status().unwrap().phase == AutoPhase::Scheduled);
    assert!(checkpoint.try_report().unwrap().is_none());
    assert!(
        store
            .maintenance()
            .auto_compaction_status()
            .unwrap()
            .active
            .is_none()
    );
    store.maintenance().stop_auto_compaction().unwrap();
    assert_eq!(
        session.wait_auto_compaction(deadline()).unwrap().phase,
        AutoPhase::Stopped
    );
    assert!(checkpoint.try_report().unwrap().is_none());
    blocker.call(|session| session.close(deadline()).unwrap());
    let report = session.wait_maintenance(&checkpoint, deadline()).unwrap();
    assert!(report.is_ok());
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 多轮自动压缩与检查点恢复保持墓碑及会话进度() {
    let gate = Arc::new(Gate::default());
    let (_root, mut store, config) = fixture(&gate);
    let mut session = store.start_session(Default::default()).unwrap();
    let id = session.id();
    let mut serial = 0;
    for _ in 0..3 {
        let before = store
            .maintenance()
            .auto_compaction_status()
            .unwrap()
            .completed_compactions;
        for key in 0..600 {
            put(&mut session, serial, key);
            serial += 1;
            if key % 7 == 0 {
                submit(&mut session, serial, Delete(key), |s, n, o| {
                    s.delete(n, o, Default::default())
                });
                serial += 1;
            }
        }
        until(|| {
            store
                .maintenance()
                .auto_compaction_status()
                .unwrap()
                .completed_compactions
                > before
        });
        store.maintenance().stop_auto_compaction().unwrap();
        let status = session.wait_auto_compaction(deadline()).unwrap();
        assert_eq!(status.phase, AutoPhase::Stopped, "{status:?}");
        let compact = status
            .last_compaction
            .as_ref()
            .unwrap()
            .as_ref()
            .as_ref()
            .unwrap();
        assert!(compact.checkpoint.is_none());
        assert!(compact.gc.as_ref().unwrap().index_cleaned);
        assert!(matches!(
            compact.gc.as_ref().unwrap().physical,
            PhysicalReclamation::Completed
        ));
        let checkpoint = store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap();
        let report = wait(&mut session, &checkpoint);
        assert_eq!(
            report
                .sessions
                .iter()
                .find(|p| p.session == id)
                .unwrap()
                .serial,
            Serial(serial - 1)
        );
        let set = crate::api::maintenance::RecoverySet {
            store: store.id(),
            index: report.token,
            log: report.token,
        };
        session.close(deadline()).unwrap();
        drop(session);
        store.shutdown(deadline()).unwrap();
        drop(store);
        let (recovered, _) = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config.clone())
            .device(Box::new(Factory(gate.clone())))
            .recover(set)
            .unwrap();
        store = recovered;
        // 恢复发布后调度器才启动；开始会话若与自动任务同时接受，等待 Busy 后重试。
        let end = deadline();
        let resumed = loop {
            match store.continue_session(id) {
                Ok(resumed) => break resumed,
                Err(Error::Busy) => {
                    assert!(!end.expired());
                    std::thread::yield_now();
                }
                Err(error) => panic!("{error:?}"),
            }
        };
        assert_eq!(resumed.progress.serial, Serial(serial - 1));
        session = resumed.session;
        for key in 0..600 {
            let result = submit(&mut session, serial, Read(key), |s, n, o| {
                s.read(n, o, Default::default())
            });
            serial += 1;
            if key % 7 == 0 {
                assert!(matches!(result, crate::api::Outcome::NotFound));
            } else {
                assert!(matches!(result, crate::api::Outcome::Success(value) if value==key));
            }
        }
    }
    store.maintenance().stop_auto_compaction().unwrap();
    session.wait_auto_compaction(deadline()).unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 自动回收被挂起读者延后后只接续物理回收直到租约释放() {
    let gate = Arc::new(Gate::default());
    gate.hold.store(true, Ordering::SeqCst);
    let _release = Release(gate.clone());
    let (_root, store, _) = fixture(&gate);
    let mut session = populate(&store);
    until(|| !gate.state.lock().unwrap().held.is_empty());
    let Submission::Pending(mut pending) = session
        .read(Serial(600), Read(0), Default::default())
        .unwrap()
    else {
        panic!("冷读取必须挂起并保存段租约");
    };
    gate.hold.store(false, Ordering::SeqCst);
    until(|| {
        store
            .maintenance()
            .auto_compaction_status()
            .unwrap()
            .last_reclamation
            .is_some()
    });
    let status = store.maintenance().auto_compaction_status().unwrap();
    assert_eq!(
        status.completed_compactions, 1,
        "段租约未归还时不能通过重复复制来重试删除"
    );
    let first = status.last_compaction.unwrap();
    assert_eq!(first.as_ref().as_ref().unwrap().until, LogAddress(8192));
    assert!(matches!(
        first
            .as_ref()
            .as_ref()
            .unwrap()
            .gc
            .as_ref()
            .unwrap()
            .physical,
        PhysicalReclamation::DeferredByRuntime { .. }
    ));
    assert!(matches!(
        pending.try_take().unwrap(),
        crate::api::TicketState::Pending
    ));
    assert!(matches!(
        session.wait(&mut pending, deadline()).unwrap().unwrap(),
        crate::api::Outcome::Success(0)
    ));
    until(|| {
        store
            .maintenance()
            .auto_compaction_status()
            .unwrap()
            .last_reclamation
            .as_ref()
            .is_some_and(|report| {
                report
                    .as_ref()
                    .as_ref()
                    .is_ok_and(|r| matches!(r.physical, PhysicalReclamation::Completed))
            })
    });
    store.maintenance().stop_auto_compaction().unwrap();
    let final_status = session.wait_auto_compaction(deadline()).unwrap();
    assert_eq!(final_status.phase, AutoPhase::Stopped, "{final_status:?}");
    assert!(
        final_status
            .last_reclamation
            .unwrap()
            .as_ref()
            .as_ref()
            .unwrap()
            .deleted_segments
            > 0
    );
    assert!(store.inner.storage.resolve(LogAddress(0)).is_err());
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 禁用时无需线程且启用的空闲线程不会阻碍存储销毁() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    store.maintenance().stop_auto_compaction().unwrap();
    assert_eq!(
        store
            .maintenance()
            .wait_auto_compaction(Deadline(Instant::now()))
            .unwrap()
            .phase,
        AutoPhase::Disabled
    );
    let mut config = Config::default();
    config.maintenance.auto_compaction = true;
    config.maintenance.auto_compaction_policy.log_size_budget = 1 << 30;
    assert!(matches!(
        RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(device::null::NullDeviceFactory))
            .create(),
        Err(Error::UnsupportedDurability)
    ));
    store.shutdown(deadline()).unwrap();
    let gate = Arc::new(Gate::default());
    let (_root, store, _) = fixture(&gate);
    assert_eq!(
        store.maintenance().auto_compaction_status().unwrap().phase,
        AutoPhase::Idle
    );
    let weak = Arc::downgrade(&store.inner);
    drop(store);
    until(|| weak.upgrade().is_none());
}

#[test]
fn 自动维护协议错误或设备恐慌失败关闭后仍能结束调度和设备() {
    for panic_poll in [false, true] {
        let gate = Arc::new(Gate::default());
        gate.hold.store(true, Ordering::SeqCst);
        let _release = Release(gate.clone());
        let (_root, store, _) = fixture(&gate);
        let mut session = populate(&store);
        session.close(deadline()).unwrap();
        drop(session);
        until(|| !gate.state.lock().unwrap().held.is_empty());
        if panic_poll {
            // 完成仍被扣留，先确认后台实际进入 panic 点；不能让另一故障抢先终结任务，
            // 将尚未消耗的 panic 开关留给后续主线程 shutdown。
            gate.panic_poll.store(true, Ordering::SeqCst);
            until(|| !gate.panic_poll.load(Ordering::SeqCst));
        } else {
            gate.fatal.store(true, Ordering::SeqCst);
            gate.fail.store(true, Ordering::SeqCst);
        }
        gate.hold.store(false, Ordering::SeqCst);
        until(|| store.maintenance().auto_compaction_status().unwrap().phase == AutoPhase::Failed);
        let status = store.maintenance().auto_compaction_status().unwrap();
        assert!(status.active.is_none());
        assert!(status.last_compaction.unwrap().is_err());
        assert!(store.inner.failed.load(Ordering::SeqCst));
        assert!(!gate.panic_poll.load(Ordering::SeqCst));
        assert_eq!(gate.state.lock().unwrap().failed, !panic_poll);
        store.shutdown(deadline()).unwrap();
        assert!(gate.state.lock().unwrap().held.is_empty());
    }
}

#[test]
fn 自动接受前恐慌同步终结手动全局动作而不让关闭永久忙碌() {
    let gate = Arc::new(Gate::default());
    let (_root, store, _) = fixture(&gate);
    let mut session = store.start_session(Default::default()).unwrap();
    let blocker = crate::engine::session_actor::session(&store);
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    for key in 0..600 {
        put(&mut session, key, key);
    }
    until(|| store.maintenance().auto_compaction_status().unwrap().phase == AutoPhase::Scheduled);
    gate.panic_accept.store(true, Ordering::SeqCst);
    until(|| store.maintenance().auto_compaction_status().unwrap().phase == AutoPhase::Failed);
    let status = store.maintenance().auto_compaction_status().unwrap();
    assert_eq!(status.completed_compactions, 0);
    assert!(status.active.is_none() && status.failure.is_some());
    assert!(store.inner.failed.load(Ordering::SeqCst));
    assert_eq!(
        store.inner.coordinator.snapshot().unwrap().phase,
        crate::coordination::Phase::Failed
    );
    assert!(checkpoint.try_report().unwrap().unwrap().is_err());
    drop(session);
    drop(blocker);
    store.shutdown(deadline()).unwrap();
}
