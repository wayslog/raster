//! Actual files automatically scheduled I/O,Stop emptying,Error results and recovery;No manual triggering of compression.
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
        if std::thread::current().name() == Some("rasterautomatic_compression")
            && self.gate.panic_accept.swap(false, Ordering::SeqCst)
        {
            panic!("Inject device capability query panic before auto-acceptance");
        }
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        // The filling phase only Upsert;All cold reads come from automated tasks,Stop it too Session Read committed while assisting with advancement.
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
            panic!("Device polling panic injected into automatic threads");
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
                        Err(Error::InvalidState("Inject automatic read protocol errors"))
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
        // The bottom layer has stopped,The completion buffer retained by the adapter is also released at the device end boundary.
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
    // Eight pages filled when triggered,At least two full cold pages in addition to the four-page dwell budget,The first round target can cover the entire section.
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
        assert!(
            !end.expired(),
            "Automatic maintenance has not reached the specified status"
        );
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
                assert!(!end.expired(), "Before accepting Busy Not released");
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
fn automatically_maintain_idempotent_and_deadline_preserving_tasks_that_stop_real_reads_in_transit()
{
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
fn turning_off_automatic_draining_has_accepted_compression_and_the_thread_does_not_hold_a_storage_ring()
 {
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
        assert!(
            !closer.is_finished(),
            "I/O not returned,Closing cannot be successful in advance"
        );
        drop(release);
        assert!(closer.join().unwrap().unwrap().device_drained);
    });
    assert_eq!(stopped(&store).completed_compactions, 1);
    drop(store);
    assert!(weak.upgrade().is_none());
}
#[test]
fn automatic_compression_of_normal_errors_preserves_original_results_and_stops_without_replaying() {
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
fn automatic_scheduling_can_be_stopped_while_waiting_for_manual_barriers_without_canceling_manual_tasks()
 {
    let gate = Arc::new(Gate::default());
    let (_root, store, _) = fixture(&gate);
    let mut session = store.start_session(Default::default()).unwrap();
    let blocker = crate::engine::session_actor::session(&store);
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    // Barrier not refreshed in second session,Automatic tasks can only wait for global actions;The write session can still write and flush pages normally.
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
fn multiple_rounds_of_automatic_compression_and_checkpoint_recovery_maintain_tombstones_and_session_progress()
 {
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
        // The scheduler is started only after publishing is resumed.;Starting a session if accepted simultaneously with an automated task,wait Busy Try again later.
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
fn automatic_collection_is_delayed_by_a_suspended_reader_and_only_physical_collection_continues_until_the_lease_is_released()
 {
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
        panic!("Cold reads must suspend and save segment leases");
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
        "Deletion cannot be retried through repeated replication when segment lease is not returned"
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
fn no_threads_are_required_when_disabled_and_idle_threads_enabled_will_not_block_storage_destruction()
 {
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
fn automatic_maintenance_protocol_errors_or_device_panic_failures_can_still_end_scheduling_and_equipment_after_shutdown()
 {
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
            // Completion still withheld,First confirm the actual entry into the background panic point;Do not allow another fault to terminate the task first,
            // unused panic The switch is left to the subsequent main thread shutdown.
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
fn automatically_accept_pre_panic_sync_termination_of_manual_global_actions_without_leaving_shutdown_permanently_busy()
 {
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
