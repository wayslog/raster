//! Exercise the actual worker thread, partial effects, and resource cleanup;
//! a latch controls parallel interleaving, and copy results are never simulated.
use super::*;
use crate::{
    api::{
        maintenance::{CompactionAlgorithm, CompactionOptions},
        operation::ReadOperation,
        scan::{Buffering, ScanOptions},
    },
    schema::{
        KeyCodec, ValueRead,
        builtin::{SerializedValue, U64ValueCodec},
        value::ValueCodec,
    },
};
use std::{
    collections::HashSet,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};
#[derive(Default)]
struct Probe {
    enabled: AtomicBool,
    failure: AtomicU8,
    calls: AtomicUsize,
    release_zero: AtomicBool,
    gate: Mutex<(HashSet<std::thread::ThreadId>, bool)>,
    wake: Condvar,
}
impl Probe {
    fn release(&self) {
        self.gate.lock().unwrap().1 = true;
        self.wake.notify_all();
    }
    fn entered(&self) -> usize {
        self.gate.lock().unwrap().0.len()
    }
}
struct Release(Arc<Probe>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Codec(Arc<Probe>);
impl ValueCodec for Codec {
    type Value = u64;
    fn format_id(&self) -> FormatId {
        U64ValueCodec.format_id()
    }
    fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
        U64ValueCodec.encode(value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
        let value = U64ValueCodec.decode(bytes)?;
        if self.0.enabled.load(Ordering::SeqCst) {
            let current = std::thread::current();
            assert!(
                current
                    .name()
                    .is_some_and(|name| name.starts_with("rastercompaction-")),
                "Copy value decoding must be performed on a worker thread"
            );
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            let mut gate = self.0.gate.lock().unwrap();
            gate.0.insert(current.id());
            self.0.wake.notify_all();
            while !gate.1 && !(value == 0 && self.0.release_zero.load(Ordering::SeqCst)) {
                gate = self.0.wake.wait(gate).unwrap();
            }
            drop(gate);
            if value == 1 {
                match self.0.failure.load(Ordering::SeqCst) {
                    1 => return Err(Error::Codec("Worker thread decoding failed")),
                    2 => panic!("Worker thread decoding panic"),
                    _ => {}
                }
            }
        }
        Ok(value)
    }
}
type ProbeSchema = SchemaPair<U64Key, SerializedValue<Codec>>;
#[derive(Debug)]
struct Write(u64, u64);
impl Keyed<ProbeSchema> for Write {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<ProbeSchema> for Write {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.1, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, ProbeSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[derive(Debug)]
struct Read(u64);
impl Keyed<ProbeSchema> for Read {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl ReadOperation<ProbeSchema> for Read {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, ProbeSchema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
fn opts(algorithm: CompactionAlgorithm, until: LogAddress, workers: usize) -> CompactionOptions {
    CompactionOptions {
        algorithm,
        until,
        workers,
        checkpoint: false,
        shift_begin: false,
    }
}
fn probe_store(probe: Arc<Probe>) -> (RasterKV<ProbeSchema>, Vec<u64>) {
    let store = RasterKV::builder(SchemaPair::new(U64Key, SerializedValue::new(Codec(probe))))
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let first = 0;
    let second = (1..100)
        .find(|key| U64Key.hash(key).0 % 64 != U64Key.hash(&first).0 % 64)
        .unwrap();
    (store, vec![first, second])
}
fn insert_probe(store: &RasterKV<ProbeSchema>, keys: &[u64]) -> Session<ProbeSchema> {
    let mut session = store.start_session(Default::default()).unwrap();
    for (value, &key) in keys.iter().enumerate() {
        assert!(matches!(
            session
                .upsert(Serial(value as u64), Write(key, value as u64))
                .unwrap(),
            Submission::Ready(Ok(_))
        ));
    }
    session
}
fn reach_two(store: &RasterKV<ProbeSchema>, session: &mut Session<ProbeSchema>, probe: &Probe) {
    let until = deadline();
    while probe.entered() < 2 {
        assert!(
            !until.expired(),
            "Two workers did not arrive at the value decoding latch at the same time"
        );
        session.poll(PollBudget::default()).unwrap();
        store.maintenance().poll(PollBudget::default()).unwrap();
        std::thread::yield_now();
    }
}
#[test]
fn the_two_algorithms_all_work_threads_exit_before_independent_threads_copy_at_the_same_time_and_report_normally()
 {
    for algorithm in [CompactionAlgorithm::Lookup, CompactionAlgorithm::ScanDedup] {
        let probe = Arc::new(Probe::default());
        let _release = Release(probe.clone());
        let (store, keys) = probe_store(probe.clone());
        let mut session = insert_probe(&store, &keys);
        let before = store.inner.log.frontiers().unwrap();
        probe.enabled.store(true, Ordering::SeqCst);
        let ticket = store
            .maintenance()
            .compact(opts(algorithm, before.tail, 2))
            .unwrap();
        reach_two(&store, &mut session, &probe);
        assert!(ticket.try_report().unwrap().is_none());
        assert!(
            !probe
                .gate
                .lock()
                .unwrap()
                .0
                .contains(&std::thread::current().id())
        );
        probe.release();
        let result = session.wait_maintenance(&ticket, deadline()).unwrap();
        assert_eq!(result.as_ref().as_ref().unwrap().copied, 2);
        probe.enabled.store(false, Ordering::SeqCst);
        for (value, key) in keys.into_iter().enumerate() {
            assert!(
                matches!(session.read(Serial(2+value as u64),Read(key),Default::default()).unwrap(),Submission::Ready(Ok(crate::api::completion::Outcome::Success(actual))) if actual==value as u64)
            );
        }
        let weak = Arc::downgrade(&store.inner);
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
        drop(session);
        drop(store);
        assert!(
            weak.upgrade().is_none(),
            "Idle worker threads cannot cycle holding storage"
        );
    }
}
#[test]
fn worker_errors_and_panics_retain_actual_replication_counts_and_are_not_automatically_replayed() {
    for mode in [1, 2] {
        let probe = Arc::new(Probe::default());
        let _release = Release(probe.clone());
        let (store, keys) = probe_store(probe.clone());
        let mut session = insert_probe(&store, &keys);
        let before = store.inner.log.frontiers().unwrap();
        probe.failure.store(mode, Ordering::SeqCst);
        probe.enabled.store(true, Ordering::SeqCst);
        let ticket = store
            .maintenance()
            .compact(opts(CompactionAlgorithm::Lookup, before.tail, 2))
            .unwrap();
        reach_two(&store, &mut session, &probe);
        {
            // Shared latch with condition wait,Avoid notifications preceded by wait And the release signal of the first record is lost.
            let _gate = probe.gate.lock().unwrap();
            probe.release_zero.store(true, Ordering::SeqCst);
            probe.wake.notify_all();
        }
        let limit = deadline();
        while store
            .inner
            .resolve_index(U64Key.hash(&keys[0]), &keys[0].to_le_bytes())
            .unwrap()
            .head
            .unwrap()
            < before.tail
        {
            assert!(
                !limit.expired(),
                "The first copy is not completed and published"
            );
            session.poll(PollBudget::default()).unwrap();
            store.maintenance().poll(PollBudget::default()).unwrap();
            std::thread::yield_now();
        }
        probe.release();
        let result = session.wait_maintenance(&ticket, deadline()).unwrap();
        let Err(Error::CompactionFailed { copied, cause, .. }) = &*result else {
            panic!("should fail:{result:?}")
        };
        assert!(matches!(&**cause, Error::Codec(_) | Error::InvalidState(_)));
        assert_eq!(store.inner.failed.load(Ordering::SeqCst), mode == 2);
        probe.enabled.store(false, Ordering::SeqCst);
        let mut address = before.tail;
        let tail = store.inner.log.frontiers().unwrap().tail;
        let mut actual = 0;
        while address < tail {
            match store.inner.log.snapshot_next(address, tail).unwrap() {
                Some((at, bytes)) => {
                    let record = crate::format::Record::decode(&bytes).unwrap();
                    actual += u64::from(!record.header.invalid);
                    address = at
                        .checked_add(record.header.encoded_len().unwrap() as u64)
                        .unwrap();
                }
                None => break,
            }
        }
        assert_eq!(*copied, actual, "Count must match actual published record");
        assert_eq!(
            *copied, 1,
            "The first one was published before the second one failed"
        );
        let calls = probe.calls.load(Ordering::SeqCst);
        for _ in 0..16 {
            let _ = store.maintenance().poll(PollBudget::default());
        }
        assert_eq!(probe.calls.load(Ordering::SeqCst), calls);
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}
#[test]
fn thread_budget_exceeded_reject_before_accept_and_multi_worker_can_be_done_without_session_driver()
{
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let before = store.inner.log.frontiers().unwrap();
    for workers in [0, usize::MAX] {
        assert!(matches!(
            store
                .maintenance()
                .compact(opts(CompactionAlgorithm::Lookup, before.tail, workers)),
            Err(Error::InvalidConfig { .. })
        ));
    }
    assert_eq!(store.inner.log.frontiers().unwrap().tail, before.tail);
    let ticket = store
        .maintenance()
        .compact(opts(CompactionAlgorithm::Lookup, before.tail, 4))
        .unwrap();
    session.close(deadline()).unwrap();
    let until = deadline();
    loop {
        assert!(!until.expired());
        store.maintenance().poll(PollBudget::default()).unwrap();
        if ticket.try_report().unwrap().is_some() {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(
        ticket
            .try_report()
            .unwrap()
            .unwrap()
            .as_ref()
            .as_ref()
            .unwrap()
            .copied,
        1
    );
    store.shutdown(deadline()).unwrap();
}

mod io_failure {
    use super::*;
    #[derive(Default)]
    struct State {
        reads: usize,
        selected: std::collections::BTreeSet<IoId>,
        held: Vec<IoCompletion>,
        ready: std::collections::VecDeque<IoCompletion>,
        failed: bool,
    }
    #[derive(Default)]
    struct Control {
        state: Mutex<State>,
        release: AtomicBool,
        fatal: bool,
        shutdown: AtomicBool,
    }
    struct Factory(Arc<Control>);
    struct DeviceGate {
        inner: Box<dyn Device>,
        control: Arc<Control>,
    }
    impl DeviceFactory for Factory {
        fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
            Ok(Box::new(DeviceGate {
                inner: device::thread_pool::ThreadPoolDeviceFactory {
                    workers: 2,
                    queue_capacity: 64,
                }
                .open(options)?,
                control: self.0.clone(),
            }))
        }
    }
    impl Device for DeviceGate {
        fn capabilities(&self) -> DeviceCapabilities {
            self.inner.capabilities()
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            let selected = matches!(request.operation, IoOperation::Read { .. })
                && std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("rastercompaction-"));
            let mut state = self.control.state.lock().unwrap();
            let id = self.inner.submit(request)?;
            if selected {
                state.reads += 1;
                state.selected.insert(id);
            }
            Ok(id)
        }
        fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
            let mut incoming = Vec::new();
            self.inner.poll(budget, &mut incoming)?;
            let mut state = self.control.state.lock().unwrap();
            for completion in incoming {
                if state.selected.remove(&completion.id) {
                    state.held.push(completion);
                } else {
                    state.ready.push_back(completion);
                }
            }
            if self.control.release.load(Ordering::SeqCst) {
                let count = if self.control.fatal && !self.control.shutdown.load(Ordering::SeqCst) {
                    usize::from(!state.failed).min(state.held.len())
                } else {
                    state.held.len()
                };
                let released: Vec<_> = state.held.drain(..count).collect();
                for mut completion in released {
                    if !state.failed {
                        assert!(matches!(completion.result, Ok(IoOutcome::Transferred(_))));
                        completion.result = if self.control.fatal {
                            Err(Error::InvalidState("Injection worker read protocol broken"))
                        } else {
                            Err(Error::Io(std::io::Error::other(
                                "Worker disk read completion failed",
                            )))
                        };
                        state.failed = true;
                    }
                    state.ready.push_back(completion);
                }
            }
            for _ in 0..budget.0.get() {
                if let Some(completion) = state.ready.pop_front() {
                    output.push(completion);
                } else {
                    break;
                }
            }
            Ok(())
        }
        fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
            self.inner.shutdown(deadline)?;
            self.control.shutdown.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    #[test]
    fn when_multiple_workers_encounter_a_single_failure_when_reading_in_transit_they_first_return_all_buffers_and_then_terminate_the_action()
     {
        let control = Arc::new(Control::default());
        let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
        let mut session = store.start_session(Default::default()).unwrap();
        for key in 0..600 {
            put(&mut session, key, key);
        }
        let before = store.inner.log.frontiers().unwrap();
        assert!(before.head > before.begin);
        let ticket = store
            .maintenance()
            .compact(opts(CompactionAlgorithm::Lookup, before.tail, 4))
            .unwrap();
        let until = deadline();
        while control.state.lock().unwrap().held.len() < 2 {
            assert!(
                !until.expired(),
                "Workers are not producing parallel disk reads"
            );
            session.poll(PollBudget::default()).unwrap();
            store.maintenance().poll(PollBudget::default()).unwrap();
            std::thread::yield_now();
        }
        assert!(ticket.try_report().unwrap().is_none());
        assert!(matches!(
            store.maintenance().shift_begin(before.begin),
            Err(Error::Busy)
        ));
        control.release.store(true, Ordering::SeqCst);
        let result = session.wait_maintenance(&ticket, deadline()).unwrap();
        let Err(Error::CompactionFailed { copied, cause, .. }) = &*result else {
            panic!("Expected read failure:{result:?}")
        };
        assert!(matches!(&**cause, Error::Io(_)));
        assert!(!store.inner.failed.load(Ordering::SeqCst));
        {
            let state = control.state.lock().unwrap();
            assert!(state.failed && state.held.is_empty() && state.selected.is_empty());
        }
        let reads = control.state.lock().unwrap().reads;
        for _ in 0..16 {
            store.maintenance().poll(PollBudget::default()).unwrap();
        }
        assert_eq!(
            control.state.lock().unwrap().reads,
            reads,
            "Do not automatically retry work requests"
        );
        let mut scan = store
            .scan(ScanOptions {
                begin: before.tail,
                end: store.inner.log.frontiers().unwrap().tail,
                buffering: Buffering::DoublePage,
            })
            .unwrap();
        let mut actual = 0;
        while let Some(record) = scan.next_record().unwrap() {
            actual += u64::from(!record.invalid);
        }
        scan.close().unwrap();
        assert_eq!(*copied, actual);
        assert_eq!(read_value(&mut session, 600, 599), Some(599));
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
    #[test]
    fn worker_failed_shutdown_retains_unreturned_read_lease_until_end_of_device() {
        let control = Arc::new(Control {
            fatal: true,
            ..Default::default()
        });
        let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
        let mut session = store.start_session(Default::default()).unwrap();
        for key in 0..600 {
            put(&mut session, key, key);
        }
        let before = store.inner.log.frontiers().unwrap();
        let ticket = store
            .maintenance()
            .compact(opts(CompactionAlgorithm::Lookup, before.tail, 4))
            .unwrap();
        let limit = deadline();
        while control.state.lock().unwrap().held.len() < 2 {
            assert!(!limit.expired(), "No arrival of two-way read in transit");
            session.poll(PollBudget::default()).unwrap();
            store.maintenance().poll(PollBudget::default()).unwrap();
            std::thread::yield_now();
        }
        control.release.store(true, Ordering::SeqCst);
        let result = session.wait_maintenance(&ticket, deadline()).unwrap();
        assert!(
            matches!(&*result,Err(Error::CompactionFailed {cause,..}) if matches!(&**cause,Error::InvalidState(_)))
        );
        assert!(store.inner.failed.load(Ordering::SeqCst));
        assert!(
            !control.state.lock().unwrap().held.is_empty(),
            "There are still worker buffers that have not been returned"
        );
        assert!(
            matches!(
                store.inner.storage.invalidate(0, Generation(0)),
                Err(Error::Busy)
            ),
            "Failed ticket does not equal in-transit lease releasable"
        );
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
        assert!(control.shutdown.load(Ordering::SeqCst));
        assert!(control.state.lock().unwrap().held.is_empty());
        store.inner.storage.invalidate(0, Generation(0)).unwrap();
    }
}

#[test]
fn repeated_checkpoint_truncation_recovery_after_multi_worker_interleaving_with_write_delete_maintains_tombstones_and_session_progress()
 {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-workers-cycle-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 8192;
    config.log.page_bytes = 4096;
    config.index.buckets = 16;
    config.cache.enabled = true;
    config.cache.capacity_bytes = 32 * 1024;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config.clone())
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 64,
        }))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    for key in 0..8 {
        assert_eq!(read_value(&mut session, 400 + key, key), Some(key));
    }
    assert!(
        store.inner.cache.allocated_bytes() > 0,
        "Cache header actually exists before compression"
    );
    let mut serial = 408;
    let mut missing = HashSet::new();
    let mut latest = None;
    let mut deleted = 0;
    for (round, (algorithm, workers)) in [
        (CompactionAlgorithm::Lookup, 2),
        (CompactionAlgorithm::ScanDedup, 4),
        (CompactionAlgorithm::Lookup, 4),
        (CompactionAlgorithm::ScanDedup, 2),
    ]
    .into_iter()
    .enumerate()
    {
        let before = store.inner.log.frontiers().unwrap();
        let mut options = opts(algorithm, before.tail, workers);
        options.checkpoint = true;
        options.shift_begin = true;
        let ticket = store.maintenance().compact(options).unwrap();
        if round == 0 || round == 3 {
            let key = if round == 0 { 5 } else { 7 };
            match session
                .delete(Serial(serial), Delete(key), Default::default())
                .unwrap()
            {
                Submission::Ready(result) => {
                    result.unwrap();
                }
                Submission::Pending(mut request) => {
                    session.wait(&mut request, deadline()).unwrap().unwrap();
                }
            }
            missing.insert(key);
        } else {
            put(&mut session, serial, 5);
            missing.remove(&5);
        }
        serial += 1;
        let result = session.wait_maintenance(&ticket, deadline()).unwrap();
        let report = result.as_ref().as_ref().unwrap();
        let checkpoint = report.checkpoint.as_ref().unwrap();
        let gc = report.gc.as_ref().unwrap();
        assert_eq!(checkpoint.begin, before.begin);
        assert_eq!(gc.begin, before.tail);
        assert!(gc.index_cleaned);
        deleted += gc.deleted_segments;
        assert_eq!(
            checkpoint
                .sessions
                .iter()
                .find(|cut| cut.session == session.id())
                .unwrap()
                .serial,
            Serial(serial - 1)
        );
        latest = Some(checkpoint.clone());
    }
    assert!(
        deleted > 0,
        "The repetitive process actually releases the work segment"
    );
    let checkpoint = latest.unwrap();
    let session_id = session.id();
    let store_id = store.id();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    drop(session);
    drop(store);
    let (reader, _) = recover_store(
        config,
        crate::api::maintenance::RecoverySet {
            store: store_id,
            index: checkpoint.token,
            log: checkpoint.token,
        },
    )
    .unwrap();
    let mut read = reader.continue_session(session_id).unwrap().session;
    for key in 0..400 {
        assert_eq!(
            read_value(&mut read, serial + key, key),
            if missing.contains(&key) {
                None
            } else {
                Some(key)
            }
        );
    }
    read.close(deadline()).unwrap();
    reader.shutdown(deadline()).unwrap();
}
