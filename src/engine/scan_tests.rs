//! Bounded read-ahead for public scans,failure,close with Drop life cycle;Controlled devices still perform real file byte operations.
use super::*;
use crate::{
    RasterKV, Submission,
    api::{operation::*, session::SessionOptions},
    config::Config,
    device::{
        memory::{MemoryDevice, MemoryFault},
        *,
    },
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
use std::sync::atomic::AtomicUsize;
type TestSchema = SchemaPair<U64Key, AtomicU64Value>;
struct Control {
    device: MemoryDevice,
    paused: AtomicBool,
    reads: AtomicUsize,
    short: AtomicBool,
    fail: AtomicBool,
}
struct Factory(Arc<Control>);
struct Controlled(Arc<Control>);
impl DeviceFactory for Factory {
    fn open(&self, _: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(Controlled(self.0.clone())))
    }
}
impl Device for Controlled {
    fn capabilities(&self) -> DeviceCapabilities {
        self.0.device.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let read = matches!(request.operation, IoOperation::Read { .. });
        if read && self.0.short.swap(false, Ordering::SeqCst) {
            self.0.device.inject_next(MemoryFault::Short(1)).unwrap();
        }
        if read && self.0.fail.swap(false, Ordering::SeqCst) {
            self.0
                .device
                .inject_next(MemoryFault::Fail(std::io::ErrorKind::Other))
                .unwrap();
        }
        let result = self.0.device.submit(request);
        if read && result.is_ok() {
            self.0.reads.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        if self.0.paused.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.0.device.poll(budget, output)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.0.device.shutdown(deadline)
    }
}
#[derive(Debug)]
struct Put(u64);
impl Keyed<TestSchema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<TestSchema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, TestSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + std::time::Duration::from_secs(5))
}
fn setup() -> (RasterKV<TestSchema>, Arc<Control>) {
    let control = Arc::new(Control {
        device: MemoryDevice::new(512, 8 << 20).unwrap(),
        paused: false.into(),
        reads: 0.into(),
        short: false.into(),
        fail: false.into(),
    });
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    // Page frame span,Resuming a short read must continue processing the end of the frame and the next segment.
    config.storage.segment_bytes = 4096;
    config.scan.max_scanners = 1;
    config.scan.timeout = std::time::Duration::from_secs(1);
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(Factory(control.clone())))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        let submission = session.upsert(Serial(key), Put(key)).unwrap();
        match submission {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
    }
    session.close(deadline()).unwrap();
    assert!(store.inner.log.frontiers().unwrap().head.0 >= 3 * 4096);
    (store, control)
}
fn options(store: &RasterKV<TestSchema>, mode: Buffering) -> ScanOptions {
    ScanOptions {
        begin: LogAddress(0),
        end: store.inner.log.frontiers().unwrap().tail,
        buffering: mode,
    }
}
#[test]
fn three_types_of_pre_reading_accept_one_two_and_three_pages_of_reading_respectively_and_no_omissions_are_missed_during_timeout_recovery()
 {
    let (store, control) = setup();
    for (mode, frames) in [
        (Buffering::Unbuffered, 1),
        (Buffering::SinglePage, 2),
        (Buffering::DoublePage, 3),
    ] {
        control.reads.store(0, Ordering::SeqCst);
        let mut scan = store.scan(options(&store, mode)).unwrap();
        control.paused.store(true, Ordering::SeqCst);
        assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
        assert_eq!(control.reads.load(Ordering::SeqCst), frames);
        control.paused.store(false, Ordering::SeqCst);
        let mut keys = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            assert_eq!(record.value, Some(record.key));
            keys.push(record.key);
        }
        assert_eq!(keys, (0..400).collect::<Vec<_>>());
        scan.close().unwrap();
    }
    store.shutdown(deadline()).unwrap();
}
#[test]
fn if_you_give_up_scanning_in_transit_your_quota_will_still_be_occupied_and_you_can_re_register_after_the_return_is_completed()
 {
    let (store, control) = setup();
    let range = options(&store, Buffering::DoublePage);
    let mut scan = store.scan(range).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    drop(scan);
    assert!(matches!(store.scan(range), Err(Error::Busy)));
    assert!(matches!(
        store.shutdown(Deadline(
            Instant::now() + std::time::Duration::from_millis(5)
        )),
        Err(Error::DeadlineExceeded)
    ));
    let reads = control.reads.load(Ordering::SeqCst);
    control.paused.store(false, Ordering::SeqCst);
    let mut next = store.scan(range).unwrap();
    // Abort scanning and only recycle accepted completions,Do not commit remaining reads across frame spans.
    assert_eq!(control.reads.load(Ordering::SeqCst), reads);
    next.close().unwrap();
    store.shutdown(deadline()).unwrap();
    next.close().unwrap();
}
#[test]
fn explicit_shutdown_timeout_continues_shutdown_without_restarting_reading() {
    let (store, control) = setup();
    let mut scan = store.scan(options(&store, Buffering::SinglePage)).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    assert!(matches!(scan.close(), Err(Error::DeadlineExceeded)));
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    let accepted = control.reads.load(Ordering::SeqCst);
    control.paused.store(false, Ordering::SeqCst);
    scan.close().unwrap();
    scan.close().unwrap();
    assert_eq!(control.reads.load(Ordering::SeqCst), accepted);
    store.shutdown(deadline()).unwrap();
}
#[test]
fn the_short_read_continues_across_segments_and_the_read_fails_causing_the_scan_to_fail_to_close_once()
 {
    let (store, control) = setup();
    let range = options(&store, Buffering::Unbuffered);
    let mut scan = store.scan(range).unwrap();
    control.short.store(true, Ordering::SeqCst);
    assert_eq!(scan.next_record().unwrap().unwrap().key, 0);
    assert!(control.reads.load(Ordering::SeqCst) >= 3);
    scan.close().unwrap();
    let mut scan = store.scan(range).unwrap();
    control.fail.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::Io(_))));
    let reads = control.reads.load(Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert_eq!(control.reads.load(Ordering::SeqCst), reads);
    scan.close().unwrap();
    // A scanned file read error does not disguise itself as an entire engine being unavailable.
    let mut other = store.scan(range).unwrap();
    assert_eq!(other.next_record().unwrap().unwrap().key, 0);
    other.close().unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn reject_hot_and_cold_half_record_boundaries_as_well_as_inversions_and_out_of_bounds_before_opening_the_scan()
 {
    let (store, _) = setup();
    let range = options(&store, Buffering::Unbuffered);
    let hot = store.inner.log.frontiers().unwrap().head;
    let (_, bytes) = store
        .inner
        .log
        .snapshot_next(hot, range.end)
        .unwrap()
        .unwrap();
    assert!(!bytes.is_empty());
    // Cold pages start or end at the second byte of the first record,You cannot deliver other records first and then report a boundary error..
    for (begin, end) in [
        (LogAddress(1), range.end),
        (LogAddress(0), LogAddress(1)),
        (range.end, LogAddress(0)),
        (LogAddress(0), range.end.checked_add(1).unwrap()),
    ] {
        assert!(
            store
                .scan(ScanOptions {
                    begin,
                    end,
                    ..range
                })
                .is_err()
        );
    }
    let (address, _) = store
        .inner
        .log
        .snapshot_next(hot, range.end)
        .unwrap()
        .unwrap();
    assert!(
        store
            .scan(ScanOptions {
                begin: address.checked_add(1).unwrap(),
                ..range
            })
            .is_err()
    );
    let mut empty = store
        .scan(ScanOptions {
            begin: LogAddress(1),
            end: LogAddress(1),
            ..range
        })
        .unwrap();
    assert!(empty.next_record().unwrap().is_none());
    assert!(empty.next_record().unwrap().is_none());
    store.shutdown(deadline()).unwrap();
}
#[test]
fn abandon_notifications_are_not_lost_due_to_the_lock_being_held_by_the_finishing_thread() {
    let (store, _) = setup();
    let scan = store.scan(options(&store, Buffering::Unbuffered)).unwrap();
    let shared = scan.state.clone();
    let guard = shared.state.lock().unwrap();
    drop(scan);
    assert!(shared.abandoned.load(Ordering::SeqCst));
    drop(guard);
    store.shutdown(deadline()).unwrap();
}
#[test]
fn scan_configuration_rejects_zero_budget_timeout_and_route_capacity_overflow() {
    let mut config = Config::default();
    config.scan.max_scanners = 0;
    assert!(config.validate().is_err());
    config.scan.max_scanners = usize::MAX;
    assert!(matches!(config.validate(), Err(Error::CapacityExceeded)));
    config.scan.max_scanners = 1;
    config.scan.timeout = std::time::Duration::ZERO;
    assert!(config.validate().is_err());
}

#[test]
fn after_the_scan_returns_all_original_resident_pages_are_allowed_to_be_eliminated_and_the_fixed_range_is_continued_from_disk()
 {
    let (store, _) = setup();
    let range = options(&store, Buffering::DoublePage);
    let mut scan = store.scan(range).unwrap();
    let first = scan.next_record().unwrap().unwrap();
    assert_eq!(first.key, 0);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 400..800 {
        match session.upsert(Serial(key), Put(key)).unwrap() {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
    }
    session.close(deadline()).unwrap();
    assert!(store.inner.log.frontiers().unwrap().head > range.end);
    let mut keys = vec![first.key];
    while let Some(record) = scan.next_record().unwrap() {
        keys.push(record.key);
    }
    assert_eq!(keys, (0..400).collect::<Vec<_>>());
    assert_eq!(first.value, Some(0));
    store.shutdown(deadline()).unwrap();
}

#[derive(Debug)]
struct Replace(u64);
impl Keyed<TestSchema> for Replace {
    fn key(&self) -> &u64 {
        &0
    }
}
impl UpsertOperation<TestSchema> for Replace {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, TestSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        value.view_mut().store(self.0, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(()))
    }
}
#[test]
fn variable_record_scans_do_not_freeze_values_and_old_output_does_not_change_with_in_place_updates()
{
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Replace(7)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let range = options(&store, Buffering::DoublePage);
    let mut old = store.scan(range).unwrap();
    let result = old.next_record().unwrap().unwrap();
    let mut later = store.scan(range).unwrap();
    assert!(matches!(
        session.upsert(Serial(1), Replace(9)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert_eq!(later.next_record().unwrap().unwrap().value, Some(9));
    assert_eq!(result.value, Some(7));
    assert_eq!(store.inner.log.frontiers().unwrap().tail, range.end);
    old.close().unwrap();
    later.close().unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn when_the_logical_boundary_is_moved_forward_the_cached_page_must_also_report_range_truncation() {
    let (store, _) = setup();
    let range = options(&store, Buffering::DoublePage);
    let mut scan = store.scan(range).unwrap();
    let first = scan.next_record().unwrap().unwrap();
    // P7 Not yet open shift_begin;Only legal logical boundary releases are injected here.,Verify Scan Observation Contract.
    store
        .inner
        .log
        .advance_begin_for_scan_test(store.inner.log.frontiers().unwrap().head);
    assert!(matches!(scan.next_record(), Err(Error::RangeTruncated)));
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert!(matches!(store.scan(range), Err(Error::RangeTruncated)));
    assert_eq!(first.value, Some(0));
    scan.close().unwrap();
    store.shutdown(deadline()).unwrap();
}

struct PanicCodec(Arc<AtomicUsize>);
impl crate::schema::value::ValueCodec for PanicCodec {
    type Value = Vec<u8>;
    fn format_id(&self) -> FormatId {
        FormatId(*b"scan-panic-test1")
    }
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(value.clone())
    }
    fn decode(&self, _: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("Inject scan decode panic")
    }
}
type PanicSchema = SchemaPair<U64Key, crate::schema::builtin::SerializedValue<PanicCodec>>;
#[derive(Debug)]
struct PanicPut;
impl Keyed<PanicSchema> for PanicPut {
    fn key(&self) -> &u64 {
        &0
    }
}
impl UpsertOperation<PanicSchema> for PanicPut {
    type Output = ();
    fn replacement(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((vec![1], ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, PanicSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn scan_expert_decode_panic_only_executes_once_and_fails_engine_shutdown() {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        crate::schema::builtin::SerializedValue::new(PanicCodec(calls.clone())),
    ))
    .device(Box::new(crate::device::null::NullDeviceFactory))
    .create()
    .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), PanicPut).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let mut scan = store
        .scan(ScanOptions {
            begin: LogAddress(0),
            end: store.inner.log.frontiers().unwrap().tail,
            buffering: Buffering::Unbuffered,
        })
        .unwrap();
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.start_session(SessionOptions::default()),
        Err(Error::InvalidState(_))
    ));
    scan.close().unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn in_transit_read_protection_spans_mappings_with_abandonment_but_completion_pages_dont_block_deletion_for_long()
 {
    let (store, control) = setup();
    let range = options(&store, Buffering::Unbuffered);
    let mut scan = store.scan(range).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    assert!(matches!(
        store.inner.storage.invalidate(0, Generation(0)),
        Err(Error::Busy)
    ));
    assert!(matches!(
        store.inner.storage.invalidate(1, Generation(0)),
        Err(Error::Busy)
    ));
    drop(scan);
    assert!(matches!(
        store.inner.storage.invalidate(0, Generation(0)),
        Err(Error::Busy)
    ));
    control.paused.store(false, Ordering::SeqCst);
    let mut next = store.scan(range).unwrap();
    assert_eq!(next.next_record().unwrap().unwrap().key, 0);
    // The read lease is released immediately after the current page becomes the owning copy.,Scanner is still active.
    store.inner.storage.invalidate(0, Generation(0)).unwrap();
    next.close().unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn session_polling_facilitates_segmented_read_ahead_of_idle_scans_and_timely_release_of_leases() {
    let (store, control) = setup();
    let mut scan = store.scan(options(&store, Buffering::DoublePage)).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    control.paused.store(false, Ordering::SeqCst);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let stop = deadline();
    loop {
        session
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
        let ready = {
            let state = scan.state.state.lock().unwrap();
            state.pages.len() == 3
                && state
                    .pages
                    .iter()
                    .all(|page| page.cursor.is_some() && page.lease.is_none())
        };
        if ready {
            break;
        }
        assert!(!stop.expired());
    }
    store.inner.storage.invalidate(0, Generation(0)).unwrap();
    scan.close().unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

struct BlockingCodec {
    armed: Arc<AtomicBool>,
    entered: std::sync::mpsc::Sender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl crate::schema::value::ValueCodec for BlockingCodec {
    type Value = u64;
    fn format_id(&self) -> FormatId {
        crate::schema::builtin::U64ValueCodec.format_id()
    }
    fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
        crate::schema::builtin::U64ValueCodec.encode(value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        crate::schema::builtin::U64ValueCodec.decode(bytes)
    }
}
type BlockingSchema = SchemaPair<U64Key, crate::schema::builtin::SerializedValue<BlockingCodec>>;
impl Keyed<BlockingSchema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<BlockingSchema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, BlockingSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn posting_logical_truncation_during_possession_of_value_decoding_must_still_reject_delivery_of_invalid_ranges()
 {
    let armed = Arc::new(AtomicBool::new(false));
    let (entered, seen) = std::sync::mpsc::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let codec = BlockingCodec {
        armed: armed.clone(),
        entered,
        resume: Mutex::new(resume),
    };
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        crate::schema::builtin::SerializedValue::new(codec),
    ))
    .config(config)
    .device(Box::new(crate::device::memory::MemoryDeviceFactory))
    .create()
    .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        match session.upsert(Serial(key), Put(key)).unwrap() {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
    }
    session.close(deadline()).unwrap();
    let frontiers = store.inner.log.frontiers().unwrap();
    assert!(frontiers.head > LogAddress(0));
    let mut scan = store
        .scan(ScanOptions {
            begin: LogAddress(0),
            end: frontiers.tail,
            buffering: Buffering::DoublePage,
        })
        .unwrap();
    armed.store(true, Ordering::SeqCst);
    let worker = std::thread::spawn(move || scan.next_record());
    seen.recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    store.inner.log.advance_begin_for_scan_test(frontiers.head);
    release.send(()).unwrap();
    assert!(matches!(worker.join().unwrap(), Err(Error::RangeTruncated)));
    store.shutdown(deadline()).unwrap();
}

fn compaction_options(
    algorithm: crate::api::maintenance::CompactionAlgorithm,
    until: LogAddress,
) -> crate::api::maintenance::CompactionOptions {
    crate::api::maintenance::CompactionOptions {
        algorithm,
        until,
        workers: 1,
        shift_begin: false,
        checkpoint: false,
    }
}
// Controlled memory device does not have an operating system I/O wait;Capture stuck with limited advancement progress,avoid sharing runner Scheduling change function acceptance.
fn finish_compaction(
    session: &mut crate::api::session::Session<TestSchema>,
    ticket: &crate::api::maintenance::MaintenanceTicket<crate::api::maintenance::CompactionReport>,
) -> crate::api::maintenance::SharedReport<crate::api::maintenance::CompactionReport> {
    for _ in 0..100_000 {
        if let Some(report) = ticket.try_report().unwrap() {
            return report;
        }
        session
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
    }
    panic!("Compression of finite input exceeds boost step limit");
}
#[derive(Debug)]
struct ReadCompaction(u64);
impl Keyed<TestSchema> for ReadCompaction {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl ReadOperation<TestSchema> for ReadCompaction {
    type Output = u64;
    fn read(&mut self, value: crate::schema::ValueRead<'_, TestSchema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
#[test]
fn compressed_polling_does_not_wait_for_the_device_and_short_reads_can_be_offloaded_across_segments_and_users_only_and_can_be_driven_by_sessions_only()
 {
    use crate::api::maintenance::CompactionAlgorithm;
    for algorithm in [CompactionAlgorithm::Lookup, CompactionAlgorithm::ScanDedup] {
        let (store, control) = setup();
        let mut session = store.start_session(Default::default()).unwrap();
        control.reads.store(0, Ordering::SeqCst);
        control.paused.store(true, Ordering::SeqCst);
        control.short.store(true, Ordering::SeqCst);
        let until = store.inner.log.frontiers().unwrap().tail;
        let ticket = store
            .maintenance()
            .compact(compaction_options(algorithm, until))
            .unwrap();
        let budget = PollBudget(std::num::NonZeroUsize::new(1).unwrap());
        for _ in 0..20 {
            store.maintenance().poll(budget).unwrap();
            assert!(ticket.try_report().unwrap().is_none());
        }
        assert_eq!(control.reads.load(Ordering::SeqCst), 1);
        let Submission::Pending(mut user) = session
            .read(Serial(0), ReadCompaction(0), Default::default())
            .unwrap()
        else {
            panic!("User cold reads should hang")
        };
        assert_eq!(control.reads.load(Ordering::SeqCst), 2);
        assert!(matches!(
            session.wait_maintenance(&ticket, Deadline(Instant::now())),
            Err(Error::DeadlineExceeded)
        ));
        control.paused.store(false, Ordering::SeqCst);
        assert_eq!(
            finish_compaction(&mut session, &ticket)
                .as_ref()
                .as_ref()
                .unwrap()
                .copied,
            400
        );
        assert!(matches!(
            session.wait(&mut user, deadline()).unwrap().unwrap(),
            crate::api::completion::Outcome::Success(0)
        ));
        assert_eq!(store.inner.log.frontiers().unwrap().begin, LogAddress(0));
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}
#[test]
fn compressed_read_failure_will_not_be_automatically_retried_and_new_actions_can_be_re_executed_in_the_same_range()
 {
    use crate::api::maintenance::CompactionAlgorithm;
    let (store, control) = setup();
    let mut session = store.start_session(Default::default()).unwrap();
    let until = store.inner.log.frontiers().unwrap().tail;
    control.reads.store(0, Ordering::SeqCst);
    control.fail.store(true, Ordering::SeqCst);
    let ticket = store
        .maintenance()
        .compact(compaction_options(CompactionAlgorithm::Lookup, until))
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*report, Err(Error::CompactionFailed { copied: 0, cause, .. }) if matches!(&**cause, Error::Io(_)))
    );
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);
    assert!(!store.inner.failed.load(Ordering::SeqCst));
    let ticket = store
        .maintenance()
        .compact(compaction_options(CompactionAlgorithm::Lookup, until))
        .unwrap();
    assert_eq!(
        finish_compaction(&mut session, &ticket)
            .as_ref()
            .as_ref()
            .unwrap()
            .copied,
        400
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn public_compression_expert_panic_finalizes_report_once_and_closes_free_task_without_instance_reference_loop()
 {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        crate::schema::builtin::SerializedValue::new(PanicCodec(calls.clone())),
    ))
    .device(Box::new(crate::device::null::NullDeviceFactory))
    .create()
    .unwrap();
    let weak = Arc::downgrade(&store.inner);
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), PanicPut).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let ticket = store
        .maintenance()
        .compact(compaction_options(
            crate::api::maintenance::CompactionAlgorithm::Lookup,
            store.inner.log.frontiers().unwrap().tail,
        ))
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*report, Err(Error::CompactionFailed { copied: 0, cause, .. }) if matches!(&**cause, Error::InvalidState(_)))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(store.maintenance().poll(PollBudget::default()).is_err());
    assert!(Arc::ptr_eq(&report, &ticket.try_report().unwrap().unwrap()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    assert!(weak.upgrade().is_none());
}

#[test]
fn ordinary_cold_reads_protect_all_segments_during_short_reads_across_segments_and_release_them_after_completion()
 {
    let (store, control) = setup();
    let mut session = store.start_session(Default::default()).unwrap();
    control.short.store(true, Ordering::SeqCst);
    control.paused.store(true, Ordering::SeqCst);
    let Submission::Pending(mut ticket) = session
        .read(Serial(0), ReadCompaction(0), Default::default())
        .unwrap()
    else {
        panic!("Cold reads should be suspended")
    };
    for number in [0, 1] {
        assert!(matches!(
            store.inner.storage.invalidate(number, Generation(0)),
            Err(Error::Busy)
        ));
    }
    control.paused.store(false, Ordering::SeqCst);
    store
        .inner
        .io
        .poll(&*store.inner.storage.device, PollBudget::default())
        .unwrap();
    // Done still in session mailbox,The remaining frames after short reading are not collected,Subsequent segments cannot be released early.
    assert!(matches!(
        store.inner.storage.invalidate(1, Generation(0)),
        Err(Error::Busy)
    ));
    assert!(matches!(
        session.wait(&mut ticket, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    for number in [0, 1] {
        store
            .inner
            .storage
            .invalidate(number, Generation(0))
            .unwrap();
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn ordinary_cold_read_failure_ends_with_segment_protection_released_and_failed_requests_not_retained()
 {
    let (store, control) = setup();
    let mut session = store.start_session(Default::default()).unwrap();
    control.fail.store(true, Ordering::SeqCst);
    let Submission::Pending(mut ticket) = session
        .read(Serial(0), ReadCompaction(0), Default::default())
        .unwrap()
    else {
        panic!("Cold reads should be suspended")
    };
    assert!(matches!(
        store.inner.storage.invalidate(0, Generation(0)),
        Err(Error::Busy)
    ));
    assert!(matches!(
        session.wait(&mut ticket, deadline()).unwrap(),
        Err(OperationError {
            cause: Error::Io(_),
            effect: Effect::NotApplied
        })
    ));
    store.inner.storage.invalidate(0, Generation(0)).unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
