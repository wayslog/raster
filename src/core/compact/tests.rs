use super::*;
use crate::core::hlog::LogConfig;
use crate::core::kv::{ReadContext, UpsertContext, ValueSlot};
use crate::core::maintenance::CompactionWork;
use crate::core::state::Phase;
use crate::core::store::StoreConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Put(u64);
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<u64> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl UpsertContext<u64, AtomicU64> for Put {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(7));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(7, Ordering::Relaxed);
        true
    }
}

fn store(name: &str) -> FasterKv<u64, AtomicU64> {
    store_with_device(name, Arc::new(crate::core::device::NullDevice))
}

fn store_with_device(
    name: &str,
    device: Arc<dyn crate::core::device::Device>,
) -> FasterKv<u64, AtomicU64> {
    FasterKv::with_device(
        StoreConfig {
            index_buckets: 16,
            log_size_budget: 1,
            checkpoint_dir: Some(
                std::env::temp_dir().join(format!("raster-review-{name}-{}", std::process::id())),
            ),
            log: LogConfig {
                page_bits: 12,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
            ..Default::default()
        },
        device,
    )
}

#[derive(Default)]
struct BytesDevice(std::sync::Mutex<Vec<u8>>);
impl crate::core::device::Device for BytesDevice {
    unsafe fn write_async(
        &self,
        offset: u64,
        ptr: *const u8,
        len: usize,
        done: crate::core::device::WriteCallback,
    ) {
        let start = offset as usize;
        {
            let mut data = self.0.lock().unwrap();
            if data.len() < start + len {
                data.resize(start + len, 0);
            }
            // SAFETY: the caller keeps the source readable through this callback.
            data[start..start + len]
                .copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, len) });
        }
        done(Ok(()));
    }
    fn read_async(&self, offset: u64, len: usize, done: crate::core::device::ReadCallback) {
        let start = offset as usize;
        let result = self
            .0
            .lock()
            .unwrap()
            .get(start..start + len)
            .map(|bytes| bytes.to_vec())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        done(result);
    }
}

#[test]
fn prepare_copy_transfers_old_lock_to_pending() {
    let store = store("prepare-copy");
    let slot = store.epoch.reserve_helper().unwrap();
    store.epoch.protect_and_drain(&slot);
    let key = 7u64;
    let source = Address::new(0, 64);
    let newer = Address::new(0, 88);
    assert!(store.index.find_or_create(key.hash()).try_update(newer));
    store.hlog.head.store(Address::new(1, 0));
    store.checkpoint_hybrid_log(None).unwrap();
    let state = store.system_state();
    assert_eq!(state.phase, Phase::Prepare);
    let words = [0, key, 7];
    let request = CopyRequest {
        words: &words,
        types: std::marker::PhantomData,
        search_min: source,
    };
    let result = store.conditional_insert(
        &slot,
        &request,
        OpMode {
            phase: Phase::Prepare,
            version: state.version,
            plain: false,
            holds_old: false,
        },
    );
    let marked = matches!(result, Outcome::Disk { marked: true, .. });
    let locks = store.locks().unwrap();
    let old_locked = locks.old_locked(key.hash());
    let new_allowed = locks.try_lock_new(key.hash());
    if new_allowed {
        locks.unlock_new(key.hash());
    }
    if old_locked {
        locks.unlock_old(key.hash());
    }
    store.epoch.release(slot);
    assert!(
        marked,
        "a PREPARE copy must keep its old-side lock while pending"
    );
    assert!(
        old_locked && !new_allowed,
        "v+1 must wait for the pending copy"
    );
}

#[test]
fn exhausted_assistance_does_not_wait_for_checkpoint() {
    assistance_during_checkpoint(true);
}

#[test]
fn completed_copy_batch_does_not_wait_for_checkpoint() {
    assistance_during_checkpoint(false);
}

fn assistance_during_checkpoint(exhausted: bool) {
    let store = store(if exhausted {
        "empty-assistance"
    } else {
        "copy-assistance"
    });
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let returned_early = std::thread::scope(|scope| {
        let mut blocker = store.start_session();
        assert_eq!(blocker.upsert(&mut Put(0), 0), Status::Ok);
        let until = store.hlog.tail();
        store.hlog.shift_read_only_to(until);
        while store.hlog.safe_read_only.load() < until {
            blocker.refresh();
        }
        let worker_store = &store;
        let worker = scope.spawn(move || {
            let mut writer = worker_store.start_session();
            ready_tx.send(()).unwrap();
            start_rx.recv().unwrap();
            writer.refresh();
            let result = writer.upsert(&mut Put(1), 1);
            done_tx.send(result).unwrap();
            writer.complete_pending(true);
        });
        ready_rx.recv().unwrap();
        let begin = store.hlog.begin.load();
        *store.maintenance.work.lock().unwrap() = Some(Arc::new(CompactionWork::new(
            begin,
            if exhausted { begin } else { until },
        )));
        store.checkpoint_hybrid_log(None).unwrap();
        start_tx.send(()).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(200));
        let deadline = Instant::now() + Duration::from_secs(3);
        while store.system_state().phase != Phase::Rest {
            blocker.complete_pending(false);
            assert!(
                Instant::now() < deadline,
                "checkpoint did not finish after release"
            );
            std::thread::yield_now();
        }
        worker.join().unwrap();
        store.maintenance.work.lock().unwrap().take();
        early
    });
    assert_eq!(returned_early.unwrap(), Status::Ok);
}

#[derive(Clone, Default)]
struct Read {
    key: u64,
    completed: std::rc::Rc<std::cell::Cell<bool>>,
    status: std::rc::Rc<std::cell::Cell<Option<Status>>>,
}
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<u64> for Read {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Read {
    fn get(&mut self, _: &AtomicU64) {}
    fn on_complete(&mut self, status: Status) {
        self.completed.set(true);
        self.status.set(Some(status));
    }
}

#[test]
fn assistance_does_not_complete_user_requests() {
    let store = store_with_device("callback-isolation", Arc::new(BytesDevice::default()));
    let mut writer = store.start_session();
    for key in 0..600 {
        assert_eq!(writer.upsert(&mut Put(key), key), Status::Ok);
    }
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(until);
    while store.hlog.safe_read_only.load() < until {
        writer.refresh();
    }
    // Keep a completed device read queued while enough real records are
    // available to hit copy_pages' periodic completion call.
    store.hlog.head.store(until);
    let completed = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut read = Read {
        key: 0,
        completed: completed.clone(),
        ..Default::default()
    };
    assert_eq!(writer.read(&mut read, 600), Status::Pending);
    let begin = store.hlog.begin.load();
    *store.maintenance.work.lock().unwrap() = Some(Arc::new(CompactionWork::new(begin, until)));
    assert_eq!(writer.upsert(&mut Put(1000), 601), Status::Ok);
    assert!(
        !completed.get(),
        "upsert must not invoke a user's read callback"
    );
    assert_eq!(writer.pending_count(), 1);
    assert!(writer.complete_pending(true));
    assert!(completed.get());
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct CollidingKey(u64);
// SAFETY: This integer header has a fixed, resource-free, relocatable layout.
unsafe impl Key for CollidingKey {
    fn hash(&self) -> u64 {
        0
    }
}
#[derive(Clone)]
struct CollidingPut(CollidingKey);
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<CollidingKey> for CollidingPut {
    fn key(&self) -> &CollidingKey {
        &self.0
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl UpsertContext<CollidingKey, u64> for CollidingPut {
    fn put(&mut self, slot: ValueSlot<'_, u64>) {
        slot.write(7);
    }
    fn put_atomic(&mut self, _: &u64) -> bool {
        false
    }
}

#[test]
fn prepare_pending_copy_and_new_version_writer_finish_checkpoint() {
    let store = FasterKv::<CollidingKey, u64>::with_device(
        StoreConfig {
            index_buckets: 16,
            checkpoint_dir: Some(
                std::env::temp_dir()
                    .join(format!("raster-pending-copy-cpr-{}", std::process::id())),
            ),
            log: LogConfig {
                page_bits: 12,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
            ..Default::default()
        },
        Arc::new(BytesDevice::default()),
    );
    let mut copier = store.start_session();
    let source = store.hlog.tail();
    assert_eq!(
        copier.upsert(&mut CollidingPut(CollidingKey(0)), 0),
        Status::Ok
    );
    assert_eq!(
        copier.upsert(&mut CollidingPut(CollidingKey(1)), 1),
        Status::Ok
    );
    let observed = store.index.find(0).unwrap().entry.address();
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(until);
    while store.hlog.safe_read_only.load() < until {
        copier.refresh();
    }
    let words = {
        let mut scanner = store.scan(source, until);
        let mut page = scanner.next_page().unwrap().unwrap();
        page.records().next().unwrap().words.to_vec()
    };
    store.hlog.head.store(until);
    let mut writer = store.start_session();
    store.checkpoint_hybrid_log(None).unwrap();
    copier.refresh();
    assert_eq!(store.system_state().phase, Phase::Prepare);
    let batch = std::rc::Rc::new(CopyBatch::default());
    assert_eq!(
        copier.copy_record(
            CopyRequest {
                words: &words,
                search_min: source,
                types: std::marker::PhantomData,
            },
            &batch
        ),
        Status::Pending
    );
    writer.refresh();
    assert_eq!(store.system_state().phase, Phase::InProgress);
    // The refresh that advances the global state still leaves its caller in
    // PREPARE; enter IN_PROGRESS locally before issuing the v+1 operation.
    writer.refresh();
    let update = writer.upsert(&mut CollidingPut(CollidingKey(1)), 2);
    assert!(matches!(update, Status::Ok | Status::Pending));
    // BytesDevice completes every request inline. Bound state-machine passes
    // instead of including filesystem metadata persistence in a wall-clock gate.
    for _ in 0..64 {
        if store.system_state().phase == Phase::Rest {
            break;
        }
        copier.complete_pending(false);
        writer.complete_pending(false);
    }
    let final_phase = store.system_state().phase;
    let pending_copies = batch.pending.get();
    let finished = final_phase == Phase::Rest;
    if !finished {
        // Restore the old chain only to release both sessions after a red run;
        // the verdict above still records the original checkpoint livelock.
        assert!(store.index.find(0).unwrap().try_update(observed));
        while store.system_state().phase != Phase::Rest {
            copier.complete_pending(false);
            writer.complete_pending(false);
        }
    }
    assert_eq!(batch.pending.get(), 0);
    assert_eq!(batch.failure.get(), None);
    assert!(
        finished,
        "PREPARE copy did not finish after 64 passes: phase={final_phase:?}, pending_copies={pending_copies}"
    );
}

#[test]
fn assistance_uses_existing_epoch_protection_when_slots_are_full() {
    let store = store("full-slots-assistance");
    let mut writer = store.start_session();
    assert_eq!(writer.upsert(&mut Put(0), 0), Status::Ok);
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(until);
    while store.hlog.safe_read_only.load() < until {
        writer.refresh();
    }
    let _others: Vec<_> = (1..crate::core::epoch::MAX_SLOTS)
        .map(|_| store.start_session())
        .collect();
    *store.maintenance.work.lock().unwrap() = Some(Arc::new(CompactionWork::new(
        store.hlog.begin.load(),
        until,
    )));
    assert_eq!(writer.upsert(&mut Put(1), 1), Status::Ok);
    let active = store.maintenance.work.lock().unwrap().take().unwrap();
    assert_eq!(*active.failure.lock().unwrap(), None);
    assert_eq!(active.assistants.load(Ordering::Acquire), 0);
}

#[test]
fn session_capacity_is_reported_without_panicking() {
    let store = store("session-capacity");
    let _sessions: Vec<_> = (0..crate::core::epoch::MAX_SLOTS)
        .map(|_| store.start_session())
        .collect();
    assert!(store.try_start_session().is_none());
    assert_eq!(store.index_distribution(), Err(Status::OutOfMemory));
}

#[test]
fn compaction_falls_back_to_coordinator_when_sessions_are_full() {
    let store = store("full-slots-coordinator");
    let mut coordinator = store.start_session();
    assert_eq!(coordinator.upsert(&mut Put(0), 0), Status::Ok);
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(until);
    while store.hlog.safe_read_only.load() < until {
        coordinator.refresh();
    }
    let mut others: Vec<_> = (1..crate::core::epoch::MAX_SLOTS)
        .map(|_| store.start_session())
        .collect();
    let mut scan = store.scan(store.hlog.begin.load(), until);
    assert!(matches!(scan.next_page(), Err(Status::OutOfMemory)));
    assert_eq!(
        coordinator.compact(until, false, 4, false).unwrap().until,
        until
    );
    drop(others.pop());
    assert!(
        scan.next_page().unwrap().is_some(),
        "scanner can retry after a slot is released"
    );
}

#[derive(Default)]
struct ShortReads(std::sync::atomic::AtomicUsize);
impl crate::core::device::Device for ShortReads {
    unsafe fn write_async(
        &self,
        _: u64,
        _: *const u8,
        _: usize,
        done: crate::core::device::WriteCallback,
    ) {
        done(Ok(()));
    }
    fn read_async(&self, _: u64, len: usize, done: crate::core::device::ReadCallback) {
        if self.0.fetch_add(1, Ordering::Relaxed) < 5 {
            let mut bytes = vec![0; len - 1];
            bytes[0] = 1;
            done(Ok(bytes));
        } else {
            // Bound the original infinite reread so the regression exits red.
            done(Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)));
        }
    }
}

#[test]
fn short_device_read_finishes_without_reissuing_the_same_prefix() {
    let device = Arc::new(ShortReads::default());
    let store = store_with_device("short-device-read", device.clone());
    let mut session = store.start_session();
    assert_eq!(session.upsert(&mut Put(0), 0), Status::Ok);
    store.hlog.head.store(store.hlog.tail());
    let completed = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut read = Read {
        key: 0,
        completed: completed.clone(),
        ..Default::default()
    };
    assert_eq!(session.read(&mut read, 1), Status::Pending);
    assert!(session.complete_pending(true));
    assert!(completed.get());
    assert_eq!(
        device.0.load(Ordering::Relaxed),
        1,
        "short reads must be terminal I/O errors"
    );
    assert_eq!(read.status.get(), Some(Status::IoError));
}

#[test]
fn checkpoint_start_failure_reports_completed_compaction() {
    let store = store("checkpoint-start-failure");
    let root = &store.dirs.as_ref().unwrap().root;
    let _ = std::fs::remove_dir_all(root);
    std::fs::write(root, b"block checkpoint directories").unwrap();
    let mut session = store.start_session();
    assert_eq!(session.upsert(&mut Put(0), 0), Status::Ok);
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(until);
    while store.hlog.safe_read_only.load() < until {
        session.refresh();
    }
    let result = session.compact(until, true, 1, true);
    std::fs::remove_file(root).unwrap();
    assert_eq!(
        store.hlog.begin.load(),
        until,
        "truncation already happened"
    );
    assert!(
        result.is_ok(),
        "completed compaction must be reported separately from checkpoint failure"
    );
    let progress = result.unwrap();
    assert_eq!(progress.until, until);
    assert_eq!(progress.begin_address, until);
    assert_eq!(progress.checkpoint, Some(Err(Status::Aborted)));
}

#[test]
fn checkpoint_callback_failure_preserves_compaction_progress() {
    let store = store("checkpoint-callback-failure");
    let mut blocker = store.start_session();
    assert_eq!(blocker.upsert(&mut Put(0), 0), Status::Ok);
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(until);
    while store.hlog.safe_read_only.load() < until {
        blocker.refresh();
    }
    let done = std::sync::atomic::AtomicBool::new(false);
    let progress = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let mut session = store.start_session();
            let result = session.compact(until, true, 1, true);
            done.store(true, Ordering::Release);
            result
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut injected = false;
        while !done.load(Ordering::Acquire) {
            // Install the fault before this session acknowledges the earliest
            // checkpoint barrier. A refresh can enter and finish PREPARE in
            // one call, so polling only that phase can miss the injection.
            if !injected
                && matches!(
                    store.system_state().phase,
                    Phase::PrepIndexCheckpoint | Phase::IndexCheckpoint | Phase::Prepare
                )
            {
                let token = store.cpr.lock().unwrap().token;
                let path = store.dirs.as_ref().unwrap().log(token).join("info.dat");
                std::fs::create_dir_all(path).unwrap();
                injected = true;
            }
            blocker.complete_pending(false);
            assert!(
                Instant::now() < deadline,
                "checkpoint did not report failure"
            );
            std::thread::yield_now();
        }
        let result = worker.join().unwrap().unwrap();
        assert!(
            injected,
            "checkpoint must reach the injected metadata failure"
        );
        result
    });
    assert_eq!(progress.until, until);
    assert_eq!(progress.begin_address, until);
    assert_eq!(progress.checkpoint, Some(Err(Status::IoError)));
    assert_eq!(store.hlog.begin.load(), until);
}

#[test]
fn compaction_rejects_partial_record_boundary_before_copying() {
    let store = store("partial-compaction-boundary");
    let mut session = store.start_session();
    assert_eq!(session.upsert(&mut Put(0), 0), Status::Ok);
    assert_eq!(session.upsert(&mut Put(1), 1), Status::Ok);
    let begin = store.hlog.begin.load();
    let tail = store.hlog.tail();
    store.hlog.shift_read_only_to(tail);
    while store.hlog.safe_read_only.load() < tail {
        session.refresh();
    }
    let result = session.compact(begin.add(40), true, 1, false);
    assert_eq!(result, Err(Status::InvalidArgument));
    assert_eq!(store.hlog.begin.load(), begin);
    assert_eq!(store.hlog.tail(), tail);
}
