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
impl KeyContext<u64> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertContext<u64, AtomicU64> for Put {
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

#[derive(Clone)]
struct Read {
    key: u64,
    completed: std::rc::Rc<std::cell::Cell<bool>>,
}
impl KeyContext<u64> for Read {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Read {
    fn get(&mut self, _: &AtomicU64) {}
    fn on_complete(&mut self, _: Status) {
        self.completed.set(true);
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
impl Key for CollidingKey {
    fn hash(&self) -> u64 {
        0
    }
}
#[derive(Clone)]
struct CollidingPut(CollidingKey);
impl KeyContext<CollidingKey> for CollidingPut {
    fn key(&self) -> &CollidingKey {
        &self.0
    }
}
impl UpsertContext<CollidingKey, u64> for CollidingPut {
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
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline && store.system_state().phase != Phase::Rest {
        copier.complete_pending(false);
        writer.complete_pending(false);
    }
    let finished = store.system_state().phase == Phase::Rest;
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
        "PREPARE copy remained pending behind a v+1 record"
    );
}
