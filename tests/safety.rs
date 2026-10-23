//! Safe log handles and typed scan views, also exercised under Miri.
use raster::core::address::Address;
use raster::core::device::{Device, NullDevice, ReadCallback, WriteCallback};
use raster::core::epoch::LightEpoch;
use raster::core::hlog::{HybridLog, LogConfig};
use raster::core::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn config() -> LogConfig {
    LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    }
}

#[derive(Default)]
struct DeferredWrites {
    pending: Mutex<Vec<WriteCallback>>,
    writes: AtomicUsize,
}

impl Device for DeferredWrites {
    unsafe fn write_async(&self, _: u64, _: *const u8, _: usize, done: WriteCallback) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().unwrap().push(done);
    }

    fn read_async(&self, _: u64, _: usize, done: ReadCallback) {
        done(Err(std::io::Error::other("write-only test device")));
    }
}

impl DeferredWrites {
    fn complete_with(&self, result: impl Fn() -> std::io::Result<()>) {
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        for done in pending {
            done(result());
        }
    }
}

#[test]
fn deferred_epoch_action_survives_dropped_log_handle() {
    let epoch = Arc::new(LightEpoch::new());
    let slot = epoch.reserve().unwrap();
    epoch.protect(&slot);
    let log = Box::new(HybridLog::new(
        config(),
        epoch.clone(),
        Arc::new(NullDevice),
    ));
    let _ = log.allocate(24);
    log.shift_read_only_to(log.tail());
    drop(log);
    epoch.protect_and_drain(&slot);
    epoch.release(slot);
}

#[test]
fn pending_flush_survives_moving_the_log_handle() {
    fn begin_flush(device: Arc<DeferredWrites>) -> HybridLog {
        let log = HybridLog::new(config(), Arc::new(LightEpoch::new()), device);
        let _ = log.allocate(448);
        log.shift_read_only_to(log.tail());
        log
    }
    let device = Arc::new(DeferredWrites::default());
    let log = Box::new(begin_flush(device.clone()));
    assert_eq!(device.writes.load(Ordering::Relaxed), 1);
    device.complete_with(|| Ok(()));
    assert_eq!(log.flushed_until.load(), Address::new(0, 512));
}

#[test]
fn asynchronous_flush_failure_releases_pending_writes() {
    let device = Arc::new(DeferredWrites::default());
    let log = HybridLog::new(config(), Arc::new(LightEpoch::new()), device.clone());
    let _ = log.allocate(448);
    log.shift_read_only_to(log.tail());
    let _ = log.allocate(512);
    log.shift_read_only_to(log.tail());
    assert_eq!(device.writes.load(Ordering::Relaxed), 2);
    assert!(!log.flush_failed());
    std::thread::scope(|scope| {
        scope.spawn(|| {
            device.complete_with(|| Err(std::io::Error::other("injected async write failure")));
        });
    });
    assert!(log.flush_failed());
    assert_eq!(log.flushed_until.load(), Address::new(0, 64));
    // Drop must observe that both failed writes have completed.
    drop(log);
}

#[test]
fn out_of_page_pointer_is_safe_to_compute_without_dereferencing() {
    let log = HybridLog::new(config(), Arc::new(LightEpoch::new()), Arc::new(NullDevice));
    let base = log.get(Address::new(0, 0));
    assert_eq!(log.get(Address::new(0, 8192)), base.wrapping_add(8192));
}

#[test]
fn scanned_atomic_values_can_be_shared_and_updated() {
    let store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
        index_buckets: 16,
        log: config(),
        ..Default::default()
    });
    let mut bytes = vec![0u8; 88];
    bytes[64..72].copy_from_slice(&(1u64 << 48).to_ne_bytes());
    bytes[72..80].copy_from_slice(&7u64.to_ne_bytes());
    bytes[80..88].copy_from_slice(&42u64.to_ne_bytes());
    store.hlog.load_page(0, &bytes);
    let begin = Address::new(0, 64);
    let end = Address::new(0, 88);
    store.hlog.recovery_reset(begin, begin, end);
    let mut scanner = store.scan(begin, end);
    let mut page = scanner.next_page().unwrap().unwrap();
    let mut records = page.records();
    let record = records.next().unwrap();
    assert_eq!(record.size(), 24);
    assert_eq!(*record.key, 7);
    let value = record.value.unwrap();
    value.store(99, Ordering::Relaxed);
    std::thread::scope(|scope| {
        scope.spawn(|| record.value.unwrap().fetch_add(1, Ordering::Relaxed));
        scope.spawn(|| value.fetch_add(1, Ordering::Relaxed));
    });
    assert_eq!(value.load(Ordering::Relaxed), 101);
}
