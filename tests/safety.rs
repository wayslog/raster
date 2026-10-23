//! Safe log handles and typed scan views, also exercised under Miri.
use raster::core::address::Address;
use raster::core::device::{Device, NullDevice, ReadCallback, WriteCallback};
use raster::core::epoch::LightEpoch;
use raster::core::hlog::{HybridLog, LogConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
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
fn invalid_read_only_boundaries_are_rejected_before_publication() {
    let epoch = Arc::new(LightEpoch::new());
    let slot = epoch.reserve().unwrap();
    epoch.protect(&slot);
    let device = Arc::new(DeferredWrites::default());
    let log = HybridLog::new(config(), epoch.clone(), device.clone());
    let _ = log.allocate(4096 - 64);
    assert!(log.new_page(0));
    let _ = log.allocate(512);
    let tail = log.tail();
    assert_eq!(tail, Address::new(1, 512));
    let before = (
        log.read_only.load(),
        log.safe_read_only.load(),
        log.flushed_until.load(),
    );
    let before_epoch = epoch.current_epoch();
    for address in [
        Address::new(0, 4097),
        Address::new(0, 4096),
        Address::new(1, 520),
        Address::new(2, 0),
        tail.to_read_cache(),
    ] {
        // Keep the epoch protected so a missing check fails without executing
        // the unsafe flush being tested on the unfixed implementation.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            log.shift_read_only_to(address);
        }));
        assert!(
            result.is_err(),
            "invalid read-only boundary was accepted: {address:?}"
        );
        assert_eq!(
            (
                log.read_only.load(),
                log.safe_read_only.load(),
                log.flushed_until.load()
            ),
            before
        );
        assert_eq!(epoch.current_epoch(), before_epoch);
        assert_eq!(log.tail(), tail);
        assert_eq!(device.writes.load(Ordering::Relaxed), 0);
    }
    epoch.release(slot);
}
