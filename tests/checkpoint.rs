//! CPR checkpoints and recovery, after FASTER `cc/test/recovery_test.h`.
use raster::core::hlog::LogConfig;
use raster::core::state::Phase;
use raster::core::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type Store = FasterKv<u64, AtomicU64>;

#[derive(Clone)]
struct Incr {
    key: u64,
}
impl RmwContext<u64, AtomicU64> for Incr {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn rmw_initial(&mut self) -> AtomicU64 {
        AtomicU64::new(1)
    }
    fn rmw_copy(&mut self, old: &AtomicU64) -> AtomicU64 {
        AtomicU64::new(old.load(Ordering::Relaxed) + 1)
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        value.fetch_add(1, Ordering::Relaxed);
        true
    }
}

fn store(dir: &std::path::Path) -> Store {
    let device = Arc::new(device::FileDevice::open(dir.join("log"), 30, 2).unwrap());
    Store::with_device(
        StoreConfig {
            index_buckets: 1 << 12,
            log: LogConfig {
                page_bits: 16,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
            checkpoint_dir: Some(dir.join("checkpoints")),
        },
        device,
    )
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("raster-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn full_checkpoint_completes_under_concurrent_rmw() {
    let dir = temp_dir("ckpt-smoke");
    let store = store(&dir);
    const THREADS: usize = 4;
    let index_calls = Arc::new(AtomicUsize::new(0));
    let log_serials = Arc::new(Mutex::new(Vec::new()));
    let stop = AtomicBool::new(false);
    let started = AtomicUsize::new(0);
    let token = std::thread::scope(|scope| {
        for t in 0..THREADS {
            let (store, stop, started) = (&store, &stop, &started);
            scope.spawn(move || {
                let mut session = store.start_session();
                started.fetch_add(1, Ordering::SeqCst);
                let mut serial = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    serial += 1;
                    let key = (serial * 7 + t as u64) % 2000;
                    let status = session.rmw(&mut Incr { key }, serial);
                    assert!(matches!(status, Status::Ok | Status::Pending), "{status:?}");
                    if serial.is_multiple_of(64) {
                        session.complete_pending(false);
                    }
                }
                assert!(session.complete_pending(true));
            });
        }
        while started.load(Ordering::SeqCst) != THREADS {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        let calls = index_calls.clone();
        let serials = log_serials.clone();
        let token = store
            .checkpoint(
                Some(Box::new(move |status| {
                    assert_eq!(status, Status::Ok);
                    calls.fetch_add(1, Ordering::SeqCst);
                })),
                Some(Arc::new(move |status, serial| {
                    assert_eq!(status, Status::Ok);
                    serials.lock().unwrap().push(serial);
                })),
            )
            .expect("checkpoint starts");
        assert!(store.checkpoint(None, None).is_none(), "one at a time");
        while store.system_state().phase != Phase::Rest {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        stop.store(true, Ordering::SeqCst);
        token
    });
    assert_eq!(store.system_state().version, 2);
    assert_eq!(index_calls.load(Ordering::SeqCst), 1);
    let serials = log_serials.lock().unwrap();
    assert_eq!(serials.len(), THREADS);
    assert!(serials.iter().all(|&s| s > 0));
    let root = dir.join("checkpoints");
    for file in ["ht.dat", "ofb.dat", "info.dat"] {
        assert!(
            root.join("index-checkpoints")
                .join(token.to_string())
                .join(file)
                .exists()
        );
    }
    let log_dir = root.join("cpr-checkpoints").join(token.to_string());
    assert_eq!(std::fs::read_dir(&log_dir).unwrap().count(), 1 + THREADS);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
