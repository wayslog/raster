//! Liveness under high mutable fractions and failed log flushes.
use raster::core::device::{Device, FileDevice, ReadCallback, WriteCallback};
use raster::core::hlog::LogConfig;
use raster::core::kv::ValueSlot;
use raster::core::state::Phase;
use raster::core::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
struct Op {
    key: u64,
    value: u64,
}
// SAFETY: The complete fixed-size key is owned by the context.
unsafe impl KeyContext<u64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
}
// SAFETY: Every destination is fully initialized with a fixed-size value.
unsafe impl UpsertContext<u64, AtomicU64> for Op {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.value));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.value, Ordering::Relaxed);
        true
    }
}
impl ReadContext<u64, AtomicU64> for Op {
    fn get(&mut self, value: &AtomicU64) {
        self.value = value.load(Ordering::Relaxed);
    }
}

/// Runs `body` on a thread and fails if it does not finish in time.
fn within(seconds: u64, body: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        body();
        let _ = done.send(());
    });
    finished
        .recv_timeout(Duration::from_secs(seconds))
        .expect("finished without hanging");
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("raster-robust-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn log_with_default_mutable_fraction_outgrows_memory() {
    within(30, || {
        let store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
            index_buckets: 1 << 10,
            log: LogConfig {
                page_bits: 12,
                memory_pages: 8,
                mutable_fraction: 0.9,
            },
            ..StoreConfig::default()
        });
        let mut session = store.start_session();
        for key in 0..20_000 {
            let mut op = Op { key, value: key };
            assert_eq!(session.upsert(&mut op, key), Status::Ok);
        }
        let mut op = Op {
            key: 19_999,
            value: 0,
        };
        assert_eq!(session.read(&mut op, 0), Status::Ok);
        assert_eq!(op.value, 19_999);
    });
}

#[test]
fn read_cache_with_high_mutable_fraction_keeps_caching_after_it_fills() {
    within(60, || {
        let dir = temp_dir("rc");
        let device = Arc::new(FileDevice::open(&dir, 16, 2).unwrap());
        let store = FasterKv::<u64, AtomicU64>::with_read_cache(
            StoreConfig {
                index_buckets: 1 << 12,
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 16,
                    mutable_fraction: 0.5,
                },
                ..StoreConfig::default()
            },
            device,
            ReadCacheConfig {
                pre_allocate: false,
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 4,
                    mutable_fraction: 0.9,
                },
            },
        );
        const KEYS: u64 = 4_000;
        let mut session = store.start_session();
        for key in 0..KEYS {
            let mut op = Op { key, value: key };
            assert_eq!(session.upsert(&mut op, key), Status::Ok);
        }
        let read = |session: &mut Session<'_, u64, AtomicU64>, key: u64| {
            let mut op = Op { key, value: 0 };
            let status = session.read(&mut op, key);
            if status == Status::Pending {
                assert!(session.complete_pending(true));
            }
            status
        };
        // Cycle far more records through the cache than it can hold.
        for key in 0..KEYS / 2 {
            let _ = read(&mut session, key);
        }
        // A key read again right away must now be served from the cache.
        assert_eq!(read(&mut session, 7), Status::Pending);
        assert_eq!(
            read(&mut session, 7),
            Status::Ok,
            "read cache stopped caching"
        );
        drop(session);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    });
}

/// A device whose writes always fail.
struct FailingWrites;
impl Device for FailingWrites {
    unsafe fn write_async(&self, _: u64, _: *const u8, _: usize, done: WriteCallback) {
        done(Err(std::io::Error::other("injected write failure")));
    }
    fn read_async(&self, _: u64, _: usize, done: ReadCallback) {
        done(Err(std::io::Error::other("nothing stored")));
    }
}

#[test]
fn failed_log_flush_reports_checkpoint_error_without_hanging() {
    let dir = temp_dir("flush");
    let status = Arc::new(Mutex::new(None));
    let reported = status.clone();
    let checkpoint_dir = dir.clone();
    within(30, move || {
        let store = FasterKv::<u64, AtomicU64>::with_device(
            StoreConfig {
                index_buckets: 1 << 10,
                checkpoint_dir: Some(checkpoint_dir),
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 16,
                    mutable_fraction: 0.5,
                },
                ..StoreConfig::default()
            },
            Arc::new(FailingWrites),
        );
        let mut session = store.start_session();
        for key in 0..100 {
            let mut op = Op { key, value: key };
            assert_eq!(session.upsert(&mut op, key), Status::Ok);
        }
        store
            .checkpoint(
                None,
                Some(Arc::new(move |status, _| {
                    *reported.lock().unwrap() = Some(status);
                })),
            )
            .expect("checkpoint starts");
        while store.system_state().phase != Phase::Rest {
            session.refresh();
        }
        drop(session);
        drop(store);
    });
    assert_eq!(*status.lock().unwrap(), Some(Status::IoError));
    std::fs::remove_dir_all(dir).unwrap();
}
