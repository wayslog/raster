//! Resident-store behavior, ported from FASTER `cc/test/in_memory_test.cc`.
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::sync::atomic::{AtomicU64, Ordering};

type Store = FasterKv<u64, AtomicU64>;

#[derive(Clone)]
struct Op {
    key: u64,
    input: u64,
    output: u64,
}
impl Op {
    fn new(key: u64, input: u64) -> Self {
        Self {
            key,
            input,
            output: 0,
        }
    }
}
impl ReadContext<u64, AtomicU64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn get(&mut self, value: &AtomicU64) {
        self.output = value.load(Ordering::Relaxed);
    }
}
impl UpsertContext<u64, AtomicU64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn put(&mut self) -> AtomicU64 {
        AtomicU64::new(self.input)
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::Relaxed);
        true
    }
}
impl RmwContext<u64, AtomicU64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn rmw_initial(&mut self) -> AtomicU64 {
        AtomicU64::new(self.input)
    }
    fn rmw_copy(&mut self, old: &AtomicU64) -> AtomicU64 {
        AtomicU64::new(old.load(Ordering::Relaxed) + self.input)
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        value.fetch_add(self.input, Ordering::Relaxed);
        true
    }
}
impl DeleteContext<u64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
}

fn small_store(index_buckets: u64) -> Store {
    Store::new(StoreConfig {
        index_buckets,
        log: LogConfig {
            page_bits: 16,
            memory_pages: 64,
            mutable_fraction: 0.5,
        },
    })
}

fn read(session: &mut Session<'_, u64, AtomicU64>, key: u64) -> Option<u64> {
    let mut op = Op::new(key, 0);
    match session.read(&mut op, 0) {
        Status::Ok => Some(op.output),
        Status::NotFound => None,
        other => panic!("unexpected read status {other:?}"),
    }
}

#[test]
fn upsert_then_read() {
    let store = small_store(128);
    let mut session = store.start_session();
    for key in 0..10_000 {
        assert_eq!(session.upsert(&mut Op::new(key, key * 3), key), Status::Ok);
    }
    for key in 0..10_000 {
        assert_eq!(read(&mut session, key), Some(key * 3));
    }
    assert_eq!(read(&mut session, 10_000), None);
}

#[test]
fn records_age_into_read_only_region_and_copy_on_update() {
    let store = small_store(1 << 12);
    let mut session = store.start_session();
    // 24-byte records: ~2700 per 64 KiB page; push early keys out of the
    // mutable half of the in-memory window but keep them resident.
    for round in 0..2u64 {
        for key in 0..20_000 {
            assert_eq!(session.upsert(&mut Op::new(key, round), key), Status::Ok);
        }
        session.refresh();
    }
    for key in 0..20_000 {
        assert_eq!(session.rmw(&mut Op::new(key, 5), key), Status::Ok);
        assert_eq!(read(&mut session, key), Some(6));
    }
}

#[test]
fn rmw_initial_copy_and_in_place() {
    let store = small_store(64);
    let mut session = store.start_session();
    for _ in 0..3 {
        for key in 0..1000 {
            assert_eq!(session.rmw(&mut Op::new(key, key + 1), 0), Status::Ok);
        }
    }
    for key in 0..1000 {
        assert_eq!(read(&mut session, key), Some(3 * (key + 1)));
    }
}

#[test]
fn delete_in_place_and_reinsert() {
    let store = small_store(64);
    let mut session = store.start_session();
    for key in 0..100 {
        let _ = session.upsert(&mut Op::new(key, 1), 0);
    }
    for key in (0..100).step_by(2) {
        assert_eq!(session.delete(&Op::new(key, 0), 0), Status::Ok);
    }
    assert_eq!(session.delete(&Op::new(1000, 0), 0), Status::NotFound);
    for key in 0..100 {
        assert_eq!(read(&mut session, key), (key % 2 == 1).then_some(1));
    }
    assert_eq!(session.rmw(&mut Op::new(0, 9), 0), Status::Ok);
    assert_eq!(read(&mut session, 0), Some(9));
}

#[test]
fn concurrent_rmw_counts_every_increment() {
    let store = small_store(1 << 10);
    const THREADS: u64 = 4;
    const KEYS: u64 = 512;
    const ROUNDS: u64 = 200;
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                let mut session = store.start_session();
                for round in 0..ROUNDS {
                    for key in 0..KEYS {
                        assert_eq!(session.rmw(&mut Op::new(key, 1), round), Status::Ok);
                    }
                    session.refresh();
                }
            });
        }
    });
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(read(&mut session, key), Some(THREADS * ROUNDS));
    }
}

#[test]
fn concurrent_upserts_on_new_keys_keep_one_chain_per_key() {
    let store = small_store(16);
    std::thread::scope(|scope| {
        for t in 0..4u64 {
            let store = &store;
            scope.spawn(move || {
                let mut session = store.start_session();
                for key in 0..5000 {
                    let _ = session.upsert(&mut Op::new(key, t), key);
                    if key % 256 == 0 {
                        session.refresh();
                    }
                }
            });
        }
    });
    let mut session = store.start_session();
    for key in 0..5000 {
        assert!(read(&mut session, key).is_some_and(|v| v < 4));
    }
}
