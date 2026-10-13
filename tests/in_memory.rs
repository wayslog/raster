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
impl KeyContext<u64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Op {
    fn get(&mut self, value: &AtomicU64) {
        self.output = value.load(Ordering::Relaxed);
    }
}
impl UpsertContext<u64, AtomicU64> for Op {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::Relaxed);
        true
    }
}
impl RmwContext<u64, AtomicU64> for Op {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(old.load(Ordering::Relaxed) + self.input));
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        value.fetch_add(self.input, Ordering::Relaxed);
        true
    }
}
impl DeleteContext<u64> for Op {}

fn small_store(index_buckets: u64) -> Store {
    Store::new(StoreConfig {
        index_buckets,
        log_size_budget: 0,
        filepath: None,
        pre_allocate: true,
        read_cache: None,
        auto_compaction: None,
        checkpoint_dir: None,
        log: LogConfig {
            page_bits: 16,
            memory_pages: 64,
            mutable_fraction: 0.5,
        },
    })
}

/// Runs an RMW to completion; it may legitimately go pending in the fuzzy region.
fn rmw(session: &mut Session<'_, u64, AtomicU64>, mut op: Op, serial: u64) {
    match session.rmw(&mut op, serial) {
        Status::Ok => {}
        Status::Pending => assert!(session.complete_pending(true)),
        other => panic!("unexpected rmw status {other:?}"),
    }
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
        rmw(&mut session, Op::new(key, 5), key);
        assert_eq!(read(&mut session, key), Some(6));
    }
}

#[test]
fn rmw_initial_copy_and_in_place() {
    let store = small_store(64);
    let mut session = store.start_session();
    for _ in 0..3 {
        for key in 0..1000 {
            rmw(&mut session, Op::new(key, key + 1), 0);
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
    rmw(&mut session, Op::new(0, 9), 0);
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
                        rmw(&mut session, Op::new(key, 1), round);
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

#[test]
fn rmw_in_fuzzy_region_retries_after_every_session_refreshes() {
    let store = Store::new(StoreConfig {
        index_buckets: 1 << 10,
        filepath: None,
        pre_allocate: true,
        read_cache: None,
        auto_compaction: None,
        log_size_budget: 0,
        checkpoint_dir: None,
        log: LogConfig {
            page_bits: 12,
            memory_pages: 8,
            mutable_fraction: 0.5,
        },
    });
    // A protected session that never refreshes pins safe_read_only.
    let mut idle = store.start_session();
    let mut session = store.start_session();
    assert_eq!(session.upsert(&mut Op::new(0, 10), 0), Status::Ok);
    // ~170 records per 4 KiB page: fill five pages so key 0 turns read-only.
    for key in 1..850 {
        assert_eq!(session.upsert(&mut Op::new(key, 1), key), Status::Ok);
    }
    assert_eq!(session.rmw(&mut Op::new(0, 5), 1000), Status::Pending);
    assert!(
        !session.complete_pending(false),
        "idle session still pins the old epoch"
    );
    idle.refresh();
    assert!(session.complete_pending(true));
    assert_eq!(read(&mut session, 0), Some(15));
}
mod common;
use common::varlen::{VarKey, VarRequest, VarValue};

fn variable_store<const C: bool>() -> FasterKv<VarKey<C>, VarValue> {
    FasterKv::new(StoreConfig {
        index_buckets: 256,
        filepath: None,
        pre_allocate: true,
        read_cache: None,
        auto_compaction: None,
        log_size_budget: 0,
        checkpoint_dir: None,
        log: LogConfig {
            page_bits: 16,
            memory_pages: 256,
            mutable_fraction: 0.9,
        },
    })
}

fn variable_keys<const C: bool>() {
    let store = variable_store::<C>();
    let mut session = store.start_session();
    for id in 0..500 {
        let mut op = VarRequest::<C>::new(id, 8 + id as usize % 701, id as usize % 513);
        assert_eq!(session.upsert(&mut op, id), Status::Ok);
    }
    for id in 0..500 {
        let mut op = VarRequest::<C>::new(id, 8 + id as usize % 701, 0);
        let (status, bytes, pending) = op.read(&mut session, id);
        assert_eq!(status, Status::Ok);
        assert_eq!(bytes, vec![88; id as usize % 513]);
        assert!(!pending);
    }
}

#[test]
fn upsert_read_variable_length_keys() {
    variable_keys::<false>();
}
#[test]
fn upsert_read_variable_keys_with_dummy_hash() {
    variable_keys::<true>();
}

#[test]
fn upsert_read_resize_value_concurrent() {
    let store = variable_store::<false>();
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let store = &store;
            scope.spawn(move || {
                let mut session = store.start_session();
                for round in 0..8 {
                    for id in 0..100 {
                        let len = [7, 11, 0, 257, 19, 513, 2, 127][round];
                        let mut op = VarRequest::new(id + worker * 100, 11 + id as usize % 31, len);
                        op.data.fill((round + 1) as u8);
                        assert_eq!(
                            session.upsert(&mut op, (round as u64 * 100 + id) * 2),
                            Status::Ok
                        );
                        let (status, bytes, pending) =
                            op.read(&mut session, (round as u64 * 100 + id) * 2 + 1);
                        assert_eq!(status, Status::Ok);
                        assert_eq!(bytes, vec![(round + 1) as u8; len]);
                        assert!(!pending);
                    }
                    session.refresh();
                }
            });
        }
    });
}

fn variable_rmw(grow_string: bool) {
    let store = variable_store::<false>();
    const THREADS: usize = 4;
    const ROUNDS: usize = 20;
    const KEYS: usize = 40;
    std::thread::scope(|scope| {
        for worker in 0..THREADS {
            let store = &store;
            scope.spawn(move || {
                let mut session = store.start_session();
                for round in 0..ROUNDS {
                    for id in 0..KEYS {
                        let mut op = VarRequest::new(id as u64, 8 + id % 19, 0);
                        op.increment = u64::from(!grow_string);
                        op.suffix = if grow_string {
                            vec![b'a' + worker as u8]
                        } else {
                            vec![88; 8]
                        };
                        let status = session.rmw(&mut op, (round * KEYS + id) as u64);
                        assert!(matches!(status, Status::Ok | Status::Pending));
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
    });
    let mut session = store.start_session();
    for id in 0..KEYS {
        let (status, bytes, pending) =
            VarRequest::new(id as u64, 8 + id % 19, 0).read(&mut session, id as u64);
        assert_eq!(status, Status::Ok);
        assert!(!pending);
        if grow_string {
            assert_eq!(bytes.len(), THREADS * ROUNDS);
            for worker in 0..THREADS {
                assert_eq!(
                    bytes.iter().filter(|&&b| b == b'a' + worker as u8).count(),
                    ROUNDS
                );
            }
        } else {
            assert_eq!(
                u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                (THREADS * ROUNDS) as u64
            );
            assert_eq!(bytes.len(), 8 + 8 * THREADS * ROUNDS);
        }
    }
}
#[test]
fn rmw_resize_value_concurrent() {
    variable_rmw(false);
}
#[test]
fn rmw_grow_string_concurrent() {
    variable_rmw(true);
}

#[test]
fn variable_record_larger_than_page_is_rejected() {
    let store = variable_store::<false>();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut session = store.start_session();
        let _ = session.upsert(&mut VarRequest::new(0, 8, 1 << 16), 0);
    }));
    assert!(result.is_err());
}
