//! Read cache, after FASTER's read-cache tests.
use raster::core::address::Address;
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::state::Phase;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct Op {
    key: u64,
    input: u64,
    out: Rc<Cell<Option<(Status, u64)>>>,
}
impl Op {
    fn new(key: u64, input: u64) -> Self {
        Self {
            key,
            input,
            out: Rc::default(),
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
        self.input = value.load(Ordering::Relaxed);
    }
    fn on_complete(&mut self, status: Status) {
        self.out.set(Some((status, self.input)));
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

type Store = FasterKv<u64, AtomicU64>;

fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("raster-rc-{name}-{}", std::process::id()))
}

/// A log of 16 4 KiB frames and a read cache of 4 such frames, so both
/// evict constantly.
fn store(dir: &std::path::Path, buckets: u64) -> Store {
    let device = Arc::new(FileDevice::open(dir, 16, 2).unwrap());
    FasterKv::with_read_cache(
        StoreConfig {
            index_buckets: buckets,
            checkpoint_dir: Some(dir.join("checkpoints")),
            log: LogConfig {
                page_bits: 12,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
        },
        device,
        ReadCacheConfig {
            log: LogConfig {
                page_bits: 12,
                memory_pages: 4,
                mutable_fraction: 0.5,
            },
        },
    )
}

fn fresh_store(name: &str, buckets: u64) -> (std::path::PathBuf, Store) {
    let dir = temp_dir(name);
    let _ = std::fs::remove_dir_all(&dir);
    let store = store(&dir, buckets);
    (dir, store)
}

/// Reads `key`; the flag says whether it completed without going pending.
fn read(session: &mut Session<'_, u64, AtomicU64>, key: u64) -> (Status, u64, bool) {
    let mut op = Op::new(key, u64::MAX);
    let out = op.out.clone();
    let immediate = match session.read(&mut op, key) {
        Status::Pending => {
            assert!(session.complete_pending(true));
            false
        }
        status => {
            ReadContext::on_complete(&mut op, status);
            true
        }
    };
    let (status, value) = out.get().expect("read finished");
    (status, value, immediate)
}

fn value(session: &mut Session<'_, u64, AtomicU64>, key: u64) -> (Status, u64) {
    let (status, value, _) = read(session, key);
    (status, value)
}

fn load(session: &mut Session<'_, u64, AtomicU64>, keys: u64) {
    for key in 0..keys {
        assert_eq!(session.upsert(&mut Op::new(key, key), key), Status::Ok);
    }
}

#[test]
fn cached_reads_complete_without_io_and_writes_invalidate() {
    let (dir, store) = fresh_store("basic", 1 << 12);
    const KEYS: u64 = 4_000;
    let mut session = store.start_session();
    load(&mut session, KEYS);
    // Key 0 is on the device: the first read goes pending, the second hits
    // the cache.
    assert_eq!(read(&mut session, 0), (Status::Ok, 0, false));
    assert_eq!(read(&mut session, 0), (Status::Ok, 0, true));
    // Each kind of write replaces the cached copy.
    assert_eq!(session.upsert(&mut Op::new(0, 7), 0), Status::Ok);
    assert_eq!(read(&mut session, 0), (Status::Ok, 7, true));
    assert_eq!(read(&mut session, 1), (Status::Ok, 1, false));
    match session.rmw(&mut Op::new(1, 10), 0) {
        Status::Ok => {}
        Status::Pending => assert!(session.complete_pending(true)),
        status => panic!("rmw {status:?}"),
    }
    assert_eq!(value(&mut session, 1), (Status::Ok, 11));
    assert_eq!(read(&mut session, 2), (Status::Ok, 2, false));
    assert_eq!(session.delete(&Op::new(2, 0), 0), Status::Ok);
    assert_eq!(read(&mut session, 2).0, Status::NotFound);
    // Reading everything cycles the small cache many times over.
    for round in 0..2 {
        for key in 3..KEYS {
            assert_eq!(value(&mut session, key), (Status::Ok, key), "round {round}");
        }
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

fn rmw(session: &mut Session<'_, u64, AtomicU64>, key: u64, input: u64, serial: u64) {
    match session.rmw(&mut Op::new(key, input), serial) {
        Status::Ok | Status::Pending => {}
        status => panic!("rmw {status:?}"),
    }
}

/// Starts a session once no state-machine action is running.
fn session(store: &Store) -> Session<'_, u64, AtomicU64> {
    loop {
        if let Some(session) = store.try_start_session() {
            return session;
        }
        std::thread::yield_now();
    }
}

#[test]
fn concurrent_reads_never_see_stale_cached_values() {
    let (dir, store) = fresh_store("busy", 1 << 10);
    const WRITERS: u64 = 2;
    const READERS: u64 = 2;
    const KEYS: u64 = 3_000;
    const ROUNDS: u64 = 20;
    std::thread::scope(|scope| {
        for t in 0..WRITERS {
            let store = &store;
            scope.spawn(move || {
                let mut session = session(store);
                for round in 0..ROUNDS {
                    for key in 0..KEYS {
                        rmw(&mut session, key, 1, (round * KEYS + key) * WRITERS + t);
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
        for t in 0..READERS {
            let store = &store;
            scope.spawn(move || {
                let mut session = session(store);
                // Each key's count only grows, so a reader seeing a smaller
                // value than before read a stale cached copy.
                let mut seen = vec![0u64; KEYS as usize];
                for round in 0..ROUNDS * 2 {
                    for i in 0..KEYS {
                        let key = (i * 7 + t + round) % KEYS;
                        let (status, value) = value(&mut session, key);
                        if status == Status::Ok {
                            assert!(value >= seen[key as usize], "key {key} went back");
                            seen[key as usize] = value;
                        } else {
                            assert_eq!(status, Status::NotFound);
                        }
                    }
                }
            });
        }
    });
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(
            value(&mut session, key),
            (Status::Ok, WRITERS * ROUNDS),
            "key {key}"
        );
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// Loads keys, overwrites them, and reads them all so many index entries
/// point into the read cache.
fn populate(session: &mut Session<'_, u64, AtomicU64>, keys: u64) {
    load(session, keys);
    for key in 0..keys {
        assert_eq!(
            session.upsert(&mut Op::new(key, key + 1), keys + key),
            Status::Ok
        );
    }
    for key in (0..keys).rev() {
        assert_eq!(value(session, key), (Status::Ok, key + 1));
    }
    assert_eq!(read(session, 0), (Status::Ok, 1, true), "key 0 is cached");
}

#[test]
fn growth_drops_cached_entries_and_keeps_keys() {
    let (dir, store) = fresh_store("grow", 16);
    const KEYS: u64 = 4_000;
    let mut session = store.start_session();
    populate(&mut session, KEYS);
    assert!(store.grow_index(None));
    assert!(session.complete_pending(true));
    for key in 0..KEYS {
        assert_eq!(value(&mut session, key), (Status::Ok, key + 1), "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn compaction_and_truncation_with_cached_entries() {
    let (dir, store) = fresh_store("compact", 1 << 10);
    const KEYS: u64 = 4_000;
    let mut session = store.start_session();
    populate(&mut session, KEYS);
    session
        .compact(Address::MAX, true, 2)
        .expect("compaction succeeds");
    for key in 0..KEYS {
        assert_eq!(value(&mut session, key), (Status::Ok, key + 1), "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn checkpoint_writes_log_addresses_for_cached_entries() {
    let (dir, store) = fresh_store("ckpt", 1 << 10);
    const KEYS: u64 = 4_000;
    let token = {
        let mut session = store.start_session();
        populate(&mut session, KEYS);
        let token = store.checkpoint(None, None).expect("checkpoint starts");
        while store.system_state().phase != Phase::Rest {
            session.refresh();
        }
        token
    };
    drop(store);
    let store = self::store(&dir, 1 << 10);
    store.recover(token, token).expect("recovery succeeds");
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(value(&mut session, key), (Status::Ok, key + 1), "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
