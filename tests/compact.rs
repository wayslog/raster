//! Log compaction, after FASTER's CompactWithLookup tests.
use raster::core::address::Address;
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
impl ReadContext<u64, AtomicU64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn get(&mut self, value: &AtomicU64) {
        self.input = value.load(Ordering::Relaxed);
    }
    fn on_complete(&mut self, status: Status) {
        self.out.set(Some((status, self.input)));
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

fn store(dir: &std::path::Path) -> FasterKv<u64, AtomicU64> {
    let _ = std::fs::remove_dir_all(dir);
    let device = Arc::new(FileDevice::open(dir, 16, 2).unwrap());
    FasterKv::with_device(
        StoreConfig {
            index_buckets: 1 << 12,
            checkpoint_dir: None,
            log: LogConfig {
                page_bits: 12,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
        },
        device,
    )
}

fn read(session: &mut Session<'_, u64, AtomicU64>, key: u64) -> (Status, u64) {
    let mut op = Op::new(key, u64::MAX);
    let out = op.out.clone();
    match session.read(&mut op, key) {
        Status::Pending => assert!(session.complete_pending(true)),
        status => ReadContext::on_complete(&mut op, status),
    }
    out.get().expect("read finished")
}

#[test]
fn compaction_keeps_live_records_and_drops_the_rest() {
    let dir = std::env::temp_dir().join(format!("raster-compact-{}", std::process::id()));
    let store = store(&dir);
    const KEYS: u64 = 20_000;
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(session.upsert(&mut Op::new(key, 1), key), Status::Ok);
    }
    for key in (0..KEYS).step_by(2) {
        assert_eq!(session.upsert(&mut Op::new(key, 2), key), Status::Ok);
    }
    for key in (0..KEYS).step_by(3) {
        assert_eq!(session.delete(&Op::new(key, 0), key), Status::Ok);
    }
    session.refresh();
    let begin_before = store.hlog.begin.load();
    let until = session
        .compact(Address::MAX, true, 3)
        .expect("compaction succeeds");
    assert!(until > begin_before);
    assert_eq!(store.hlog.begin.load(), until);
    for key in 0..KEYS {
        let expected = if key % 3 == 0 {
            (Status::NotFound, u64::MAX)
        } else if key % 2 == 0 {
            (Status::Ok, 2)
        } else {
            (Status::Ok, 1)
        };
        assert_eq!(read(&mut session, key), expected, "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn compaction_under_concurrent_rmw_loses_no_update() {
    let dir = std::env::temp_dir().join(format!("raster-compact-busy-{}", std::process::id()));
    let store = store(&dir);
    const THREADS: u64 = 4;
    const KEYS: u64 = 3_000;
    const ROUNDS: u64 = 30;
    let compactions = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let store = &store;
            scope.spawn(move || {
                // Another action may already be running; wait for REST.
                let mut session = loop {
                    if let Some(session) = store.try_start_session() {
                        break session;
                    }
                    std::thread::yield_now();
                };
                for round in 0..ROUNDS {
                    for key in 0..KEYS {
                        let serial = (round * KEYS + key) * THREADS + t;
                        match session.rmw(&mut Op::new(key, 1), serial) {
                            Status::Ok | Status::Pending => {}
                            status => panic!("rmw {status:?}"),
                        }
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
        let mut session = store.start_session();
        while compactions.load(Ordering::SeqCst) < 3 {
            std::thread::sleep(std::time::Duration::from_millis(5));
            session
                .compact(Address::MAX, true, 2)
                .expect("compaction succeeds");
            compactions.fetch_add(1, Ordering::SeqCst);
        }
    });
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(
            read(&mut session, key),
            (Status::Ok, THREADS * ROUNDS),
            "key {key}"
        );
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn auto_compaction_keeps_the_log_near_its_budget() {
    use raster::core::auto_compact::AutoCompactionConfig;
    let dir = std::env::temp_dir().join(format!("raster-autocompact-{}", std::process::id()));
    let store = Arc::new(store(&dir));
    const KEYS: u64 = 2_000;
    const BUDGET: u64 = 256 << 10;
    let config = AutoCompactionConfig {
        check_interval: std::time::Duration::from_millis(5),
        threads: 2,
        ..AutoCompactionConfig::with_budget(BUDGET)
    };
    let begin = store.hlog.begin.load();
    let compactor = store.spawn_auto_compaction(config);
    std::thread::scope(|scope| {
        for t in 0..2u64 {
            let store = &store;
            scope.spawn(move || {
                // Another action may already be running; wait for REST.
                let mut session = loop {
                    if let Some(session) = store.try_start_session() {
                        break session;
                    }
                    std::thread::yield_now();
                };
                for round in 0..200u64 {
                    for key in 0..KEYS {
                        let _ =
                            session.upsert(&mut Op::new(key, round), (round * KEYS + key) * 2 + t);
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
    });
    drop(compactor);
    assert!(
        store.hlog.begin.load() > begin,
        "auto compaction truncated the log"
    );
    assert!(
        store.log_size() < 4 * BUDGET,
        "log size {}",
        store.log_size()
    );
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(read(&mut session, key), (Status::Ok, 199), "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
