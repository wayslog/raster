//! Larger-than-memory behavior, after FASTER `cc/test/paging_test.h`.
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

type Store = FasterKv<u64, AtomicU64>;
const KEYS: u64 = 50_000;

/// Checks the value it observes and counts completions.
#[derive(Clone)]
struct Op {
    key: u64,
    input: u64,
    expect: Option<u64>,
    done: Rc<Cell<u64>>,
}
impl Op {
    fn new(key: u64, input: u64, expect: Option<u64>, done: &Rc<Cell<u64>>) -> Self {
        Self {
            key,
            input,
            expect,
            done: done.clone(),
        }
    }
    fn check(&self, status: Status, value: Option<u64>) {
        match self.expect {
            Some(want) => assert_eq!(
                (status, value),
                (Status::Ok, Some(want)),
                "key {}",
                self.key
            ),
            None => assert_eq!(status, Status::NotFound, "key {}", self.key),
        }
        self.done.set(self.done.get() + 1);
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
        self.check(status, (status == Status::Ok).then_some(self.input));
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
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok, "rmw key {}", self.key);
        self.done.set(self.done.get() + 1);
    }
}

fn store(name: &str) -> (Store, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("raster-paging-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let device = Arc::new(FileDevice::open(&dir, 20, 4).unwrap());
    let config = StoreConfig {
        index_buckets: 1 << 14,
        log: LogConfig {
            page_bits: 12,
            memory_pages: 8,
            mutable_fraction: 0.5,
        },
    };
    (Store::with_device(config, device), dir)
}

/// Runs `op`; returns 1 if it went pending, otherwise checks it immediately.
fn read(session: &mut Session<'_, u64, AtomicU64>, mut op: Op) -> u64 {
    let serial = op.key;
    match session.read(&mut op, serial) {
        Status::Pending => 1,
        status => {
            ReadContext::on_complete(&mut op, status);
            0
        }
    }
}

#[test]
fn read_back_values_spilled_to_disk() {
    let (store, dir) = store("read");
    let done = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for key in 0..KEYS {
            assert_eq!(
                session.upsert(&mut Op::new(key, key * 7, None, &done), key),
                Status::Ok
            );
        }
        let mut pending = 0;
        for key in 0..KEYS + 100 {
            let expect = (key < KEYS).then_some(key * 7);
            pending += read(&mut session, Op::new(key, 0, expect, &done));
            if key % 1000 == 0 {
                session.complete_pending(false);
            }
        }
        assert!(session.complete_pending(true));
        assert!(
            pending > KEYS / 2,
            "most reads should hit the device, got {pending}"
        );
    }
    assert_eq!(done.get(), KEYS + 100);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn rmw_copies_values_from_disk() {
    let (store, dir) = store("rmw");
    let done = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for round in 0..3 {
            for key in 0..KEYS {
                let status =
                    session.rmw(&mut Op::new(key, key + 1, None, &done), round * KEYS + key);
                assert!(matches!(status, Status::Ok | Status::Pending), "{status:?}");
                if key % 512 == 0 {
                    session.complete_pending(false);
                }
            }
            assert!(session.complete_pending(true));
        }
        for key in 0..KEYS {
            read(&mut session, Op::new(key, 0, Some(3 * (key + 1)), &done));
        }
        assert!(session.complete_pending(true));
    }
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn concurrent_rmw_over_disk() {
    let (store, dir) = store("concurrent");
    const THREADS: u64 = 4;
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let store = &store;
            scope.spawn(move || {
                let done = Rc::new(Cell::new(0));
                let mut session = store.start_session();
                for key in 0..KEYS / 2 {
                    let _ = session.rmw(&mut Op::new(key, 1, None, &done), key);
                    if key % 256 == 0 {
                        session.complete_pending(false);
                    }
                }
                assert!(session.complete_pending(true));
                let _ = t;
            });
        }
    });
    let done = Rc::new(Cell::new(0));
    let mut session = store.start_session();
    for key in 0..KEYS / 2 {
        read(&mut session, Op::new(key, 0, Some(THREADS), &done));
    }
    assert!(session.complete_pending(true));
    assert_eq!(done.get(), KEYS / 2);
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
