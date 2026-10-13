//! Hash index growth, after FASTER's GrowIndex tests.
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
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

fn store(dir: &std::path::Path, buckets: u64) -> FasterKv<u64, AtomicU64> {
    let _ = std::fs::remove_dir_all(dir);
    let device = Arc::new(FileDevice::open(dir, 20, 2).unwrap());
    FasterKv::with_device(
        StoreConfig {
            index_buckets: buckets,
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

fn rmw(session: &mut Session<'_, u64, AtomicU64>, key: u64, input: u64, serial: u64) {
    match session.rmw(&mut Op::new(key, input), serial) {
        Status::Ok | Status::Pending => {}
        status => panic!("rmw {status:?}"),
    }
}

#[test]
fn growth_keeps_every_key_reachable() {
    let dir = std::env::temp_dir().join(format!("raster-grow-{}", std::process::id()));
    let store = store(&dir, 16);
    const KEYS: u64 = 20_000;
    let mut session = store.start_session();
    for key in 0..KEYS {
        rmw(&mut session, key, key + 1, key);
    }
    assert!(session.complete_pending(true));
    let reported = Arc::new(AtomicU64::new(0));
    for round in 1..=3u64 {
        let r = reported.clone();
        assert!(store.grow_index(Some(Box::new(move |buckets| {
            r.store(buckets, Ordering::SeqCst)
        }))));
        assert!(session.complete_pending(true));
        assert_eq!(reported.load(Ordering::SeqCst), 16 << round);
        for key in 0..KEYS {
            assert_eq!(
                read(&mut session, key),
                (Status::Ok, key + 1),
                "round {round} key {key}"
            );
        }
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn growth_under_concurrent_rmw_loses_no_update() {
    let dir = std::env::temp_dir().join(format!("raster-grow-busy-{}", std::process::id()));
    let store = store(&dir, 64);
    const THREADS: u64 = 4;
    const KEYS: u64 = 4_000;
    const ROUNDS: u64 = 25;
    let grown = AtomicU64::new(0);
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let store = &store;
            scope.spawn(move || {
                let mut session = store.start_session();
                for round in 0..ROUNDS {
                    for key in 0..KEYS {
                        rmw(&mut session, key, 1, (round * KEYS + key) * THREADS + t);
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
        while grown.load(Ordering::SeqCst) < 3 {
            std::thread::sleep(std::time::Duration::from_millis(3));
            let g = &grown;
            if store.grow_index(None) {
                let target = g.load(Ordering::SeqCst) + 1;
                while store.system_state().phase != state::Phase::Rest {
                    std::thread::yield_now();
                }
                g.store(target, Ordering::SeqCst);
            }
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
