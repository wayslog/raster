//! Checkpoint and recovery benchmark paired with tools/performance/checkpoint.cc.
//!   checkpoint run THREADS KEYS OPS DIR
//!   checkpoint recover THREADS KEYS TOKEN DIR
//! Values start at key and every RMW adds 1, so after recovery the sum of all
//! values must equal sum(keys) + sum(persisted serial numbers).
use raster::core::device::FileDevice;
use raster::core::guid::Guid;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Clone)]
struct Request {
    key: u64,
    input: u64,
    output: u64,
    sum: Rc<Cell<u64>>,
}
impl KeyContext<u64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Request {
    fn get(&mut self, value: &AtomicU64) {
        self.output = value.load(Ordering::Relaxed);
    }
    fn get_atomic(&mut self, value: &AtomicU64) {
        self.output = value.load(Ordering::SeqCst);
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.sum.set(self.sum.get() + self.output);
    }
}
impl UpsertContext<u64, AtomicU64> for Request {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::SeqCst);
        true
    }
}
impl RmwContext<u64, AtomicU64> for Request {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(old.load(Ordering::Relaxed) + self.input));
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        value.fetch_add(self.input, Ordering::SeqCst);
        true
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
    }
}

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        5,
        "usage: checkpoint run|recover THREADS KEYS OPS|TOKEN DIR"
    );
    let threads: usize = args[1].parse().unwrap();
    let keys: u64 = args[2].parse().unwrap();
    let dir = std::path::PathBuf::from(&args[4]);
    let device = Arc::new(FileDevice::open_with(dir.join("log"), 30, 8, false).unwrap());
    let store = FasterKv::<u64, AtomicU64>::with_device(
        StoreConfig {
            index_buckets: 1 << 22,
            log_size_budget: 0,
            log: LogConfig {
                page_bits: 25,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
            checkpoint_dir: Some(dir.join("checkpoints")),
        },
        device,
    );
    let ignored = Rc::new(Cell::new(0));
    if args[0] == "run" {
        let ops: usize = args[3].parse().unwrap();
        {
            let mut session = store.start_session();
            for key in 0..keys {
                let mut request = Request {
                    key,
                    input: key,
                    output: 0,
                    sum: ignored.clone(),
                };
                assert_eq!(session.upsert(&mut request, key), Status::Ok);
                if key & 255 == 255 {
                    session.refresh();
                }
            }
            session.complete_pending(true);
        }
        let done = AtomicUsize::new(0);
        let enrolled = AtomicUsize::new(0);
        let log_callbacks = Arc::new(AtomicUsize::new(0));
        let persisted = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let (window, during, token) = std::thread::scope(|scope| {
            for t in 0..threads {
                let (store, done, enrolled, log_callbacks) =
                    (&store, &done, &enrolled, &log_callbacks);
                scope.spawn(move || {
                    let ignored = Rc::new(Cell::new(0));
                    let mut session = store.start_session();
                    let mut state = (t as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    enrolled.fetch_add(1, Ordering::SeqCst);
                    while enrolled.load(Ordering::SeqCst) != threads {
                        std::thread::yield_now();
                    }
                    for i in 1..=(ops / threads) as u64 {
                        let mut request = Request {
                            key: next(&mut state) % keys,
                            input: 1,
                            output: 0,
                            sum: ignored.clone(),
                        };
                        let status = session.rmw(&mut request, i);
                        assert!(matches!(status, Status::Ok | Status::Pending));
                        if i & 255 == 0 {
                            session.complete_pending(false);
                            done.fetch_add(256, Ordering::Relaxed);
                        }
                    }
                    // Keep driving the checkpoint until every session has persisted.
                    while log_callbacks.load(Ordering::SeqCst) < threads {
                        session.complete_pending(false);
                    }
                    session.complete_pending(true);
                });
            }
            while done.load(Ordering::Relaxed) < ops / 4 {
                std::thread::yield_now();
            }
            let checkpoint_started = Instant::now();
            let ops_at_start = done.load(Ordering::Relaxed);
            let (calls, sum) = (log_callbacks.clone(), persisted.clone());
            let token = store
                .checkpoint(
                    Some(Box::new(|status| assert_eq!(status, Status::Ok))),
                    Some(Arc::new(move |status, serial| {
                        assert_eq!(status, Status::Ok);
                        sum.fetch_add(serial, Ordering::SeqCst);
                        calls.fetch_add(1, Ordering::SeqCst);
                    })),
                )
                .expect("checkpoint starts");
            while log_callbacks.load(Ordering::SeqCst) < threads {
                std::thread::yield_now();
            }
            let window = checkpoint_started.elapsed().as_nanos();
            (window, done.load(Ordering::Relaxed) - ops_at_start, token)
        });
        println!(
            "{{\"engine\":\"rust\",\"mode\":\"run\",\"threads\":{threads},\"keys\":{keys},\"ops\":{ops},\"elapsed_ns\":{},\"checkpoint_ns\":{window},\"ops_during_checkpoint\":{during},\"persisted_serials\":{},\"token\":\"{token}\"}}",
            started.elapsed().as_nanos(),
            persisted.load(Ordering::SeqCst)
        );
    } else {
        let token = Guid::parse(&args[3]).expect("token");
        let started = Instant::now();
        let info = store.recover(token, token).expect("recovery succeeds");
        let recovered = started.elapsed().as_nanos();
        let serials: u64 = info
            .sessions
            .iter()
            .map(|guid| store.continue_session(*guid).expect("recovered session").1)
            .sum();
        let sum = Rc::new(Cell::new(0u64));
        {
            let mut session = store.start_session();
            for key in 0..keys {
                let mut request = Request {
                    key,
                    input: 0,
                    output: 0,
                    sum: sum.clone(),
                };
                match session.read(&mut request, key) {
                    Status::Ok => sum.set(sum.get() + request.output),
                    status => assert_eq!(status, Status::Pending),
                }
                if key & 255 == 255 {
                    session.complete_pending(false);
                }
            }
            session.complete_pending(true);
        }
        let expected = keys * (keys - 1) / 2 + serials;
        println!(
            "{{\"engine\":\"rust\",\"mode\":\"recover\",\"recover_ns\":{recovered},\"sessions\":{},\"persisted_serials\":{serials},\"sum\":{},\"expected\":{expected}}}",
            info.sessions.len(),
            sum.get()
        );
        assert_eq!(sum.get(), expected);
    }
}
