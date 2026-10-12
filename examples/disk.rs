//! Larger-than-memory benchmark paired with tools/performance/disk.cc.
//! Usage: disk OP THREADS KEYS COUNT SAMPLE_STRIDE DIR
use raster::core::device::FileDevice;
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
    /// Per-thread accumulator for pending completions.
    sum: Rc<Cell<u64>>,
}
impl Request {
    fn add_output(&self) {
        self.sum.set(self.sum.get().wrapping_add(self.output));
    }
}
impl ReadContext<u64, AtomicU64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn get(&mut self, value: &AtomicU64) {
        self.output = value.load(Ordering::Relaxed);
    }
    fn get_atomic(&mut self, value: &AtomicU64) {
        self.output = value.load(Ordering::SeqCst);
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.add_output();
    }
}
impl UpsertContext<u64, AtomicU64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn put(&mut self) -> AtomicU64 {
        AtomicU64::new(self.input)
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::SeqCst);
        true
    }
}
impl RmwContext<u64, AtomicU64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn rmw_initial(&mut self) -> AtomicU64 {
        self.output = self.input;
        AtomicU64::new(self.input)
    }
    fn rmw_copy(&mut self, old: &AtomicU64) -> AtomicU64 {
        self.output = old.load(Ordering::Relaxed) + self.input;
        AtomicU64::new(self.output)
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        self.output = value.fetch_add(self.input, Ordering::SeqCst) + self.input;
        true
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.add_output();
    }
}

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[derive(Default)]
#[repr(align(64))]
struct WorkerResult {
    elapsed: u64,
    sum: u64,
    pending: u64,
    samples: Vec<u64>,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        6,
        "usage: disk OP THREADS KEYS COUNT SAMPLE_STRIDE DIR"
    );
    let operation = args[0].as_str();
    let threads: usize = args[1].parse().unwrap();
    let keys: u64 = args[2].parse().unwrap();
    let count: usize = args[3].parse().unwrap();
    let stride: usize = args[4].parse().unwrap();
    assert!(matches!(operation, "read" | "rmw"));
    assert!(count.is_multiple_of(threads) && stride.is_power_of_two());
    let device = Arc::new(FileDevice::open_with(&args[5], 30, 8, false).unwrap());
    let store = FasterKv::<u64, AtomicU64>::with_device(
        StoreConfig {
            index_buckets: 1 << 22,
            log: LogConfig {
                page_bits: 25,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        device,
    );
    let ignored = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for key in 0..keys {
            let mut context = Request {
                key,
                input: key,
                output: 0,
                sum: ignored.clone(),
            };
            assert_eq!(session.upsert(&mut context, key), Status::Ok);
            if key & 255 == 255 {
                session.refresh();
            }
        }
        session.complete_pending(true);
    }

    let enrolled = AtomicUsize::new(0);
    let results: Vec<WorkerResult> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|worker| {
                let (store, enrolled) = (&store, &enrolled);
                scope.spawn(move || {
                    let mut session = store.start_session();
                    let sum = Rc::new(Cell::new(0u64));
                    let mut result = WorkerResult {
                        samples: Vec::with_capacity(count / threads / stride + 1),
                        ..Default::default()
                    };
                    let mut state = (worker as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    enrolled.fetch_add(1, Ordering::SeqCst);
                    while enrolled.load(Ordering::SeqCst) != threads {
                        std::thread::yield_now();
                    }
                    let started = Instant::now();
                    for i in 0..count / threads {
                        let mut context = Request {
                            key: next(&mut state) % keys,
                            input: 1,
                            output: 0,
                            sum: sum.clone(),
                        };
                        let sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
                        let clock = sampled.then(Instant::now);
                        let status = if operation == "read" {
                            session.read(&mut context, i as u64)
                        } else {
                            session.rmw(&mut context, i as u64)
                        };
                        match status {
                            Status::Ok => {
                                context.add_output();
                                if let Some(clock) = clock {
                                    result.samples.push(clock.elapsed().as_nanos() as u64);
                                }
                            }
                            Status::Pending => result.pending += 1,
                            other => panic!("unexpected status {other:?}"),
                        }
                        if i & 255 == 255 {
                            session.complete_pending(false);
                        }
                    }
                    session.complete_pending(true);
                    result.elapsed = started.elapsed().as_nanos() as u64;
                    result.sum = sum.get();
                    result
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let (mut elapsed, mut sum, mut pending, mut samples) = (0u64, 0u64, 0u64, Vec::new());
    for result in results {
        elapsed = elapsed.max(result.elapsed);
        sum = sum.wrapping_add(result.sum);
        pending += result.pending;
        samples.extend(result.samples);
    }
    samples.sort_unstable();
    // Digest: total of all final values, read back through the store.
    let digest = Rc::new(Cell::new(0u64));
    let mut session = store.start_session();
    for key in 0..keys {
        let mut context = Request {
            key,
            input: 0,
            output: 0,
            sum: digest.clone(),
        };
        match session.read(&mut context, key) {
            Status::Ok => context.add_output(),
            status => assert_eq!(status, Status::Pending),
        }
        if key & 255 == 255 {
            session.complete_pending(false);
        }
    }
    session.complete_pending(true);
    println!(
        "{{\"engine\":\"rust\",\"operation\":\"{operation}\",\"threads\":{threads},\"keys\":{keys},\"count\":{count},\"elapsed_ns\":{elapsed},\"p99_ns\":{},\"samples\":{},\"pending\":{pending},\"checksum\":{sum},\"digest\":{}}}",
        samples.get(samples.len() * 99 / 100).copied().unwrap_or(0),
        samples.len(),
        digest.get()
    );
}
