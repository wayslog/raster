//! Larger-than-memory benchmark paired with tools/performance/disk.cc.
//! Usage: disk OP THREADS KEYS COUNT SAMPLE_STRIDE DIR [READ_CACHE_MB]
mod bench_support;
use bench_support::{Sample, Samples};
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::{Cell, RefCell};
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
    sample: Option<Sample>,
}
impl Request {
    fn add_output(&self) {
        self.sum.set(self.sum.get().wrapping_add(self.output));
    }
}
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<u64> for Request {
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
        self.add_output();
        Sample::finish(&mut self.sample, true);
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl UpsertContext<u64, AtomicU64> for Request {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::SeqCst);
        true
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl RmwContext<u64, AtomicU64> for Request {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        self.output = self.input;
        slot.write(AtomicU64::new(self.input));
    }
    fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
        self.output = old.load(Ordering::Relaxed) + self.input;
        slot.write(AtomicU64::new(self.output));
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        self.output = value.fetch_add(self.input, Ordering::SeqCst) + self.input;
        true
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.add_output();
        Sample::finish(&mut self.sample, true);
    }
}

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// hotread sends 9 of 10 reads to the first keys / 32, which were loaded
/// first and so live on the device.
fn pick(state: &mut u64, keys: u64, hot: bool) -> u64 {
    let r = next(state);
    if hot && !r.is_multiple_of(10) {
        next(state) % (keys / 32)
    } else {
        r % keys
    }
}

#[derive(Default)]
#[repr(align(64))]
struct WorkerResult {
    elapsed: u64,
    sum: u64,
    pending: u64,
    samples: Vec<u64>,
    pending_samples: usize,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert!(
        matches!(args.len(), 6 | 7),
        "usage: disk OP THREADS KEYS COUNT SAMPLE_STRIDE DIR [READ_CACHE_MB]"
    );
    let operation = args[0].as_str();
    let threads: usize = args[1].parse().unwrap();
    let keys: u64 = args[2].parse().unwrap();
    let count: usize = args[3].parse().unwrap();
    let stride: usize = args[4].parse().unwrap();
    assert!(matches!(operation, "read" | "hotread" | "rmw"));
    let hot = operation == "hotread";
    let read_cache_mb: u64 = args.get(6).map_or(0, |mb| mb.parse().unwrap());
    assert!(count.is_multiple_of(threads) && stride.is_power_of_two());
    let io_threads = bench_support::io_threads();
    let device = Arc::new(FileDevice::open_with(&args[5], 30, io_threads, false).unwrap());
    let config = StoreConfig {
        index_buckets: 1 << 22,
        filepath: None,
        pre_allocate: true,
        read_cache: None,
        auto_compaction: None,
        log_size_budget: 0,
        checkpoint_dir: None,
        log: LogConfig {
            page_bits: 25,
            memory_pages: 8,
            mutable_fraction: 0.5,
        },
    };
    let store = if read_cache_mb > 0 {
        let log = LogConfig {
            page_bits: 25,
            memory_pages: read_cache_mb >> 5,
            mutable_fraction: 0.9,
        };
        FasterKv::<u64, AtomicU64>::with_read_cache(
            config,
            device,
            ReadCacheConfig {
                log,
                pre_allocate: true,
            },
        )
    } else {
        FasterKv::<u64, AtomicU64>::with_device(config, device)
    };
    let ignored = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for key in 0..keys {
            let mut context = Request {
                key,
                input: key,
                output: 0,
                sum: ignored.clone(),
                sample: None,
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
                        ..Default::default()
                    };
                    let samples = Rc::new(RefCell::new(Samples {
                        values: Vec::with_capacity(count / threads / stride + 1),
                        pending: 0,
                    }));
                    let mut expected_samples = 0;
                    let mut state = (worker as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    enrolled.fetch_add(1, Ordering::SeqCst);
                    while enrolled.load(Ordering::SeqCst) != threads {
                        std::thread::yield_now();
                    }
                    let started = Instant::now();
                    for i in 0..count / threads {
                        let mut context = Request {
                            key: pick(&mut state, keys, hot),
                            input: 1,
                            output: 0,
                            sum: sum.clone(),
                            sample: None,
                        };
                        let sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
                        if sampled {
                            expected_samples += 1;
                            context.sample = Some(Sample::start(&samples));
                        }
                        let status = if operation != "rmw" {
                            session.read(&mut context, i as u64)
                        } else {
                            session.rmw(&mut context, i as u64)
                        };
                        match status {
                            Status::Ok => {
                                context.add_output();
                                Sample::finish(&mut context.sample, false);
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
                    result.pending_samples = samples.borrow().pending;
                    result.samples = std::mem::take(&mut samples.borrow_mut().values);
                    assert_eq!(
                        result.samples.len(),
                        expected_samples,
                        "every sampled request must finish"
                    );
                    result
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let (mut elapsed, mut sum, mut pending, mut samples) = (0u64, 0u64, 0u64, Vec::new());
    let mut pending_samples = 0;
    for result in results {
        elapsed = elapsed.max(result.elapsed);
        sum = sum.wrapping_add(result.sum);
        pending += result.pending;
        pending_samples += result.pending_samples;
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
            sample: None,
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
        "{{\"engine\":\"rust\",\"operation\":\"{operation}\",\"threads\":{threads},\"keys\":{keys},\"count\":{count},\"elapsed_ns\":{elapsed},\"latency_scope\":\"end_to_end\",\"p99_ns\":{},\"samples\":{},\"pending_samples\":{pending_samples},\"io_threads\":{io_threads},\"pending\":{pending},\"checksum\":{sum},\"digest\":{}}}",
        samples.get(samples.len() * 99 / 100).copied().unwrap_or(0),
        samples.len(),
        digest.get()
    );
}
