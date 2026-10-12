//! Direct-engine resident benchmark paired line by line with tools/performance/memory.cc.
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

struct Request {
    key: u64,
    input: u64,
    output: u64,
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
}
impl UpsertContext<u64, AtomicU64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn put(&mut self) -> AtomicU64 {
        self.output = self.input;
        AtomicU64::new(self.input)
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::SeqCst);
        self.output = self.input;
        true
    }
}
impl RmwContext<u64, AtomicU64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
    fn rmw_initial(&mut self) -> AtomicU64 {
        self.output = 1;
        AtomicU64::new(1)
    }
    fn rmw_copy(&mut self, old: &AtomicU64) -> AtomicU64 {
        self.output = old.load(Ordering::Relaxed) + 1;
        AtomicU64::new(self.output)
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        self.output = value.fetch_add(1, Ordering::SeqCst) + 1;
        true
    }
}

fn key_for(distribution: &str, worker: usize, i: usize) -> u64 {
    match distribution {
        "uniform" => (worker * 256 + (i * 17) % 256) as u64,
        "worker-hot" => (worker * 256) as u64,
        _ => 0,
    }
}

#[derive(Default)]
#[repr(align(64))]
struct WorkerResult {
    elapsed: u64,
    checksum: u64,
    samples: Vec<u64>,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        5,
        "usage: parity OP DISTRIBUTION THREADS COUNT SAMPLE_STRIDE"
    );
    let (operation, distribution) = (args[0].as_str(), args[1].as_str());
    let threads: usize = args[2].parse().unwrap();
    let count: usize = args[3].parse().unwrap();
    let stride: usize = args[4].parse().unwrap();
    assert!(matches!(operation, "read" | "upsert" | "rmw"));
    assert!(matches!(
        distribution,
        "uniform" | "worker-hot" | "shared-hot"
    ));
    assert!(matches!(threads, 1 | 4) && count > 0 && count.is_multiple_of(threads));
    assert!(stride.is_power_of_two());

    let store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
        index_buckets: 1024,
        log: LogConfig {
            page_bits: 25,
            memory_pages: 8,
            mutable_fraction: 0.9,
        },
    });
    {
        let mut session = store.start_session();
        for key in 0..(threads * 256) as u64 {
            let mut context = Request {
                key,
                input: 7,
                output: 0,
            };
            assert_eq!(session.upsert(&mut context, key), Status::Ok);
        }
    }

    let enrolled = AtomicUsize::new(0);
    let results: Vec<WorkerResult> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|worker| {
                let (store, enrolled) = (&store, &enrolled);
                scope.spawn(move || {
                    let mut session = store.start_session();
                    let mut result = WorkerResult {
                        samples: Vec::with_capacity(count / threads / stride + 1),
                        ..Default::default()
                    };
                    let mut checksum = 0u64;
                    enrolled.fetch_add(1, Ordering::SeqCst);
                    while enrolled.load(Ordering::SeqCst) != threads {
                        std::thread::yield_now();
                    }
                    let started = Instant::now();
                    for i in 0..count / threads {
                        let mut context = Request {
                            key: key_for(distribution, worker, i),
                            input: 42,
                            output: 0,
                        };
                        let sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
                        let clock = sampled.then(Instant::now);
                        let status = match operation {
                            "upsert" => session.upsert(&mut context, i as u64),
                            "read" => session.read(&mut context, i as u64),
                            _ => session.rmw(&mut context, i as u64),
                        };
                        assert_eq!(status, Status::Ok);
                        if let Some(clock) = clock {
                            result.samples.push(clock.elapsed().as_nanos() as u64);
                        }
                        checksum = checksum.wrapping_add(context.output);
                        if i & 255 == 255 {
                            session.refresh();
                        }
                    }
                    result.elapsed = started.elapsed().as_nanos() as u64;
                    result.checksum = checksum;
                    result
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut expected = vec![7u64; threads * 256];
    let (mut elapsed, mut checksum, mut samples) = (0u64, 0u64, Vec::new());
    // Diagnostics only: per-thread elapsed time and sampled P99.
    let mut per_thread = Vec::new();
    for (worker, mut result) in results.into_iter().enumerate() {
        elapsed = elapsed.max(result.elapsed);
        result.samples.sort_unstable();
        per_thread.push(format!(
            "[{},{}]",
            result.elapsed,
            result.samples[result.samples.len() * 99 / 100]
        ));
        checksum = checksum.wrapping_add(result.checksum);
        samples.extend(result.samples);
        for i in 0..count / threads {
            let value = &mut expected[key_for(distribution, worker, i) as usize];
            match operation {
                "upsert" => *value = 42,
                "rmw" => *value += 1,
                _ => {}
            }
        }
    }
    samples.sort_unstable();
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    let mut session = store.start_session();
    for (key, &want) in expected.iter().enumerate() {
        let mut context = Request {
            key: key as u64,
            input: 0,
            output: 0,
        };
        assert_eq!(session.read(&mut context, key as u64), Status::Ok);
        assert_eq!(context.output, want, "final value of key {key}");
        digest = (digest ^ context.output).wrapping_mul(0x0100_0000_01b3);
    }
    println!(
        "{{\"engine\":\"rust\",\"operation\":\"{operation}\",\"distribution\":\"{distribution}\",\"threads\":{threads},\"count\":{count},\"elapsed_ns\":{elapsed},\"p99_ns\":{},\"samples\":{},\"checksum\":{checksum},\"digest\":{digest},\"verified_keys\":{},\"retries\":0,\"pending\":0,\"per_thread\":[{}]}}",
        samples[samples.len() * 99 / 100],
        samples.len(),
        expected.len(),
        per_thread.join(",")
    );
}
