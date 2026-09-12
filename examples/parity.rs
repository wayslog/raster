//! Direct-engine memory benchmark paired with tools/performance/memory.cc.
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    sync::{Barrier, atomic::Ordering},
    time::{Duration, Instant},
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Measurement {
    elapsed: u64,
    samples: Vec<u64>,
    checksum: u64,
    retries: u64,
    pending: u64,
}
#[derive(Clone, Copy)]
struct Request {
    key: u64,
    value: u64,
}
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<u64, Error> {
        Ok(*value.view())
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = u64;
    fn replacement(&mut self) -> std::result::Result<(u64, u64), Error> {
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> std::result::Result<UpdateDecision<u64>, Error> {
        value.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.value))
    }
}
impl RmwOperation<Schema> for Request {
    type Output = u64;
    fn initial(&mut self) -> std::result::Result<(u64, u64), Error> {
        Ok((1, 1))
    }
    fn copy_update(
        &mut self,
        value: ValueRead<'_, Schema>,
    ) -> std::result::Result<(u64, u64), Error> {
        let next = value.view().wrapping_add(1);
        Ok((next, next))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> std::result::Result<UpdateDecision<u64>, Error> {
        let next = value
            .view_mut()
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        Ok(UpdateDecision::Updated(next))
    }
}
fn ready(submission: Submission<u64>) -> Result<u64> {
    match submission {
        Submission::Ready(Ok(Outcome::Success(value))) => Ok(value),
        Submission::Ready(Err(error)) => Err(error.into()),
        _ => Err("memory benchmark requires synchronous success".into()),
    }
}
fn key_for(distribution: &str, worker: usize, index: usize) -> u64 {
    match distribution {
        "uniform" => (worker * 256 + (index * 17) % 256) as u64,
        "worker-hot" => (worker * 256) as u64,
        "shared-hot" => 0,
        _ => unreachable!(),
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(30))
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 {
        return Err("usage: parity OP DISTRIBUTION THREADS COUNT SAMPLE_STRIDE".into());
    }
    let operation = args[0].as_str();
    let distribution = args[1].as_str();
    let threads: usize = args[2].parse()?;
    let count: usize = args[3].parse()?;
    let stride: usize = args[4].parse()?;
    if !matches!(operation, "read" | "upsert" | "rmw")
        || !matches!(distribution, "uniform" | "worker-hot" | "shared-hot")
        || !matches!(threads, 1 | 4)
        || count == 0
        || !count.is_multiple_of(threads)
        || !stride.is_power_of_two()
    {
        return Err("invalid benchmark arguments".into());
    }
    let config = raster::config::Config {
        log: raster::config::LogConfig {
            memory_pages: 8,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()?;
    let mut setup = store.start_session(SessionOptions::default())?;
    for key in 0..(threads * 256) as u64 {
        ready(
            setup
                .upsert(Serial(key), Request { key, value: 7 })
                .map_err(|e| e.reason)?,
        )?;
    }
    setup.close(deadline())?;
    let barrier = Barrier::new(threads);
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..threads {
            let store = &store;
            let barrier = &barrier;
            handles.push(scope.spawn(move || -> Result<Measurement> {
                // All sessions must be enrolled before workers enter the start barrier.
                let mut session = store
                    .start_session(SessionOptions::default())
                    .expect("session enrollment");
                let mut samples = Vec::with_capacity(count / threads / stride + 1);
                let mut checksum = 0u64;
                let mut retries = 0u64;
                let mut pending = 0u64;
                barrier.wait();
                let started = Instant::now();
                let limit = Deadline(started + Duration::from_secs(120));
                for i in 0..count / threads {
                    let request = Request {
                        key: key_for(distribution, worker, i),
                        value: 42,
                    };
                    let sampled = i & (stride - 1) == (i / stride) & (stride - 1);
                    let clock = sampled.then(Instant::now);
                    let output = loop {
                        let submitted = match operation {
                            "upsert" => session.upsert(Serial(i as u64), request),
                            "read" => {
                                session.read(Serial(i as u64), request, ReadOptions::default())
                            }
                            "rmw" => session.rmw(Serial(i as u64), request, RmwOptions::default()),
                            _ => unreachable!(),
                        };
                        match submitted {
                            Ok(Submission::Pending(mut ticket)) => {
                                pending += 1;
                                break Submission::Ready(session.wait(&mut ticket, limit)?);
                            }
                            Ok(output) => break output,
                            Err(rejected) if matches!(rejected.reason, Error::Busy) => {
                                retries += 1;
                                if limit.expired() {
                                    return Err(Error::DeadlineExceeded.into());
                                }
                                std::thread::yield_now();
                            }
                            Err(rejected) => return Err(rejected.reason.into()),
                        }
                    };
                    let value = ready(output)?;
                    if let Some(clock) = clock {
                        samples.push(clock.elapsed().as_nanos() as u64);
                    }
                    checksum = checksum.wrapping_add(value);
                    if i & 255 == 255 {
                        session.refresh()?;
                    }
                }
                let elapsed = started.elapsed().as_nanos() as u64;
                session.close(deadline())?;
                Ok(Measurement {
                    elapsed,
                    samples,
                    checksum,
                    retries,
                    pending,
                })
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark worker"))
            .collect::<Result<Vec<_>>>()
    })?;
    let elapsed = results.iter().map(|r| r.elapsed).max().unwrap();
    let checksum = results
        .iter()
        .fold(0u64, |sum, r| sum.wrapping_add(r.checksum));
    let retries: u64 = results.iter().map(|r| r.retries).sum();
    let pending: u64 = results.iter().map(|r| r.pending).sum();
    let mut samples: Vec<_> = results.into_iter().flat_map(|r| r.samples).collect();
    samples.sort_unstable();
    let mut expected = vec![7u64; threads * 256];
    for worker in 0..threads {
        for i in 0..count / threads {
            let value = &mut expected[key_for(distribution, worker, i) as usize];
            if operation == "upsert" {
                *value = 42;
            }
            if operation == "rmw" {
                *value = value.wrapping_add(1);
            }
        }
    }
    let mut verify = store.start_session(SessionOptions::default())?;
    let mut digest = 0xcbf29ce484222325u64;
    for (key, expected) in expected.iter().enumerate() {
        let value = ready(
            verify
                .read(
                    Serial(key as u64),
                    Request {
                        key: key as u64,
                        value: 0,
                    },
                    ReadOptions::default(),
                )
                .map_err(|e| e.reason)?,
        )?;
        assert_eq!(value, *expected, "final value for key {key}");
        digest = (digest ^ value).wrapping_mul(0x100000001b3);
    }
    verify.close(deadline())?;
    store.shutdown(deadline())?;
    println!(
        "{{\"engine\":\"rust\",\"operation\":\"{operation}\",\"distribution\":\"{distribution}\",\"threads\":{threads},\"count\":{count},\"elapsed_ns\":{elapsed},\"p99_ns\":{},\"samples\":{},\"checksum\":{checksum},\"digest\":{digest},\"verified_keys\":{},\"retries\":{retries},\"pending\":{pending}}}",
        samples[samples.len() * 99 / 100],
        samples.len(),
        expected.len()
    );
    Ok(())
}
