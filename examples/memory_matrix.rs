//! 可重复的内存写入基线；分组比较，不作为性能承诺。
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    schema::{
        Schema, ValueUpdate,
        builtin::{AtomicU64Value, ByteValueCodec, SchemaPair, SerializedValue, U64Key},
    },
    types::*,
};
use std::{
    sync::{Barrier, atomic::Ordering},
    time::{Duration, Instant},
};
type ExampleResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Fixed = SchemaPair<U64Key, AtomicU64Value>;
type Variable = SchemaPair<U64Key, SerializedValue<ByteValueCodec>>;
struct Number {
    key: u64,
    value: u64,
}
impl Keyed<Fixed> for Number {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertOperation<Fixed> for Number {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Fixed>,
    ) -> Result<UpdateDecision<u64>, Error> {
        v.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.value))
    }
}
struct Bytes {
    key: u64,
    value: Vec<u8>,
    sequence: u64,
}
impl Keyed<Variable> for Bytes {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertOperation<Variable> for Bytes {
    type Output = u64;
    fn replacement(&mut self) -> Result<(Vec<u8>, u64), Error> {
        Ok((self.value.clone(), self.sequence))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Variable>,
    ) -> Result<UpdateDecision<u64>, Error> {
        match v.view_mut().replace(&self.value) {
            Ok(()) => Ok(UpdateDecision::Updated(self.sequence)),
            Err(Error::Codec(_)) => Ok(UpdateDecision::Append),
            Err(e) => Err(e),
        }
    }
}
fn run<S: Schema, O: UpsertOperation<S, Output = u64>>(
    schema: S,
    make: impl Fn(u64, u64) -> O + Sync,
    threads: usize,
    hot: bool,
) -> ExampleResult<(f64, u128, u128, u128, u64)> {
    let store = RasterKV::builder(schema)
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()?;
    let barrier = Barrier::new(threads + 1);
    let count = 20_000usize;
    let (elapsed, results) = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..threads {
            let store = &store;
            let barrier = &barrier;
            let make = &make;
            handles.push(scope.spawn(move || -> ExampleResult<(Vec<u128>, u64)> {
                let session = store.start_session(SessionOptions::default());
                let mut latency = Vec::with_capacity(count / threads);
                let mut retries = 0;
                barrier.wait();
                let mut session = session?;
                let end = Instant::now() + Duration::from_secs(60);
                for i in 0..count / threads {
                    let sequence = (worker * count + i) as u64;
                    let key = if hot {
                        1
                    } else {
                        ((i * 17 + worker * 73) % 256) as u64
                    };
                    let mut request = make(key, sequence);
                    let start = Instant::now();
                    loop {
                        match session.upsert(Serial(i as u64), request) {
                            Err(rejected) => {
                                if !matches!(rejected.reason, Error::Busy) {
                                    return Err(rejected.reason.into());
                                }
                                if Instant::now() >= end {
                                    return Err(Error::DeadlineExceeded.into());
                                }
                                retries += 1;
                                request = rejected.request;
                                std::thread::yield_now();
                            }
                            Ok(Submission::Ready(Ok(Outcome::Success(output)))) => {
                                assert_eq!(output, sequence);
                                latency.push(start.elapsed().as_nanos());
                                break;
                            }
                            Ok(Submission::Ready(Err(error))) => return Err(error.into()),
                            _ => return Err(Error::InvalidState("基线预期同步完成").into()),
                        }
                    }
                }
                session.close(Deadline(Instant::now() + Duration::from_secs(5)))?;
                Ok((latency, retries))
            }));
        }
        let start = Instant::now();
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|h| h.join().expect("工作线程完成"))
            .collect::<Vec<_>>();
        (start.elapsed(), results)
    });
    let mut latency = Vec::with_capacity(count);
    let mut retries = 0;
    for result in results {
        let (mut samples, n) = result?;
        latency.append(&mut samples);
        retries += n;
    }
    assert_eq!(latency.len(), count);
    latency.sort_unstable();
    store.shutdown(Deadline(Instant::now() + Duration::from_secs(5)))?;
    Ok((
        count as f64 / elapsed.as_secs_f64(),
        latency[count / 2],
        latency[count * 95 / 100],
        latency[count * 99 / 100],
        retries,
    ))
}
fn main() -> ExampleResult<()> {
    println!("布局,分布,线程,轮次,操作数,每秒操作,P50纳秒,P95纳秒,P99纳秒,拒绝重试");
    for variable in [false, true] {
        for hot in [false, true] {
            for threads in [1, 4] {
                for round in 1..=3 {
                    let result = if variable {
                        run(
                            SchemaPair::new(U64Key, SerializedValue::new(ByteValueCodec)),
                            |key, sequence| Bytes {
                                key,
                                value: vec![sequence as u8; ((sequence % 16 + 1) * 32) as usize],
                                sequence,
                            },
                            threads,
                            hot,
                        )?
                    } else {
                        run(
                            SchemaPair::new(U64Key, AtomicU64Value),
                            |key, value| Number { key, value },
                            threads,
                            hot,
                        )?
                    };
                    println!(
                        "{},{},{threads},{round},20000,{:.0},{},{},{},{}",
                        if variable {
                            "变长32至512字节"
                        } else {
                            "原子u64"
                        },
                        if hot { "单键热点" } else { "256键均匀" },
                        result.0,
                        result.1,
                        result.2,
                        result.3,
                        result.4
                    );
                }
            }
        }
    }
    Ok(())
}
