//! Run the bounded memory counter and record the single-threaded hotspot RMW baseline.
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
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Counter;
impl Keyed<Schema> for Counter {
    fn key(&self) -> &u64 {
        &1
    }
}
impl RmwOperation<Schema> for Counter {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        Ok((1, 1))
    }
    fn copy_update(&mut self, v: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        let n = v.view().wrapping_add(1);
        Ok((n, n))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Updated(
            v.view_mut().fetch_add(1, Ordering::SeqCst).wrapping_add(1),
        ))
    }
}
impl ReadOperation<Schema> for Counter {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*v.view())
    }
}
impl DeleteOperation<Schema> for Counter {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {}
}
fn ready<T: 'static>(result: Submission<T>) -> Result<T, Box<dyn std::error::Error>> {
    match result {
        Submission::Ready(Ok(Outcome::Success(value))) => Ok(value),
        Submission::Ready(Err(error)) => Err(error.into()),
        _ => Err(Error::InvalidState("Example expects immediate success").into()),
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()?;
    let mut session = store.start_session(SessionOptions::default())?;
    let count = 10_000;
    let mut latencies = Vec::with_capacity(count as usize);
    let start = Instant::now();
    for serial in 0..count {
        let before = Instant::now();
        let value = ready(
            session
                .rmw(Serial(serial), Counter, RmwOptions::default())
                .map_err(|r| r.reason)?,
        )?;
        latencies.push(before.elapsed().as_nanos());
        assert_eq!(value, serial + 1);
    }
    let elapsed = start.elapsed();
    latencies.sort_unstable();
    assert_eq!(
        ready(
            session
                .read(Serial(count), Counter, ReadOptions::default())
                .map_err(|r| r.reason)?
        )?,
        count
    );
    ready(
        session
            .delete(Serial(count + 1), Counter, DeleteOptions::default())
            .map_err(|r| r.reason)?,
    )?;
    let deadline = Deadline(Instant::now() + Duration::from_secs(5));
    session.close(deadline)?;
    store.shutdown(deadline)?;
    println!(
        "Memory counter life cycle passes:{count} times RMW,Read verification,delete,Closed successfully"
    );
    println!(
        "Single-threaded hotspot baseline:{:.0} operation/seconds,P50={}ns P95={}ns P99={}ns",
        count as f64 / elapsed.as_secs_f64(),
        latencies[count as usize / 2],
        latencies[count as usize * 95 / 100],
        latencies[count as usize * 99 / 100]
    );
    Ok(())
}
