//! 仅用于单线程驻留内存成本归因；协议移除变体不是可交付引擎。
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*},
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
#[derive(Debug)]
struct Put(u64);
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &1
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.0, self.0))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        v.view_mut().store(self.0, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.0))
    }
}
#[derive(Debug)]
struct Read;
impl Keyed<Schema> for Read {
    fn key(&self) -> &u64 {
        &1
    }
}
impl ReadOperation<Schema> for Read {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*v.view())
    }
}
fn take(s: Submission<u64>) -> u64 {
    match s {
        Submission::Ready(Ok(Outcome::Success(value))) => value,
        _ => panic!("诊断只允许真实同步成功结果"),
    }
}
fn main() {
    assert_eq!(
        std::env::var("RASTER_COST_DIAGNOSTIC").as_deref(),
        Ok("仅单线程驻留内存"),
        "必须显式选择受限诊断"
    );
    let count: u64 = std::env::var("RASTER_COST_OPS")
        .unwrap_or_else(|_| "20000000".into())
        .parse()
        .unwrap();
    assert!((1000000..=40000000).contains(&count));
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let warmup = 1000;
    for serial in 1..=warmup {
        assert_eq!(
            take(session.upsert(Serial(serial), Put(serial)).unwrap()),
            serial
        );
    }
    eprintln!("诊断窗口就绪");
    let start = Instant::now();
    for serial in warmup + 1..=warmup + count {
        assert_eq!(
            take(session.upsert(Serial(serial), Put(serial)).unwrap()),
            serial
        );
    }
    let nanos = start.elapsed().as_nanos();
    let expected = warmup + count;
    assert_eq!(session.last_accepted(), Some(Serial(expected)));
    let read = take(
        session
            .read(Serial(expected + 1), Read, Default::default())
            .unwrap(),
    );
    assert_eq!(read, expected);
    let accepted = session.last_accepted().unwrap().0;
    let deadline = || Deadline(Instant::now() + Duration::from_secs(5));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    println!("操作数,预热,耗时纳秒,每秒操作,读取结果,接受序号");
    println!(
        "{count},{warmup},{nanos},{:.0},{read},{accepted}",
        count as f64 * 1e9 / nanos as f64
    );
}
