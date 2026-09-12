//! Writes after freezing use real Session and log,Do not execute old value update callback.
use crate::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    config::Config,
    schema::{
        KeyCodec, ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Put(u64, u64);
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.1, self.1))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        panic!("Frozen records should not enter the in-place write callback")
    }
}
struct Add(u64, u64);
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        panic!("Existing frozen values cannot be used as missing keys")
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        let result = value.view().wrapping_add(self.1);
        Ok((result, result))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        panic!("Frozen records should not be entered in place RMW callback")
    }
}
fn success(result: Submission<u64>, expected: u64) {
    assert!(matches!(result, Submission::Ready(Ok(Outcome::Success(value))) if value == expected));
}
#[test]
fn after_freezing_replacement_copy_and_append_retain_the_original_value_and_the_engine_continues_to_serve()
 {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..60 {
        success(
            session
                .upsert(Serial(key), Put(key, key))
                .map_err(|r| r.reason)
                .unwrap(),
            key,
        );
    }
    let find = |key| {
        let entry = store.inner.index.prepare(U64Key.hash(&key)).unwrap();
        store
            .inner
            .log
            .find(&U64Key, &key, super::Engine::<Schema>::head(entry).unwrap())
            .unwrap()
            .unwrap()
    };
    let first = find(0);
    let second = find(1);
    store.inner.log.advance_read_only(LogAddress(4096)).unwrap();
    success(
        session
            .upsert(Serial(60), Put(0, 100))
            .map_err(|r| r.reason)
            .unwrap(),
        100,
    );
    success(
        session
            .rmw(Serial(61), Add(1, 41), RmwOptions::default())
            .map_err(|r| r.reason)
            .unwrap(),
        42,
    );
    assert_eq!(first.read(|v| v).unwrap(), 0);
    assert_eq!(second.read(|v| v).unwrap(), 1);
    assert_eq!(find(0).read(|v| v).unwrap(), 100);
    assert_eq!(find(1).read(|v| v).unwrap(), 42);
    assert_eq!(session.last_accepted(), Some(Serial(61)));
    assert!(!store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        store.inner.log.frontiers().unwrap().flushed_until,
        LogAddress(0)
    );
}
