use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    config::Config,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
use std::{sync::mpsc, time::Duration};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Request;
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((42, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
#[test]
fn public_read_does_not_wait_for_an_unchanged_routing_write_lock() {
    let mut config = Config::default();
    config.index.buckets = 1;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut preload = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        preload.upsert(Serial(0), Request),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    drop(preload);
    let (started_tx, started_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            started_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            let value = match session.read(Serial(0), Request, ReadOptions::default()) {
                Ok(Submission::Ready(Ok(Outcome::Success(value)))) => value,
                _ => panic!("Expected a completed resident read"),
            };
            result_tx.send(value).unwrap();
        });
        started_rx.recv().unwrap();
        let state = store.inner.index.state.write().unwrap();
        go_tx.send(()).unwrap();
        let while_locked = result_rx.recv_timeout(Duration::from_secs(5));
        drop(state);
        worker.join().unwrap();
        assert_eq!(
            while_locked,
            Ok(42),
            "Read waited for an unchanged routing lock"
        );
    });
}

struct SetValue;
impl Keyed<Schema> for SetValue {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for SetValue {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((43, ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        value
            .view_mut()
            .store(43, std::sync::atomic::Ordering::SeqCst);
        Ok(UpdateDecision::Updated(()))
    }
}

#[test]
fn public_upsert_does_not_wait_for_an_unchanged_routing_write_lock() {
    let mut config = Config::default();
    config.index.buckets = 1;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut preload = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        preload.upsert(Serial(0), Request),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    drop(preload);
    let (started_tx, started_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            assert!(matches!(
                session.upsert(Serial(0), SetValue),
                Ok(Submission::Ready(Ok(Outcome::Success(()))))
            ));
            started_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            let completed = matches!(
                session.upsert(Serial(1), SetValue),
                Ok(Submission::Ready(Ok(Outcome::Success(()))))
            );
            result_tx.send(completed).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let state = store.inner.index.state.write().unwrap();
        go_tx.send(()).unwrap();
        let while_locked = result_rx.recv_timeout(Duration::from_secs(5));
        drop(state);
        worker.join().unwrap();
        assert_eq!(
            while_locked,
            Ok(true),
            "Upsert waited for an unchanged routing lock"
        );
    });
}
