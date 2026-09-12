use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Request(u64);
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &1
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.0, self.0))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        value
            .view_mut()
            .store(self.0, std::sync::atomic::Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.0))
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}

#[test]
fn resident_operations_do_not_wait_for_unchanged_registration_metadata() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut owner_session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        owner_session.upsert(Serial(0), Request(7)),
        Ok(Submission::Ready(Ok(Outcome::Success(7))))
    ));
    let result = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (start_tx, start_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let worker = scope.spawn(move || {
            let mut session = owner.start_session(Default::default()).unwrap();
            ready_tx.send(()).unwrap();
            start_rx.recv().unwrap();
            // Drive the public operations, including phase observation and
            // serial admission, while another thread holds unchanged metadata.
            let read = session.read(Serial(1), Request(0), ReadOptions::default());
            let write = session.upsert(Serial(2), Request(11));
            let result = match (read, write, session.last_accepted()) {
                (
                    Ok(Submission::Ready(Ok(Outcome::Success(7)))),
                    Ok(Submission::Ready(Ok(Outcome::Success(11)))),
                    Some(Serial(2)),
                ) => Ok(()),
                _ => Err("resident operations or accepted serial did not match"),
            };
            result_tx.send(result).unwrap();
            session.close(deadline()).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let registry = store.inner.coordinator.registry.lock().unwrap();
        assert_eq!(
            registry
                .sessions
                .get(&owner_session.id())
                .unwrap()
                .last(&store.inner.coordinator.failed)
                .unwrap(),
            Some(Serial(0))
        );
        start_tx.send(()).unwrap();
        let result = result_rx.recv_timeout(Duration::from_secs(2));
        // Release the deliberate stall before joining even on the old path.
        drop(registry);
        worker.join().unwrap();
        if result.is_err() {
            assert_eq!(
                result_rx.recv_timeout(Duration::from_secs(5)),
                Ok(Ok(())),
                "operations and accepted serial must complete after the deliberate stall"
            );
        }
        result
    });
    assert!(matches!(
        owner_session.read(Serial(1), Request(0), ReadOptions::default()),
        Ok(Submission::Ready(Ok(Outcome::Success(11))))
    ));
    owner_session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    assert_eq!(
        result,
        Ok(Ok(())),
        "resident operations must not wait for unchanged registration metadata"
    );
}
