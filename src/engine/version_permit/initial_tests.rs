use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        KeyCodec, ValueRead, ValueUpdate,
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
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
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
        value.view_mut().store(self.0, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.0))
    }
}
impl RmwOperation<Schema> for Request {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.0, self.0))
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        let next = value.view().wrapping_add(self.0);
        Ok((next, next))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Updated(
            value
                .view_mut()
                .fetch_add(self.0, Ordering::SeqCst)
                .wrapping_add(self.0),
        ))
    }
}
impl DeleteOperation<Schema> for Request {
    type Output = u64;
    fn complete(self, _: DeleteOutcome) -> u64 {
        1
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}

#[test]
fn resident_operations_do_not_wait_for_an_empty_version_registry() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut owner = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        owner.upsert(Serial(0), Request(7)),
        Ok(Submission::Ready(Ok(Outcome::Success(7))))
    ));
    let result = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (start_tx, start_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            ready_tx.send(()).unwrap();
            start_rx.recv().unwrap();
            let read = matches!(
                session.read(Serial(1), Request(0), Default::default()),
                Ok(Submission::Ready(Ok(Outcome::Success(7))))
            );
            let put = matches!(
                session.upsert(Serial(2), Request(11)),
                Ok(Submission::Ready(Ok(Outcome::Success(11))))
            );
            let rmw = matches!(
                session.rmw(Serial(3), Request(2), Default::default()),
                Ok(Submission::Ready(Ok(Outcome::Success(13))))
            );
            let delete = matches!(
                session.delete(Serial(4), Request(0), Default::default()),
                Ok(Submission::Ready(Ok(Outcome::Success(1))))
            );
            result_tx
                .send((read, put, rmw, delete, session.last_accepted()))
                .unwrap();
            session.close(deadline()).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let registry = store
            .inner
            .version_permits
            .shard(U64Key.hash(&1))
            .active
            .lock()
            .unwrap();
        assert!(registry.is_empty());
        start_tx.send(()).unwrap();
        let early = result_rx.recv_timeout(Duration::from_secs(2));
        // Release the deliberate stall before checking late output or joining.
        drop(registry);
        let completed = match early {
            Ok(result) => result,
            Err(_) => result_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        };
        worker.join().unwrap();
        assert_eq!(completed, (true, true, true, true, Some(Serial(4))));
        early
    });
    assert!(matches!(
        owner.read(Serial(1), Request(0), Default::default()),
        Ok(Submission::Ready(Ok(Outcome::NotFound)))
    ));
    owner.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    assert_eq!(result, Ok((true, true, true, true, Some(Serial(4)))));
}

#[test]
fn real_pending_operations_register_before_return_and_hold_back_a_newer_version() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Request(7)),
        Ok(Submission::Ready(Ok(Outcome::Success(7))))
    ));
    let hash = U64Key.hash(&1);
    let crate::index::IndexHead::Log(address) = store.inner.index.prepare(hash).unwrap().head
    else {
        panic!("preloaded record exists")
    };
    let (mut tickets, newer) = std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let holder = scope.spawn(move || {
            owner
                .inner
                .log
                .lease(address)
                .unwrap()
                .read(|_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let submissions = [
            session.read(Serial(1), Request(0), Default::default()),
            session.upsert(Serial(2), Request(11)),
            session.rmw(Serial(3), Request(2), Default::default()),
            session.delete(Serial(4), Request(0), Default::default()),
        ];
        let registrations = store
            .inner
            .version_permits
            .shard(hash)
            .with_state(|active| Ok(active.get(&(hash.0, 0)).copied()))
            .unwrap();
        let newer = store
            .inner
            .version_permits
            .reserve(hash, CheckpointVersion(1))
            .unwrap();
        let newer_ready = newer.ready().unwrap();
        let rejected = session.read(Serial(5), Request(0), Default::default());
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(registrations, Some(4));
        assert!(!newer_ready);
        assert!(matches!(
            rejected,
            Err(Rejected {
                reason: Error::Busy,
                ..
            })
        ));
        assert_eq!(session.last_accepted(), Some(Serial(4)));
        let tickets = submissions
            .into_iter()
            .map(|submitted| match submitted {
                Ok(Submission::Pending(ticket)) => ticket,
                _ => panic!("the value gate must suspend the actual operation"),
            })
            .collect::<Vec<_>>();
        (tickets, newer)
    });
    for _ in 0..8 {
        session.poll(PollBudget::default()).unwrap();
    }
    for (ticket, expected) in tickets.iter_mut().zip([7, 11, 13, 1]) {
        assert!(
            matches!(ticket.try_take().unwrap(),crate::api::completion::TicketState::Ready(Ok(Outcome::Success(value))) if value==expected)
        );
        assert!(ticket.try_take().is_err());
    }
    assert!(newer.ready().unwrap());
    drop(newer);
    assert!(
        !store
            .inner
            .version_permits
            .shard(hash)
            .has_registrations
            .load(Ordering::Acquire)
    );
    assert!(matches!(
        session.read(Serial(5), Request(0), Default::default()),
        Ok(Submission::Ready(Ok(Outcome::NotFound)))
    ));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn initial_hints_preserve_poisoning_activation_identity_and_reference_counts() {
    let permits = VersionPermits::default();
    let hash = KeyHash(7);
    let mut initial = permits
        .reserve_initial(hash, CheckpointVersion(u64::MAX))
        .unwrap();
    assert!(initial.is_none());
    assert!(permits.ready_initial(hash, initial.as_ref()).unwrap());
    permits
        .activate(hash, CheckpointVersion(u64::MAX), &mut initial)
        .unwrap();
    permits
        .activate(hash, CheckpointVersion(u64::MAX), &mut initial)
        .unwrap();
    assert_eq!(
        permits
            .shard(hash)
            .with_state(|active| Ok(active.get(&(hash.0, u64::MAX)).copied()))
            .unwrap(),
        Some(1)
    );
    assert!(permits.reserve_initial(hash, CheckpointVersion(0)).is_err());
    assert!(
        permits
            .activate(hash, CheckpointVersion(0), &mut initial)
            .is_err()
    );
    let other = VersionPermits::default();
    assert!(
        other
            .activate(hash, CheckpointVersion(u64::MAX), &mut initial)
            .is_err()
    );
    assert!(other.ready_initial(hash, initial.as_ref()).is_err());
    drop(initial);
    assert!(
        permits
            .reserve_initial(hash, CheckpointVersion(0))
            .unwrap()
            .is_none()
    );
    let idle = permits.reserve_initial(hash, CheckpointVersion(0)).unwrap();
    assert!(
        std::panic::catch_unwind(|| {
            let _ = permits
                .shard(KeyHash(8))
                .with_state::<()>(|_| panic!("injected version registry poison"));
        })
        .is_err()
    );
    assert!(permits.ready_initial(hash, idle.as_ref()).is_err());
    assert!(permits.reserve_initial(hash, CheckpointVersion(0)).is_err());
}
