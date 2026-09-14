use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    coordination::{Action, Phase},
    schema::{
        KeyCodec, ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
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

fn store() -> RasterKV<Schema> {
    let mut config = crate::config::Config::default();
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
    store
}

fn read_while_another_thread_holds_the_stripe(prepare: bool) -> (bool, Option<Serial>) {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let gate = &store.inner.operations[U64Key.hash(&7).0 as usize % store.inner.operations.len()];
    let action = prepare.then(|| {
        store
            .inner
            .coordinator
            .start_action(Action::CheckpointLog)
            .unwrap()
    });
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let result = std::thread::scope(|scope| {
        let holder = scope.spawn(move || {
            let _guard = gate.lock().unwrap();
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let submitted = session.read(Serial(7), Request, ReadOptions::default());
        let accepted = session.last_accepted();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        (
            matches!(submitted, Ok(Submission::Ready(Ok(Outcome::Success(42))))),
            accepted,
        )
    });
    if let Some(action) = action {
        // This fixture tests admission, not checkpoint material publication.
        store
            .inner
            .coordinator
            .fail_action(action, Error::SessionAbandoned)
            .unwrap();
    }
    result
}

#[test]
fn read_in_rest_completes_while_the_operation_stripe_is_held() {
    assert_eq!(
        read_while_another_thread_holds_the_stripe(false),
        (true, Some(Serial(7)))
    );
}

#[test]
fn read_in_prepare_still_requires_the_operation_stripe_before_acceptance() {
    assert_eq!(
        read_while_another_thread_holds_the_stripe(true),
        (false, None)
    );
}

#[test]
fn read_in_rest_rejects_a_poisoned_operation_stripe_without_acceptance() {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let gate = &store.inner.operations[U64Key.hash(&7).0 as usize % store.inner.operations.len()];
    assert!(
        std::panic::catch_unwind(|| {
            let _guard = gate.lock().unwrap();
            panic!("injected operation stripe poison");
        })
        .is_err()
    );
    assert!(matches!(
        session.read(Serial(7), Request, ReadOptions::default()),
        Err(Rejected {
            reason: Error::Busy,
            ..
        })
    ));
    assert_eq!(session.last_accepted(), None);
}

struct BlockingRead {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}
impl Keyed<Schema> for BlockingRead {
    fn key(&self) -> &u64 {
        &7
    }
}
impl ReadOperation<Schema> for BlockingRead {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
        Ok(*value.view())
    }
}

#[test]
fn a_checkpoint_started_inside_a_rest_read_waits_for_that_sessions_next_observation() {
    let store = store();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (command_tx, command_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    std::thread::scope(|scope| {
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            let result = session.read(
                Serial(7),
                BlockingRead {
                    entered: entered_tx,
                    release: release_rx,
                },
                ReadOptions::default(),
            );
            done_tx
                .send(matches!(
                    result,
                    Ok(Submission::Ready(Ok(Outcome::Success(42))))
                ))
                .unwrap();
            while command_rx.recv().unwrap() {
                session.refresh().unwrap();
                done_tx.send(true).unwrap();
            }
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let coordinator = &store.inner.coordinator;
        let action = coordinator.start_action(Action::CheckpointLog).unwrap();
        let while_reading = coordinator.advance(action, Phase::Prepare);
        release_tx.send(()).unwrap();
        let read_finished = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let before_refresh = coordinator.advance(action, Phase::Prepare);
        // Release the worker before reporting a failed barrier assertion.
        if !matches!(while_reading, Err(Error::Busy)) || !matches!(before_refresh, Err(Error::Busy))
        {
            command_tx.send(false).unwrap();
            worker.join().unwrap();
            panic!("checkpoint version advanced without the reading session's observation");
        }
        for phase in [Phase::Prepare, Phase::InProgress, Phase::WaitPending] {
            command_tx.send(true).unwrap();
            assert!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
            coordinator.advance(action, phase).unwrap();
        }
        coordinator.advance(action, Phase::WaitFlush).unwrap();
        coordinator.finish_action(action).unwrap();
        command_tx.send(false).unwrap();
        worker.join().unwrap();
        assert!(read_finished);
    });
}

struct OrderedRead {
    order: std::rc::Rc<std::cell::RefCell<Vec<u64>>>,
    number: u64,
}
impl Keyed<Schema> for OrderedRead {
    fn key(&self) -> &u64 {
        &7
    }
}
impl ReadOperation<Schema> for OrderedRead {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.order.borrow_mut().push(self.number);
        Ok(*value.view())
    }
}

#[test]
fn a_rest_read_registers_its_pending_version_before_the_next_checkpoint_observation() {
    use std::{cell::RefCell, rc::Rc, time::Instant};
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let entry = store.inner.index.prepare(U64Key.hash(&7)).unwrap();
    let crate::index::IndexHead::Log(address) = entry.head else {
        panic!("missing resident record")
    };
    let order = Rc::new(RefCell::new(Vec::new()));
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let holder_engine = store.inner.clone();
    let submission = std::thread::scope(|scope| {
        let holder = scope.spawn(move || {
            let lease = holder_engine.log.lease(address).unwrap();
            drop(holder_engine);
            lease
                .read(|_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let submitted = session.read(
            Serial(7),
            OrderedRead {
                order: order.clone(),
                number: 0,
            },
            ReadOptions::default(),
        );
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        submitted
    });
    let Submission::Pending(mut old) = submission.map_err(|error| error.reason).unwrap() else {
        panic!("the held value must suspend the original Read")
    };
    let coordinator = &store.inner.coordinator;
    let action = coordinator.start_action(Action::CheckpointLog).unwrap();
    session.refresh().unwrap();
    let state = coordinator.advance(action, Phase::Prepare).unwrap();
    assert_eq!(state.version, CheckpointVersion(1));
    session.refresh().unwrap();
    // The value is no longer contended. Only the original version registration
    // prevents this newer Read from completing before the suspended request.
    let next = session.read(
        Serial(8),
        OrderedRead {
            order: order.clone(),
            number: 1,
        },
        ReadOptions::default(),
    );
    let Submission::Pending(mut next) = next.map_err(|error| error.reason).unwrap() else {
        panic!("a newer Read overtook the suspended old version")
    };
    assert!(order.borrow().is_empty());
    let deadline = Deadline(Instant::now() + Duration::from_secs(5));
    for ticket in [&mut old, &mut next] {
        assert!(matches!(
            session.wait(ticket, deadline).unwrap(),
            Ok(Outcome::Success(42))
        ));
    }
    assert_eq!(*order.borrow(), vec![0, 1]);
    assert_eq!(session.last_accepted(), Some(Serial(8)));
    coordinator.advance(action, Phase::InProgress).unwrap();
    session.refresh().unwrap();
    coordinator.advance(action, Phase::WaitPending).unwrap();
    coordinator.advance(action, Phase::WaitFlush).unwrap();
    coordinator.finish_action(action).unwrap();
    session.close(deadline).unwrap();
}

#[test]
fn concurrent_atomic_reads_finish_while_another_read_callback_is_held() {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let second = std::thread::scope(|scope| {
        let store = &store;
        let first = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            session
                .read(
                    Serial(7),
                    BlockingRead {
                        entered: entered_tx,
                        release: release_rx,
                    },
                    ReadOptions::default(),
                )
                .map(|result| matches!(result, Submission::Ready(Ok(Outcome::Success(42)))))
                .map_err(|rejected| rejected.reason)
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = session.read(Serial(7), Request, ReadOptions::default());
        release_tx.send(()).unwrap();
        assert!(matches!(first.join().unwrap(), Ok(true)));
        second
    });
    let ready = matches!(&second, Ok(Submission::Ready(Ok(Outcome::Success(42)))));
    if let Ok(Submission::Pending(mut ticket)) = second {
        assert!(matches!(
            session
                .wait(
                    &mut ticket,
                    Deadline(std::time::Instant::now() + Duration::from_secs(5))
                )
                .unwrap(),
            Ok(Outcome::Success(42))
        ));
    }
    assert!(
        ready,
        "an atomic Read should not wait for another Read callback"
    );
    assert_eq!(session.last_accepted(), Some(Serial(7)));
}

struct UpdateDuringRead;
impl Keyed<Schema> for UpdateDuringRead {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for UpdateDuringRead {
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
impl DeleteOperation<Schema> for UpdateDuringRead {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {}
}

#[test]
fn a_held_atomic_read_allows_an_update_but_delays_tombstone_publication() {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (updated, deleted) = std::thread::scope(|scope| {
        let store = &store;
        let first = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            session
                .read(
                    Serial(7),
                    BlockingRead {
                        entered: entered_tx,
                        release: release_rx,
                    },
                    ReadOptions::default(),
                )
                .map(|result| matches!(result, Submission::Ready(Ok(Outcome::Success(42)))))
                .map_err(|rejected| rejected.reason)
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let updated = session.upsert(Serial(7), UpdateDuringRead);
        let deleted = session.delete(
            Serial(8),
            UpdateDuringRead,
            DeleteOptions {
                force_tombstone: true,
            },
        );
        release_tx.send(()).unwrap();
        assert!(matches!(first.join().unwrap(), Ok(true)));
        (updated, deleted)
    });
    assert!(matches!(
        updated,
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    let Submission::Pending(mut ticket) = deleted.map_err(|rejected| rejected.reason).unwrap()
    else {
        panic!("tombstone publication must wait for the held read permit");
    };
    assert!(matches!(
        session
            .wait(
                &mut ticket,
                Deadline(std::time::Instant::now() + Duration::from_secs(5))
            )
            .unwrap(),
        Ok(Outcome::Success(()))
    ));
    assert!(matches!(
        session.read(Serial(9), Request, ReadOptions::default()),
        Ok(Submission::Ready(Ok(Outcome::NotFound)))
    ));
}
