use super::*;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc,
    time::{Duration, Instant},
};

fn id(value: u8) -> SessionId {
    SessionId([value; 16])
}

fn cut(session: SessionId, serial: u64) -> SessionCut {
    SessionCut {
        session,
        last_accepted: Some(Serial(serial)),
        old_pending: 0,
    }
}

#[test]
fn handles_reject_wrong_owners_identities_and_old_activations() {
    let coordinator = Coordinator::new(2).unwrap();
    let first = coordinator.enroll_registered(id(1)).unwrap();
    coordinator
        .accept_registered(&first.handle, id(1), Serial(7), first.version)
        .unwrap();
    assert!(
        coordinator
            .accept_registered(&first.handle, id(2), Serial(9), first.version)
            .is_err()
    );
    let other = Coordinator::new(1).unwrap();
    let other_session = other.enroll_registered(id(1)).unwrap();
    assert!(
        other
            .accept_registered(&first.handle, id(1), Serial(9), first.version)
            .is_err()
    );
    assert_eq!(other.last_accepted(id(1)).unwrap(), None);
    assert!(
        coordinator
            .session_state(&other_session.handle, id(1))
            .is_err()
    );
    coordinator
        .leave_drained((first.version, cut(id(1), 7)), None)
        .unwrap();
    let second = coordinator.enroll_registered(id(1)).unwrap();
    assert_eq!(second.last_accepted, Some(Serial(7)));
    assert!(!Arc::ptr_eq(&first.handle.slot, &second.handle.slot));
    assert!(
        coordinator
            .accept_registered(&first.handle, id(1), Serial(11), second.version)
            .is_err()
    );
    assert_eq!(coordinator.last_accepted(id(1)).unwrap(), Some(Serial(7)));
    coordinator
        .accept_registered(&second.handle, id(1), Serial(11), second.version)
        .unwrap();
    assert!(
        coordinator
            .accept_registered(&second.handle, id(1), Serial(11), second.version)
            .is_err()
    );
    assert_eq!(coordinator.last_accepted(id(1)).unwrap(), Some(Serial(11)));
}

#[test]
fn recovery_preserves_latest_admission_and_original_durable_progress() {
    let coordinator = Coordinator::from_checkpoint(
        1,
        CheckpointVersion(7),
        &[(id(1), Serial(9)), (id(2), Serial(12))],
    )
    .unwrap();
    let (first, durable, version) = coordinator.resume_registered(id(1)).unwrap();
    assert_eq!((durable, version), (Serial(9), CheckpointVersion(7)));
    assert_eq!(first.version, CheckpointVersion(8));
    assert!(matches!(
        coordinator.resume_registered(id(2)),
        Err(Error::CapacityExceeded)
    ));
    coordinator
        .accept_registered(&first.handle, id(1), Serial(20), first.version)
        .unwrap();
    coordinator
        .leave_drained((first.version, cut(id(1), 20)), None)
        .unwrap();
    let (second, durable, version) = coordinator.resume_registered(id(1)).unwrap();
    assert_eq!((durable, version), (Serial(9), CheckpointVersion(7)));
    assert_eq!(second.last_accepted, Some(Serial(20)));
    assert!(
        coordinator
            .accept_registered(&first.handle, id(1), Serial(30), first.version)
            .is_err()
    );
    assert!(
        coordinator
            .accept_registered(&second.handle, id(1), Serial(19), second.version)
            .is_err()
    );
    assert_eq!(coordinator.last_accepted(id(1)).unwrap(), Some(Serial(20)));
    coordinator
        .accept_registered(&second.handle, id(1), Serial(30), second.version)
        .unwrap();
}

#[test]
fn one_session_slot_does_not_block_another_sessions_state_or_admission() {
    let coordinator = Coordinator::new(2).unwrap();
    let first = coordinator.enroll_registered(id(1)).unwrap();
    let second = coordinator.enroll_registered(id(2)).unwrap();
    std::thread::scope(|scope| {
        let (start_tx, start_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let owner = &coordinator;
        let worker = scope.spawn(move || {
            start_rx.recv().unwrap();
            let state = owner.session_state(&second.handle, id(2)).unwrap();
            let admitted =
                owner.accept_registered(&second.handle, id(2), Serial(19), state.version);
            done_tx.send((state, admitted.is_ok())).unwrap();
        });
        let received = first
            .handle
            .slot
            .with_state(&coordinator.failed, |_| {
                start_tx.send(()).unwrap();
                Ok(done_rx.recv_timeout(Duration::from_secs(2)))
            })
            .unwrap();
        worker.join().unwrap();
        let (state, admitted) = received.unwrap();
        assert_eq!(state.phase, Phase::Rest);
        assert!(admitted);
    });
    assert_eq!(coordinator.last_accepted(id(1)).unwrap(), None);
    assert_eq!(coordinator.last_accepted(id(2)).unwrap(), Some(Serial(19)));
}

#[test]
fn publication_waits_are_complete_and_release_the_slot_before_waiting_on_the_registry() {
    let coordinator = Arc::new(Coordinator::new(2).unwrap());
    let first = coordinator.enroll_registered(id(1)).unwrap();
    let second = coordinator.enroll_registered(id(2)).unwrap();
    for (handle, session) in [(&first.handle, id(1)), (&second.handle, id(2))] {
        coordinator
            .accept_registered(handle, session, Serial(7), CheckpointVersion(0))
            .unwrap();
    }
    let action = coordinator.start_action(Action::CheckpointLog).unwrap();
    for session in [id(1), id(2)] {
        coordinator
            .acknowledge(action, cut(session, 7), Phase::Prepare)
            .unwrap();
    }
    let stalled = first.handle.slot.state.lock().unwrap();
    let (published_tx, published_rx) = mpsc::sync_channel(1);
    let publisher = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            let state = coordinator.advance(action, Phase::Prepare);
            published_tx.send(state).unwrap();
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !coordinator.publishing.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let publishing = coordinator.publishing.load(Ordering::SeqCst);
    if !publishing {
        drop(stalled);
        panic!("publisher did not reach the held session slot");
    }
    let (observed_tx, observed_rx) = mpsc::sync_channel(1);
    let observer = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            let state = coordinator.session_state(&second.handle, id(2));
            let admitted = coordinator.accept_registered(
                &second.handle,
                id(2),
                Serial(9),
                CheckpointVersion(1),
            );
            observed_tx.send((state, admitted.is_ok())).unwrap();
        })
    };
    let early = observed_rx.recv_timeout(Duration::from_millis(20));
    drop(stalled);
    // Owned threads make a protocol deadlock a bounded test failure rather
    // than forcing a scoped join on the broken implementation.
    let published = published_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let observed = match early {
        Ok(value) => value,
        Err(_) => observed_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
    };
    publisher.join().unwrap();
    observer.join().unwrap();
    assert_eq!(published.phase, Phase::InProgress);
    assert_eq!(published.version, CheckpointVersion(1));
    let (observed_state, admitted) = observed;
    assert_eq!(observed_state.unwrap(), published);
    assert!(admitted);
    assert!(!coordinator.publishing.load(Ordering::SeqCst));
    assert_eq!(coordinator.last_accepted(id(2)).unwrap(), Some(Serial(9)));
}

#[test]
fn a_partial_publication_failure_cannot_expose_success_on_an_updated_slot() {
    let coordinator = Coordinator::new(2).unwrap();
    let first = coordinator.enroll_registered(id(1)).unwrap();
    let second = coordinator.enroll_registered(id(2)).unwrap();
    coordinator
        .accept_registered(&first.handle, id(1), Serial(7), first.version)
        .unwrap();
    // Bypass notification only to model a latent poison discovered after the
    // first slot was updated. Production access always uses with_state.
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = second.handle.slot.state.lock().unwrap();
            panic!("injected latent session poison");
        }))
        .is_err()
    );
    assert!(!coordinator.failed.load(Ordering::SeqCst));
    assert!(coordinator.start_action(Action::CheckpointLog).is_err());
    assert!(coordinator.failed.load(Ordering::SeqCst));
    assert!(!coordinator.publishing.load(Ordering::SeqCst));
    assert!(coordinator.snapshot().is_err());
    assert!(coordinator.session_state(&first.handle, id(1)).is_err());
    assert!(
        coordinator
            .accept_registered(&first.handle, id(1), Serial(9), first.version)
            .is_err()
    );
    let state = first.handle.slot.state.lock().unwrap();
    assert_eq!(state.system.phase, Phase::Prepare);
    assert_eq!(state.last_accepted, Some(Serial(7)));
}

#[test]
fn new_slot_poison_closes_all_sessions_but_existing_unwind_does_not() {
    let coordinator = Coordinator::new(2).unwrap();
    let first = coordinator.enroll_registered(id(1)).unwrap();
    let second = coordinator.enroll_registered(id(2)).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _: Result<(), Error> = first.handle.slot.with_state(&coordinator.failed, |_| {
                panic!("injected session access panic");
            });
        }))
        .is_err()
    );
    assert!(
        coordinator
            .accept_registered(&second.handle, id(2), Serial(7), second.version)
            .is_err()
    );
    assert!(coordinator.start_action(Action::Compact).is_err());

    struct Cleanup<'a> {
        coordinator: &'a Coordinator,
        closed: &'a AtomicBool,
    }
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            self.closed
                .store(self.coordinator.leave(id(1)).is_ok(), Ordering::SeqCst);
        }
    }
    let healthy = Coordinator::new(1).unwrap();
    healthy.enroll(id(1)).unwrap();
    let closed = AtomicBool::new(false);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _cleanup = Cleanup {
                coordinator: &healthy,
                closed: &closed,
            };
            panic!("outer application panic");
        }))
        .is_err()
    );
    assert!(closed.load(Ordering::SeqCst));
    assert!(healthy.snapshot().is_ok());
    assert!(healthy.enroll_registered(id(1)).is_ok());
}

#[test]
fn maximum_versions_and_serials_do_not_wrap_or_start_partial_publication() {
    let coordinator =
        Coordinator::from_checkpoint(1, CheckpointVersion(u64::MAX - 1), &[(id(1), Serial(7))])
            .unwrap();
    let (registered, _, _) = coordinator.resume_registered(id(1)).unwrap();
    let action = coordinator.start_action(Action::CheckpointLog).unwrap();
    coordinator
        .acknowledge(action, cut(id(1), 7), Phase::Prepare)
        .unwrap();
    let before = coordinator
        .session_state(&registered.handle, id(1))
        .unwrap();
    assert!(matches!(
        coordinator.advance(action, Phase::Prepare),
        Err(Error::CapacityExceeded)
    ));
    assert_eq!(
        coordinator
            .session_state(&registered.handle, id(1))
            .unwrap(),
        before
    );
    assert!(!coordinator.publishing.load(Ordering::SeqCst));
    coordinator
        .accept_registered(
            &registered.handle,
            id(1),
            Serial(u64::MAX),
            registered.version,
        )
        .unwrap();
    assert!(
        coordinator
            .accept_registered(
                &registered.handle,
                id(1),
                Serial(u64::MAX),
                registered.version
            )
            .is_err()
    );
    assert_eq!(
        coordinator.last_accepted(id(1)).unwrap(),
        Some(Serial(u64::MAX))
    );
}
