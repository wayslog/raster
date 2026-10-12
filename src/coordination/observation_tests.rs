use super::*;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};
fn id(n: u8) -> SessionId {
    SessionId([n; 16])
}

#[test]
fn unchanged_rest_observation_does_not_wait_for_the_session_slot() {
    let coordinator = Arc::new(Coordinator::new(1).unwrap());
    let session = coordinator.enroll_registered(id(1)).unwrap();
    let mut cache = None;
    assert_eq!(
        coordinator
            .observed_session_state(&session.handle, id(1), &mut cache)
            .unwrap()
            .phase,
        Phase::Rest
    );
    let slot = session.handle.slot.clone();
    let held = slot.state.lock().unwrap();
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let observer = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            done_tx
                .send(coordinator.observed_session_state(&session.handle, id(1), &mut cache))
                .unwrap()
        })
    };
    let early = done_rx.recv_timeout(Duration::from_millis(100));
    drop(held);
    observer.join().unwrap();
    assert!(
        early.is_ok(),
        "unchanged REST observation waited for the session slot"
    );
    assert_eq!(early.unwrap().unwrap().phase, Phase::Rest);
}

#[test]
fn cached_rest_observes_phase_version_and_preserves_serial_admission() {
    let coordinator = Coordinator::new(1).unwrap();
    let session = coordinator.enroll_registered(id(1)).unwrap();
    let mut cache = None;
    let state = coordinator
        .observed_session_state(&session.handle, id(1), &mut cache)
        .unwrap();
    coordinator
        .accept_registered(&session.handle, id(1), Serial(7), state.version)
        .unwrap();
    assert!(
        coordinator
            .accept_registered(&session.handle, id(1), Serial(7), state.version)
            .is_err()
    );
    assert_eq!(coordinator.last_accepted(id(1)).unwrap(), Some(Serial(7)));
    assert!(
        coordinator
            .observed_session_state(&session.handle, id(2), &mut cache)
            .is_err()
    );
    assert!(
        Coordinator::new(1)
            .unwrap()
            .observed_session_state(&session.handle, id(1), &mut cache)
            .is_err()
    );
    let action = coordinator.start_action(Action::CheckpointLog).unwrap();
    let state = coordinator
        .observed_session_state(&session.handle, id(1), &mut cache)
        .unwrap();
    assert_eq!(state.phase, Phase::Prepare);
    coordinator
        .acknowledge(
            action,
            SessionCut {
                session: id(1),
                last_accepted: Some(Serial(7)),
                old_pending: 0,
            },
            Phase::Prepare,
        )
        .unwrap();
    coordinator.advance(action, Phase::Prepare).unwrap();
    let state = coordinator
        .observed_session_state(&session.handle, id(1), &mut cache)
        .unwrap();
    assert_eq!(
        (state.phase, state.version),
        (Phase::InProgress, CheckpointVersion(1))
    );
    coordinator
        .fail_action(action, Error::SessionAbandoned)
        .unwrap();
    assert_eq!(
        coordinator
            .observed_session_state(&session.handle, id(1), &mut cache)
            .unwrap()
            .phase,
        Phase::Failed
    );
}

#[test]
fn cached_activations_cannot_survive_departure_or_recovery_resumption() {
    for recovered in [false, true] {
        for drained in [false, true] {
            let coordinator = if recovered {
                Coordinator::from_checkpoint(1, CheckpointVersion(3), &[(id(1), Serial(5))])
                    .unwrap()
            } else {
                Coordinator::new(1).unwrap()
            };
            let first = if recovered {
                coordinator.resume_registered(id(1)).unwrap().0
            } else {
                coordinator.enroll_registered(id(1)).unwrap()
            };
            let mut cache = None;
            coordinator
                .observed_session_state(&first.handle, id(1), &mut cache)
                .unwrap();
            if drained {
                coordinator
                    .leave_drained(
                        (
                            first.version,
                            SessionCut {
                                session: id(1),
                                last_accepted: first.last_accepted,
                                old_pending: 0,
                            },
                        ),
                        None,
                    )
                    .unwrap();
            } else {
                coordinator.leave(id(1)).unwrap();
            }
            assert!(
                coordinator
                    .observed_session_state(&first.handle, id(1), &mut cache)
                    .is_err()
            );
            let next = if recovered {
                coordinator.resume_registered(id(1)).unwrap().0
            } else {
                coordinator.enroll_registered(id(1)).unwrap()
            };
            coordinator
                .observed_session_state(&next.handle, id(1), &mut cache)
                .unwrap();
            assert!(
                coordinator
                    .observed_session_state(&first.handle, id(1), &mut cache)
                    .is_err()
            );
            coordinator.leave(id(1)).unwrap();
            coordinator.shutdown().unwrap();
            assert!(
                coordinator
                    .observed_session_state(&next.handle, id(1), &mut cache)
                    .is_err()
            );
        }
    }
}

#[test]
fn warmed_rest_cache_waits_for_complete_publication() {
    let coordinator = Arc::new(Coordinator::new(2).unwrap());
    let first = coordinator.enroll_registered(id(1)).unwrap();
    let second = coordinator.enroll_registered(id(2)).unwrap();
    let mut cache = None;
    coordinator
        .observed_session_state(&second.handle, id(2), &mut cache)
        .unwrap();
    let held = first.handle.slot.state.lock().unwrap();
    let publisher = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || coordinator.start_action(Action::CheckpointLog))
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !coordinator.publishing.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let publishing = coordinator.publishing.load(Ordering::SeqCst);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let observer = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            done_tx
                .send(coordinator.observed_session_state(&second.handle, id(2), &mut cache))
                .unwrap()
        })
    };
    let early = done_rx.recv_timeout(Duration::from_millis(20));
    drop(held);
    assert!(publisher.join().unwrap().is_ok());
    observer.join().unwrap();
    assert!(publishing);
    assert!(early.is_err(), "cached REST escaped an active publication");
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .phase,
        Phase::Prepare
    );
}

#[test]
fn warmed_rest_cache_does_not_hide_poison_or_partial_publication_failure() {
    for mode in 0..3 {
        let coordinator = Coordinator::new(2).unwrap();
        let first = coordinator.enroll_registered(id(1)).unwrap();
        let second = coordinator.enroll_registered(id(2)).unwrap();
        let mut cache = None;
        coordinator
            .observed_session_state(&first.handle, id(1), &mut cache)
            .unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if mode == 0 {
                let _guard = first.handle.slot.state.lock().unwrap();
                panic!("injected session poison");
            }
            if mode == 1 {
                let _guard = coordinator.registry.lock().unwrap();
                panic!("injected registry poison");
            }
            let _guard = second.handle.slot.state.lock().unwrap();
            panic!("injected publication poison");
        }));
        if mode == 2 {
            assert!(coordinator.start_action(Action::CheckpointLog).is_err());
        }
        assert!(
            coordinator
                .observed_session_state(&first.handle, id(1), &mut cache)
                .is_err()
        );
        assert!(
            coordinator
                .accept_registered(&first.handle, id(1), Serial(9), first.version)
                .is_err()
        );
    }
}

#[test]
fn saturated_observation_revision_disables_caching_without_wrapping() {
    let coordinator = Coordinator::new(1).unwrap();
    let session = coordinator.enroll_registered(id(1)).unwrap();
    session
        .handle
        .slot
        .observation_revision
        .store(u64::MAX - 1, Ordering::SeqCst);
    let mut cache = None;
    coordinator
        .observed_session_state(&session.handle, id(1), &mut cache)
        .unwrap();
    let action = coordinator.start_action(Action::Compact).unwrap();
    assert_eq!(
        coordinator
            .observed_session_state(&session.handle, id(1), &mut cache)
            .unwrap()
            .phase,
        Phase::Compacting
    );
    coordinator.advance(action, Phase::Compacting).unwrap();
    coordinator.finish_action(action).unwrap();
    assert_eq!(
        coordinator
            .observed_session_state(&session.handle, id(1), &mut cache)
            .unwrap()
            .phase,
        Phase::Rest
    );
    assert!(cache.is_none());
    assert_eq!(
        session
            .handle
            .slot
            .observation_revision
            .load(Ordering::SeqCst),
        u64::MAX
    );
}
