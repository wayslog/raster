use super::*;

#[test]
fn live_session_rest_observation_does_not_wait_for_the_registry() {
    use crate::{
        RasterKV,
        schema::builtin::{AtomicU64Value, SchemaPair, U64Key},
    };
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    let deadline = || Deadline(Instant::now() + Duration::from_secs(5));
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let result = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (start_tx, start_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let observer = scope.spawn(move || {
            let mut session = owner.start_session(Default::default()).unwrap();
            ready_tx.send(()).unwrap();
            start_rx.recv().unwrap();
            // Exercise the same phase-observation entry used by data operations,
            // with a real enrolled session. Admission remains a separate step.
            let observed = owner
                .inner
                .observe_session(&mut session.runtime)
                .map_err(|_| "phase observation failed");
            result_tx.send(observed).unwrap();
            session.close(deadline()).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let registry = store.inner.coordinator.registry.lock().unwrap();
        start_tx.send(()).unwrap();
        let result = result_rx.recv_timeout(Duration::from_secs(2));
        drop(registry);
        observer.join().unwrap();
        result
    });
    store.shutdown(deadline()).unwrap();
    assert_eq!(
        result,
        Ok(Ok(false)),
        "matching REST observation should not wait for the registry"
    );
}

#[test]
fn rest_observation_tracks_actions_versions_recovery_and_poisoning() {
    let coordinator = Coordinator::new(2).unwrap();
    assert!(coordinator.is_rest_at(CheckpointVersion(0)));
    assert!(!coordinator.is_rest_at(CheckpointVersion(1)));
    let action = coordinator.start_action(Action::CheckpointLog).unwrap();
    for phase in [
        Phase::Prepare,
        Phase::InProgress,
        Phase::WaitPending,
        Phase::WaitFlush,
    ] {
        assert!(!coordinator.is_rest_at(CheckpointVersion(0)));
        assert!(!coordinator.is_rest_at(CheckpointVersion(1)));
        coordinator.advance(action, phase).unwrap();
    }
    assert!(!coordinator.is_rest_at(CheckpointVersion(1)));
    coordinator.finish_action(action).unwrap();
    assert!(!coordinator.is_rest_at(CheckpointVersion(0)));
    assert!(coordinator.is_rest_at(CheckpointVersion(1)));
    let action = coordinator.start_action(Action::Compact).unwrap();
    assert!(!coordinator.is_rest_at(CheckpointVersion(1)));
    coordinator.fail_action(action, Error::Busy).unwrap();
    assert!(!coordinator.is_rest_at(CheckpointVersion(1)));

    let restored = Coordinator::from_checkpoint(2, CheckpointVersion(7), &[]).unwrap();
    assert!(!restored.is_rest_at(CheckpointVersion(0)));
    assert!(restored.is_rest_at(CheckpointVersion(8)));
    assert!(
        std::panic::catch_unwind(|| {
            let _guard = restored.registry.lock().unwrap();
            panic!("injected registry poison");
        })
        .is_err()
    );
    assert!(!restored.is_rest_at(CheckpointVersion(8)));
    assert!(restored.snapshot().is_err());

    let exhausted = Coordinator::from_checkpoint(2, CheckpointVersion(u64::MAX - 1), &[]).unwrap();
    assert!(!exhausted.is_rest_at(CheckpointVersion(u64::MAX)));
    assert_eq!(
        exhausted.snapshot().unwrap().version,
        CheckpointVersion(u64::MAX)
    );
}
#[test]
fn an_action_started_after_rest_observation_still_requires_the_session_barrier() {
    let coordinator = Coordinator::new(1).unwrap();
    let session = SessionId([1; 16]);
    coordinator.enroll(session).unwrap();
    assert!(coordinator.is_rest_at(CheckpointVersion(0)));
    // Start on another thread after the observation, before admission.
    let action = std::thread::scope(|scope| {
        scope
            .spawn(|| coordinator.start_action(Action::CheckpointLog).unwrap())
            .join()
            .unwrap()
    });
    coordinator
        .accept_serial(session, Serial(7), CheckpointVersion(0))
        .unwrap();
    assert!(matches!(
        coordinator.advance(action, Phase::Prepare),
        Err(Error::Busy)
    ));
    let cut = SessionCut {
        session,
        last_accepted: Some(Serial(7)),
        old_pending: 0,
    };
    coordinator
        .acknowledge(action, cut, Phase::Prepare)
        .unwrap();
    coordinator.advance(action, Phase::Prepare).unwrap();
    assert!(!coordinator.is_rest_at(CheckpointVersion(0)));
    assert!(matches!(
        coordinator.accept_serial(session, Serial(9), CheckpointVersion(0)),
        Err(Error::Busy)
    ));
    assert_eq!(coordinator.last_accepted(session).unwrap(), Some(Serial(7)));
    coordinator
        .accept_serial(session, Serial(9), CheckpointVersion(1))
        .unwrap();
    for phase in [Phase::InProgress, Phase::WaitPending] {
        coordinator.acknowledge(action, cut, phase).unwrap();
        coordinator.advance(action, phase).unwrap();
    }
    assert_eq!(coordinator.cuts(action).unwrap(), vec![cut]);
    coordinator.advance(action, Phase::WaitFlush).unwrap();
    coordinator.finish_action(action).unwrap();
    assert!(coordinator.is_rest_at(CheckpointVersion(1)));
}
