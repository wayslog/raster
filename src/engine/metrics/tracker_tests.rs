use super::*;
use std::{sync::mpsc, time::Duration};

#[test]
fn independent_trackers_preserve_pending_sampling_and_terminal_effects() {
    let metrics = Arc::new(Metrics::new(false));
    let first = metrics.register().unwrap();
    let second = metrics.register().unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    let first_lifetime = Arc::downgrade(&first);
    let mut read = first.accept(Kind::Read);
    read.pending();
    read.pending();
    drop(first);
    metrics.enable(true);
    let mut write = second.accept(Kind::Upsert);
    let mut copy = metrics.accept(Kind::Copy);
    copy.pending();
    assert_eq!(metrics.activity().unwrap(), (2, 1));
    assert!(first_lifetime.upgrade().is_some());
    metrics.enable(false);
    read.finish(Completed::Success, Some(0));
    write.finish(Completed::Failed(Effect::Applied), Some(2));
    write.finish(Completed::Success, Some(0));
    assert_eq!(metrics.activity().unwrap(), (0, 0));
    drop((read, write, copy, second));
    assert!(first_lifetime.upgrade().is_none());
    let stats = metrics.snapshot();
    assert_eq!(stats.reads.accepted, 0);
    assert_eq!(stats.reads.completed, 0);
    assert_eq!(stats.upserts.accepted, 1);
    assert_eq!(stats.upserts.completed, 1);
    assert_eq!(stats.upserts.failed_after_applied, 1);
    assert_eq!(stats.upserts.io_per_request[2], 1);
    assert_eq!(stats.conditional_copies.failed_with_unknown_effect, 1);
    assert!(!stats.measurements_complete);
}

#[test]
fn a_live_monitor_keeps_its_registration_observable_and_expired_slots_are_reused() {
    let metrics = Metrics::new(false);
    let core = Arc::downgrade(&metrics.core);
    for _ in 0..2048 {
        let tracker = metrics.register().unwrap();
        let monitor = tracker.accept(Kind::Read);
        drop(tracker);
        assert_eq!(metrics.activity().unwrap(), (1, 0));
        drop(monitor);
        assert_eq!(metrics.activity().unwrap(), (0, 0));
        assert_eq!(metrics.trackers.lock().unwrap().len(), 1);
        assert_eq!(Arc::strong_count(&metrics.core), 2);
    }
    drop(metrics);
    assert!(core.upgrade().is_none());
}

#[test]
fn accepting_and_finishing_requests_does_not_wait_for_the_registration_table() {
    let metrics = Metrics::new(false);
    let tracker = metrics.register().unwrap();
    let (start_tx, start_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let result = std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            start_rx.recv().unwrap();
            let mut monitor = tracker.accept(Kind::Rmw);
            monitor.pending();
            monitor.finish(Completed::Success, Some(0));
            drop(monitor);
            done_tx.send(()).unwrap();
        });
        let registry = metrics.trackers.lock().unwrap();
        start_tx.send(()).unwrap();
        let result = done_rx.recv_timeout(Duration::from_secs(2));
        drop(registry);
        if result.is_err() {
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        worker.join().unwrap();
        result
    });
    assert_eq!(metrics.activity().unwrap(), (0, 0));
    assert_eq!(result, Ok(()));
}
