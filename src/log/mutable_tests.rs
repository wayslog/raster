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
fn mutable_session_update_does_not_wait_for_unchanged_control_metadata() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session
            .upsert(Serial(0), Request(7))
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(7)))
    ));
    session.close(deadline()).unwrap();
    let result = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (start_tx, start_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let writer = scope.spawn(move || {
            let mut session = owner.start_session(Default::default()).unwrap();
            ready_tx.send(()).unwrap();
            start_rx.recv().unwrap();
            let result = match session.upsert(Serial(1), Request(11)) {
                Ok(Submission::Ready(Ok(Outcome::Success(value)))) => Ok(value),
                _ => Err("mutable update did not complete synchronously"),
            };
            result_tx.send(result).unwrap();
            session.close(deadline()).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let control = store.inner.log.state.write().unwrap();
        start_tx.send(()).unwrap();
        let result = result_rx.recv_timeout(Duration::from_secs(2));
        // Release the deliberate stall before joining, even for the old slow path.
        drop(control);
        writer.join().unwrap();
        result
    });
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session
            .read(Serial(0), Request(0), ReadOptions::default())
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(11)))
    ));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    assert_eq!(
        result,
        Ok(Ok(11)),
        "an in-place update must not wait for unrelated control bookkeeping"
    );
}

fn config() -> LogConfig {
    LogConfig {
        page_bytes: 256,
        memory_pages: 4,
        mutable_fraction: 0.5,
    }
}
fn log() -> HybridLog<AtomicU64Value> {
    HybridLog::new(config(), Arc::new(AtomicU64Value)).unwrap()
}
fn insert(log: &HybridLog<AtomicU64Value>, key: &[u8]) -> LogAddress {
    log.finish_initialization(log.reserve_record(key, None, 7).unwrap())
        .unwrap()
}

#[test]
fn rejected_boundaries_preserve_mutability_and_intra_page_begin_excludes_old_records() {
    let log = log();
    let first = insert(&log, b"a");
    let second = insert(&log, b"b");
    assert!(log.find_mutable(b"a", Some(first)).unwrap().is_some());
    let reservation = log.reserve_record(b"c", None, 7).unwrap();
    assert!(matches!(log.publish_begin(second), Err(Error::Busy)));
    assert!(log.find_mutable(b"a", Some(first)).unwrap().is_some());
    drop(reservation);
    assert!(log.publish_begin(LogAddress(1024)).is_err());
    assert!(log.advance_read_only(LogAddress(1024)).is_err());
    assert!(log.advance_read_only(LogAddress(1)).is_err());
    assert!(log.find_mutable(b"a", Some(first)).unwrap().is_some());
    log.publish_begin(second).unwrap();
    assert!(log.find_mutable(b"a", Some(first)).unwrap().is_none());
    assert!(log.find_mutable(b"b", Some(second)).unwrap().is_some());
    assert!(log.publish_begin(first).is_err());
    assert!(log.find_mutable(b"b", Some(second)).unwrap().is_some());
    log.discard_prefix().unwrap();
    assert!(log.find_mutable(b"a", Some(first)).unwrap().is_none());
    assert!(log.find_mutable(b"b", Some(second)).unwrap().is_some());
}

#[test]
fn a_contended_freeze_keeps_its_target_restriction_before_safe_progress() {
    let log = log();
    let address = insert(&log, b"key");
    let end = log.pad_tail().unwrap();
    std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = &log;
        let writer = scope.spawn(move || {
            let lease = owner.lease(address).unwrap();
            lease
                .update(|_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let freeze = log.advance_read_only(end);
        let bounds = log.frontiers().unwrap();
        let excluded = log.find_mutable(b"key", Some(address)).unwrap().is_none();
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        assert!(matches!(freeze, Err(Error::Busy)));
        assert_eq!(bounds.read_only, end);
        assert_eq!(bounds.safe_read_only, LogAddress(0));
        assert!(
            excluded,
            "a failed freeze must still block new in-place updates"
        );
    });
    log.advance_read_only(end).unwrap();
    assert_eq!(log.frontiers().unwrap().safe_read_only, end);
    assert!(log.find_mutable(b"key", Some(address)).unwrap().is_none());
}

#[test]
fn recovered_boundaries_reject_cold_history_and_freezing_excludes_new_resident_records() {
    let log = HybridLog::from_checkpoint(
        config(),
        Arc::new(AtomicU64Value),
        LogAddress(256),
        LogAddress(512),
    )
    .unwrap();
    assert!(
        log.find_mutable(b"old", Some(LogAddress(256)))
            .unwrap()
            .is_none()
    );
    let address = insert(&log, b"new");
    assert_eq!(address, LogAddress(512));
    assert!(log.find_mutable(b"new", Some(address)).unwrap().is_some());
    let end = log.pad_tail().unwrap();
    log.advance_read_only(end).unwrap();
    assert!(log.find_mutable(b"new", Some(address)).unwrap().is_none());
    log.publish_begin(end).unwrap();
    log.discard_prefix().unwrap();
    while log.frontiers().unwrap().head < end {
        assert_eq!(log.evict_next().unwrap().completed, 1);
    }
    let next = insert(&log, b"after");
    assert_eq!(next, end);
    assert!(log.find_mutable(b"after", Some(next)).unwrap().is_some());
}
