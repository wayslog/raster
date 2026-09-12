//! Conditional replication uses real index,Logs and equipment;Test pause points only control interleaving,Does not replace publishing agreement.
use super::*;
use crate::{
    api::operation::RmwOperation,
    coordination::{Action, Phase},
    engine::{
        Engine,
        conditional_copy::{ConditionalCopy, CopyResult},
    },
    schema::{KeyCodec, ValueRead},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn budget() -> PollBudget {
    PollBudget(std::num::NonZeroUsize::new(1).unwrap())
}
fn start<S: crate::schema::Schema>(engine: &Engine<S>) -> MaintenanceId {
    engine.coordinator.start_action(Action::Compact).unwrap()
}
fn finish<S: crate::schema::Schema>(engine: &Engine<S>, id: MaintenanceId) {
    engine.coordinator.advance(id, Phase::Compacting).unwrap();
    engine.coordinator.finish_action(id).unwrap();
}
fn source<S: crate::schema::Schema<Key = U64Key>>(engine: &Engine<S>, key: u64) -> LogAddress {
    engine
        .resolve_index(U64Key.hash(&key), &key.to_le_bytes())
        .unwrap()
        .head
        .unwrap()
}
fn drive<S: crate::schema::Schema>(engine: &Engine<S>, task: &mut ConditionalCopy) -> CopyResult {
    let end = deadline();
    loop {
        assert!(
            !end.expired(),
            "Conditional replication did not end within the deadline"
        );
        match engine.conditional_copy(task, budget()).unwrap() {
            CopyResult::Retry => {
                engine.poll_maintenance(budget()).unwrap();
                std::thread::yield_now();
            }
            result => return result,
        }
    }
}
#[derive(Debug)]
struct Add(u64, u64);
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        panic!("source must exist")
    }
    fn copy_update(&mut self, old: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        let new = old.view().wrapping_add(self.1);
        Ok((new, new))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Updated(
            value
                .view_mut()
                .fetch_add(self.1, Ordering::SeqCst)
                .wrapping_add(self.1),
        ))
    }
}
#[test]
fn in_place_update_after_candidate_capture_keeps_the_source_address_but_copies_the_current_value_and_only_ends_once()
 {
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let old = source(&store.inner, 7);
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 7u64.to_le_bytes().to_vec())
        .unwrap();
    session
        .rmw(Serial(1), Add(7, 5), Default::default())
        .unwrap();
    assert_eq!(source(&store.inner, 7), old);
    let before = store.inner.log.frontiers().unwrap();
    let CopyResult::Copied(new) = drive(&store.inner, &mut task) else {
        panic!("Live source should be copied")
    };
    assert!(new > old);
    assert_eq!(task.published_address(), Some(new));
    assert_eq!(store.inner.log.frontiers().unwrap().begin, before.begin);
    assert!(matches!(
        store.inner.conditional_copy(&mut task, budget()),
        Err(Error::InvalidState(_))
    ));
    assert!(task.drain(&store.inner.storage).unwrap());
    let stats = store.statistics().conditional_copies;
    assert_eq!((stats.accepted, stats.completed, stats.success), (1, 1, 1));
    assert_eq!(stats.io_per_request[0], 1);
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 2, 7), Some(12));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn appending_invalidates_the_old_source_and_deletes_it_in_place_and_only_copies_the_current_tombstone()
 {
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 9);
    let old = source(&store.inner, 9);
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 9u64.to_le_bytes().to_vec())
        .unwrap();
    put(&mut session, 1, 9);
    let tail = store.inner.log.frontiers().unwrap().tail;
    assert_eq!(drive(&store.inner, &mut task), CopyResult::Obsolete);
    assert_eq!(store.statistics().conditional_copies.not_found, 1);
    assert_eq!(store.inner.log.frontiers().unwrap().tail, tail);
    let current = source(&store.inner, 9);
    let mut task = store
        .inner
        .new_conditional_copy(id, current, 9u64.to_le_bytes().to_vec())
        .unwrap();
    session
        .delete(Serial(2), Delete(9), Default::default())
        .unwrap();
    let CopyResult::Copied(deleted) = drive(&store.inner, &mut task) else {
        panic!(
            "Delete in place and keep the source address,Copying must obtain the current tombstone"
        );
    };
    assert!(store.inner.log.lease(deleted).unwrap().is_tombstone());
    let tombstone = source(&store.inner, 9);
    let mut task = store
        .inner
        .new_conditional_copy(id, tombstone, 9u64.to_le_bytes().to_vec())
        .unwrap();
    let CopyResult::Copied(new) = drive(&store.inner, &mut task) else {
        panic!("Tombstones should be copied")
    };
    assert!(store.inner.log.lease(new).unwrap().is_tombstone());
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 3, 9), None);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn different_complete_keys_in_the_same_bucket_and_same_label_can_copy_the_source_within_the_chain_and_retain_the_other_key()
 {
    let mut config = Config::default();
    config.index.buckets = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 8969);
    let old = source(&store.inner, 8969);
    put(&mut session, 1, 9239);
    assert_ne!(U64Key.hash(&8969).0 % 64, U64Key.hash(&9239).0 % 64);
    assert_eq!(source(&store.inner, 8969), source(&store.inner, 9239));
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 8969u64.to_le_bytes().to_vec())
        .unwrap();
    assert!(matches!(
        drive(&store.inner, &mut task),
        CopyResult::Copied(_)
    ));
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 2, 8969), Some(8969));
    assert_eq!(read_value(&mut session, 3, 9239), Some(9239));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn append_can_be_completed_while_cold_source_read_is_pending_but_the_outdated_source_cannot_be_republished()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 0);
    let old = source(&store.inner, 0);
    for key in 1..400 {
        put(&mut session, key, key);
    }
    assert!(old < store.inner.log.frontiers().unwrap().head);
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 0u64.to_le_bytes().to_vec())
        .unwrap();
    while !task.has_inflight() {
        assert_eq!(
            store.inner.conditional_copy(&mut task, budget()).unwrap(),
            CopyResult::Retry
        );
    }
    // Search index upsert No need to read old values;Pending replication does not hold business or source record permissions.
    put(&mut session, 400, 0);
    let latest = source(&store.inner, 0);
    assert_eq!(drive(&store.inner, &mut task), CopyResult::Obsolete);
    assert_eq!(source(&store.inner, 0), latest);
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 401, 0), Some(0));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn the_foreign_instance_refuses_replication_while_the_owning_instance_can_still_complete() {
    let (_one, first) = setup(None);
    let (_two, second) = setup(None);
    let mut session = first.start_session(Default::default()).unwrap();
    put(&mut session, 0, 1);
    let id = start(&first.inner);
    let mut task = first
        .inner
        .new_conditional_copy(id, source(&first.inner, 1), 1u64.to_le_bytes().to_vec())
        .unwrap();
    assert!(matches!(
        second.inner.conditional_copy(&mut task, budget()),
        Err(Error::InvalidState(_))
    ));
    assert!(!second.inner.failed.load(Ordering::SeqCst));
    assert!(matches!(
        drive(&first.inner, &mut task),
        CopyResult::Copied(_)
    ));
    finish(&first.inner, id);
    session.close(deadline()).unwrap();
    first.shutdown(deadline()).unwrap();
    second.shutdown(deadline()).unwrap();
}

mod ordinary {
    use super::*;
    use crate::schema::{
        builtin::{SerializedValue, U64ValueCodec},
        value::ValueCodec,
    };
    use std::sync::{Mutex, mpsc};
    type Ordinary = SchemaPair<U64Key, SerializedValue<Codec>>;
    struct Fault {
        encode_countdown: AtomicUsize,
        encodes: AtomicUsize,
        mode: AtomicUsize,
        reached: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    struct Codec(Arc<Fault>);
    impl ValueCodec for Codec {
        type Value = u64;
        fn format_id(&self) -> FormatId {
            U64ValueCodec.format_id()
        }
        fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
            self.0.encodes.fetch_add(1, Ordering::SeqCst);
            if self
                .0
                .encode_countdown
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                == Ok(1)
            {
                match self.0.mode.load(Ordering::SeqCst) {
                    1 => {
                        return Err(Error::Codec(
                            "Injection target initialization encoding failed",
                        ));
                    }
                    2 => panic!("Inject target initialization panic"),
                    3 => return Err(Error::Busy),
                    _ => {
                        self.0.reached.send(()).unwrap();
                        self.0
                            .resume
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(10))
                            .unwrap();
                    }
                }
            }
            U64ValueCodec.encode(value)
        }
        fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
            U64ValueCodec.decode(bytes)
        }
    }
    #[derive(Debug)]
    struct Write(u64, u64);
    impl Keyed<Ordinary> for Write {
        fn key(&self) -> &u64 {
            &self.0
        }
    }
    impl UpsertOperation<Ordinary> for Write {
        type Output = ();
        fn replacement(&mut self) -> Result<(u64, ()), Error> {
            Ok((self.1, ()))
        }
        fn update_in_place(
            &mut self,
            _: ValueUpdate<'_, Ordinary>,
        ) -> Result<UpdateDecision<()>, Error> {
            Ok(UpdateDecision::Append)
        }
    }
    fn setup() -> (
        RasterKV<Ordinary>,
        Arc<Fault>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let (reached, receiver) = mpsc::channel();
        let (sender, resume) = mpsc::channel();
        let fault = Arc::new(Fault {
            encode_countdown: 0.into(),
            encodes: 0.into(),
            mode: 0.into(),
            reached,
            resume: Mutex::new(resume),
        });
        let mut config = Config::default();
        config.index.buckets = 1;
        let store = RasterKV::builder(SchemaPair::new(
            U64Key,
            SerializedValue::new(Codec(fault.clone())),
        ))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
        (store, fault, receiver, sender)
    }
    fn write(session: &mut Session<Ordinary>, serial: u64, key: u64, value: u64) {
        let Submission::Ready(result) = session.upsert(Serial(serial), Write(key, value)).unwrap()
        else {
            panic!("Hot recording should be completed simultaneously")
        };
        result.unwrap();
    }
    fn value(store: &RasterKV<Ordinary>, key: u64) -> u64 {
        let mut at = Some(source(&store.inner, key));
        while let Some(address) = at {
            let lease = store.inner.log.lease(address).unwrap();
            if lease.key() == key.to_le_bytes() {
                return lease.read(|value| value).unwrap();
            }
            at = lease.previous();
        }
        panic!("key must exist")
    }
    #[test]
    fn the_source_license_overrides_the_target_initialization_and_collides_with_the_release_conflict_retry_after_cleaning_up_the_target_keep_both_keys()
     {
        let (store, fault, reached, resume) = setup();
        let mut session = store.start_session(Default::default()).unwrap();
        write(&mut session, 0, 8969, 11);
        let old = source(&store.inner, 8969);
        write(&mut session, 1, 9239, 22);
        let id = start(&store.inner);
        let mut task = store
            .inner
            .new_conditional_copy(id, old, 8969u64.to_le_bytes().to_vec())
            .unwrap();
        // The second encoding is target initialization,The allocation has occupied the slot but has not yet entered the address table..
        fault.encode_countdown.store(2, Ordering::SeqCst);
        let end_before = store.inner.log.frontiers().unwrap().tail;
        std::thread::scope(|scope| {
            let copying = scope.spawn(|| {
                loop {
                    let result = store.inner.conditional_copy(&mut task, budget()).unwrap();
                    if fault.encode_countdown.load(Ordering::SeqCst) == 0 {
                        return result;
                    }
                    assert_eq!(result, CopyResult::Retry);
                }
            });
            reached.recv_timeout(Duration::from_secs(10)).unwrap();
            let source_lease = store.inner.log.lease(old).unwrap();
            assert!(matches!(
                source_lease.update_if_mutable(|mut value| value.replace(&99)),
                Err(Error::Busy)
            ));
            assert!(matches!(
                store.inner.log.snapshot_next(old, end_before),
                Err(Error::Busy)
            ));
            drop(source_lease);
            write(&mut session, 2, 9239, 33);
            let collided_head = source(&store.inner, 9239);
            resume.send(()).unwrap();
            assert_eq!(copying.join().unwrap(), CopyResult::Retry);
            assert_eq!(source(&store.inner, 9239), collided_head);
        });
        // The failed target's address slot may have alignment padding,but does not appear in physical record scans.
        let mut count = 0;
        let end = store.inner.log.frontiers().unwrap().tail;
        let mut at = LogAddress(0);
        while let Some((address, bytes)) = store.inner.log.snapshot_next(at, end).unwrap() {
            count += 1;
            at = address
                .checked_add(
                    crate::format::Record::decode(&bytes)
                        .unwrap()
                        .header
                        .encoded_len()
                        .unwrap() as u64,
                )
                .unwrap();
        }
        assert_eq!(count, 3);
        assert!(matches!(
            drive(&store.inner, &mut task),
            CopyResult::Copied(_)
        ));
        assert_eq!((value(&store, 8969), value(&store, 9239)), (11, 33));
        finish(&store.inner, id);
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
    #[test]
    fn target_initialization_failed_retaining_source_and_index_and_encoding_cannot_be_performed_again()
     {
        for mode in [1, 3] {
            let (store, fault, _, _) = setup();
            let mut session = store.start_session(Default::default()).unwrap();
            write(&mut session, 0, 7, 42);
            let old = source(&store.inner, 7);
            let id = start(&store.inner);
            let mut task = store
                .inner
                .new_conditional_copy(id, old, 7u64.to_le_bytes().to_vec())
                .unwrap();
            fault.mode.store(mode, Ordering::SeqCst);
            fault.encode_countdown.store(2, Ordering::SeqCst);
            assert!(matches!(
                store.inner.conditional_copy(&mut task, budget()),
                Err(Error::Codec(_) | Error::Busy)
            ));
            let calls = fault.encodes.load(Ordering::SeqCst);
            assert!(matches!(
                store.inner.conditional_copy(&mut task, budget()),
                Err(Error::InvalidState(_))
            ));
            assert_eq!(fault.encodes.load(Ordering::SeqCst), calls);
            assert_eq!(source(&store.inner, 7), old);
            assert_eq!(value(&store, 7), 42);
            assert!(task.drain(&store.inner.storage).unwrap());
            assert!(!store.inner.failed.load(Ordering::SeqCst));
            finish(&store.inner, id);
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
        }
    }

    #[test]
    fn target_initialization_panic_failed_to_close_and_no_target_was_released() {
        let (store, fault, _, _) = setup();
        let mut session = store.start_session(Default::default()).unwrap();
        write(&mut session, 0, 7, 42);
        let old = source(&store.inner, 7);
        let id = start(&store.inner);
        let mut task = store
            .inner
            .new_conditional_copy(id, old, 7u64.to_le_bytes().to_vec())
            .unwrap();
        fault.mode.store(2, Ordering::SeqCst);
        fault.encode_countdown.store(2, Ordering::SeqCst);
        assert!(matches!(
            store.inner.conditional_copy(&mut task, budget()),
            Err(Error::InvalidState(_))
        ));
        assert!(store.inner.failed.load(Ordering::SeqCst));
        assert_eq!(task.published_address(), None);
        assert_eq!(source(&store.inner, 7), old);
        assert!(task.drain(&store.inner.storage).unwrap());
        finish(&store.inner, id);
        let _ = session.close(deadline());
        let _ = store.shutdown(deadline());
    }
}

#[test]
fn full_replication_and_in_flight_draining_of_cold_sources_without_preserving_routes_or_blocking_subsequent_actions()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 0);
    let old = source(&store.inner, 0);
    for key in 1..400 {
        put(&mut session, key, key);
    }
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 0u64.to_le_bytes().to_vec())
        .unwrap();
    let CopyResult::Copied(new) = drive(&store.inner, &mut task) else {
        panic!("The cold source should be completely migrated")
    };
    assert!(new > old);
    assert_eq!(source(&store.inner, 0), new);
    let begin = store.inner.log.frontiers().unwrap().begin;
    assert_eq!(begin, LogAddress(0));
    let old_one = store
        .inner
        .resolve_index(U64Key.hash(&1), &1u64.to_le_bytes())
        .unwrap()
        .head
        .unwrap();
    let mut draining = store
        .inner
        .new_conditional_copy(id, old_one, 1u64.to_le_bytes().to_vec())
        .unwrap();
    while !draining.has_inflight() {
        assert_eq!(
            store
                .inner
                .conditional_copy(&mut draining, budget())
                .unwrap(),
            CopyResult::Retry
        );
    }
    assert!(!draining.drain(&store.inner.storage).unwrap());
    let end = deadline();
    while !draining.drain(&store.inner.storage).unwrap() {
        assert!(!end.expired());
        store
            .inner
            .io
            .poll(&*store.inner.storage.device, budget())
            .unwrap();
    }
    assert_eq!(draining.published_address(), None);
    assert!(matches!(
        store.inner.conditional_copy(&mut draining, budget()),
        Err(Error::InvalidState(_))
    ));
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    wait(&mut session, &checkpoint);
    // Older version license prevents copying;After release, the same request can continue,Compression itself does not increment the version.
    let id = start(&store.inner);
    let permit = store
        .inner
        .version_permits
        .reserve(U64Key.hash(&0), CheckpointVersion(0))
        .unwrap();
    let mut task = store
        .inner
        .new_conditional_copy(id, new, 0u64.to_le_bytes().to_vec())
        .unwrap();
    assert_eq!(
        store.inner.conditional_copy(&mut task, budget()).unwrap(),
        CopyResult::Retry
    );
    drop(permit);
    assert!(matches!(
        drive(&store.inner, &mut task),
        CopyResult::Copied(_)
    ));
    finish(&store.inner, id);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
