//! Verify delete contract from public session and real file device,No replacement of internal engine modules.
#![cfg(any(target_os = "linux", target_os = "macos"))]
use raster::{
    RasterKV, Session, Submission,
    api::{completion::Outcome, operation::*},
    config::Config,
    device::{thread_pool::ThreadPoolDeviceFactory, *},
    schema::{
        KeyCodec, ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
type Schema<K = U64Key> = SchemaPair<K, AtomicU64Value>;

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct CountingFactory(Arc<AtomicUsize>);
struct CountingDevice {
    inner: Box<dyn Device>,
    reads: Arc<AtomicUsize>,
}
impl DeviceFactory for CountingFactory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(CountingDevice {
            inner: ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 128,
            }
            .open(options)?,
            reads: self.0.clone(),
        }))
    }
}
impl Device for CountingDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let read = matches!(request.operation, IoOperation::Read { .. });
        let result = self.inner.submit(request);
        if result.is_ok() && read {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        self.inner.poll(budget, output)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}
#[derive(Debug)]
struct Request(u64);
impl<K: KeyCodec<Key = u64>> Keyed<Schema<K>> for Request {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl<K: KeyCodec<Key = u64>> UpsertOperation<Schema<K>> for Request {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.0, self.0))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema<K>>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}
impl<K: KeyCodec<Key = u64>> ReadOperation<Schema<K>> for Request {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema<K>>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
impl<K: KeyCodec<Key = u64>> DeleteOperation<Schema<K>> for Request {
    type Output = DeleteOutcome;
    fn complete(self, outcome: DeleteOutcome) -> DeleteOutcome {
        outcome
    }
}
impl<K: KeyCodec<Key = u64>> RmwOperation<Schema<K>> for Request {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        panic!("This use case prohibits the creation of")
    }
    fn copy_update(&mut self, _: ValueRead<'_, Schema<K>>) -> Result<(u64, u64), Error> {
        panic!("There is no old value in this use case")
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema<K>>,
    ) -> Result<UpdateDecision<u64>, Error> {
        panic!("There is no old value in this use case")
    }
}
#[test]
fn missing_read_write_creation_is_prohibited_and_upstream_empty_index_slots_are_retained_for_subsequent_blind_deletion()
 {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let result = session
        .rmw(
            Serial(0),
            Request(7),
            RmwOptions {
                create_if_missing: false,
            },
        )
        .unwrap();
    assert!(matches!(take(&mut session, result), Outcome::NotFound));
    let result = session
        .read(
            Serial(1),
            Request(7),
            ReadOptions {
                abort_if_tombstone: true,
            },
        )
        .unwrap();
    assert!(matches!(take(&mut session, result), Outcome::NotFound));
    let result = session
        .delete(Serial(2), Request(7), Default::default())
        .unwrap();
    assert!(matches!(
        take(&mut session, result),
        Outcome::Success(DeleteOutcome::TombstoneWritten)
    ));
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(30))
}
fn take<K: KeyCodec<Key = u64>, T: 'static>(
    session: &mut Session<Schema<K>>,
    submission: Submission<T>,
) -> Outcome<T> {
    match submission {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap(),
    }
    .unwrap()
}

#[test]
fn ordinary_variable_link_head_deletion_releases_the_index_slot_while_forced_deletion_retains_the_tombstone()
 {
    use raster::api::completion::AbortReason;
    for force_tombstone in [false, true] {
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .device(Box::new(null::NullDeviceFactory))
            .create()
            .unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        let result = session.upsert(Serial(0), Request(7)).unwrap();
        assert!(matches!(take(&mut session, result), Outcome::Success(7)));
        let before = store.diagnostics().unwrap();
        assert_eq!(before.bucket_distribution.iter().sum::<u64>(), 1);
        let result = session
            .delete(Serial(1), Request(7), DeleteOptions { force_tombstone })
            .unwrap();
        match (force_tombstone, take(&mut session, result)) {
            (false, Outcome::Success(DeleteOutcome::IndexRemoved))
            | (true, Outcome::Success(DeleteOutcome::TombstoneWritten)) => {}
            other => panic!("Delete result error {other:?}"),
        }
        let after = store.diagnostics().unwrap();
        assert_eq!(
            after.tail, before.tail,
            "Variable record deletion should be done in-place"
        );
        assert_eq!(
            after.bucket_distribution.iter().sum::<u64>(),
            u64::from(force_tombstone)
        );
        let result = session
            .read(
                Serial(2),
                Request(7),
                ReadOptions {
                    abort_if_tombstone: true,
                },
            )
            .unwrap();
        match (force_tombstone, take(&mut session, result)) {
            (false, Outcome::NotFound) | (true, Outcome::Aborted(AbortReason::Tombstone)) => {}
            other => panic!("Tombstone reachability error {other:?}"),
        }
        let result = session
            .delete(Serial(3), Request(7), DeleteOptions { force_tombstone })
            .unwrap();
        match (force_tombstone, take(&mut session, result)) {
            (false, Outcome::NotFound)
            | (true, Outcome::Success(DeleteOutcome::TombstoneWritten)) => {}
            other => panic!("Delete result error again {other:?}"),
        }
        session.close(deadline()).unwrap();
        drop(session);
        store.shutdown(deadline()).unwrap();
    }
}

struct CollisionKey;
impl KeyCodec for CollisionKey {
    type Key = u64;
    type OwnedKey = u64;
    fn format_id(&self) -> FormatId {
        FormatId([91; 16])
    }
    fn hash_descriptor(&self) -> HashDescriptor {
        U64Key.hash_descriptor()
    }
    fn hash(&self, _: &u64) -> KeyHash {
        KeyHash(0)
    }
    fn encoded_len(&self, key: &u64) -> Result<u32, Error> {
        U64Key.encoded_len(key)
    }
    fn encode(&self, key: &u64, output: &mut [u8]) -> Result<(), Error> {
        U64Key.encode(key, output)
    }
    fn equals_encoded(&self, key: &u64, encoded: &[u8]) -> Result<bool, Error> {
        U64Key.equals_encoded(key, encoded)
    }
    fn decode_owned(&self, encoded: &[u8]) -> Result<u64, Error> {
        U64Key.decode_owned(encoded)
    }
}
#[test]
fn deleting_records_in_the_same_tag_chain_and_non_existing_collision_keys_will_not_lose_other_keys()
{
    use raster::api::completion::AbortReason;
    let store = RasterKV::builder(SchemaPair::new(CollisionKey, AtomicU64Value))
        .device(Box::new(null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..3 {
        let result = session.upsert(Serial(key), Request(key)).unwrap();
        assert!(matches!(take(&mut session, result), Outcome::Success(n) if n == key));
    }
    // Chain keys and chain heads with valid predecessors must be retained;Same tag Blind deletion is accepted even if there is no matching key.
    for (serial, key) in [(3, 1), (4, 2), (5, 99), (6, 1)] {
        let result = session
            .delete(Serial(serial), Request(key), Default::default())
            .unwrap();
        assert!(matches!(
            take(&mut session, result),
            Outcome::Success(DeleteOutcome::TombstoneWritten)
        ));
    }
    assert_eq!(
        store
            .diagnostics()
            .unwrap()
            .bucket_distribution
            .iter()
            .sum::<u64>(),
        1
    );
    let result = session
        .read(Serial(7), Request(0), Default::default())
        .unwrap();
    assert!(matches!(take(&mut session, result), Outcome::Success(0)));
    for (serial, key) in [(8, 1), (9, 2), (10, 99)] {
        let result = session
            .read(
                Serial(serial),
                Request(key),
                ReadOptions {
                    abort_if_tombstone: true,
                },
            )
            .unwrap();
        assert!(matches!(
            take(&mut session, result),
            Outcome::Aborted(AbortReason::Tombstone)
        ));
    }
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
}

fn builder(config: Config) -> raster::Builder<Schema> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 128,
        }))
}
#[test]
fn delete_the_old_checkpoint_without_modifying_it_and_force_the_tombstone_to_still_be_reachable_after_recovery()
 {
    use raster::api::{
        completion::AbortReason,
        maintenance::{CheckpointKind, RecoverySet},
    };
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-delete-recover-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    let store = builder(config.clone()).create().unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let result = session.upsert(Serial(0), Request(7)).unwrap();
    assert!(matches!(take(&mut session, result), Outcome::Success(7)));
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    let token = report.as_ref().as_ref().unwrap().token;
    let old = RecoverySet {
        store: store.id(),
        index: token,
        log: token,
    };
    let before = store.diagnostics().unwrap().tail;
    let result = session
        .delete(
            Serial(1),
            Request(7),
            DeleteOptions {
                force_tombstone: true,
            },
        )
        .unwrap();
    assert!(matches!(
        take(&mut session, result),
        Outcome::Success(DeleteOutcome::TombstoneWritten)
    ));
    assert!(
        store.diagnostics().unwrap().tail > before,
        "Old version records must be deleted by appending"
    );
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    let token = report.as_ref().as_ref().unwrap().token;
    let new = RecoverySet {
        store: store.id(),
        index: token,
        log: token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    for (set, deleted) in [(old, false), (new, true)] {
        let (store, _) = builder(config.clone()).recover(set).unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        let result = session
            .read(
                Serial(0),
                Request(7),
                ReadOptions {
                    abort_if_tombstone: true,
                },
            )
            .unwrap();
        match (deleted, take(&mut session, result)) {
            (false, Outcome::Success(7)) | (true, Outcome::Aborted(AbortReason::Tombstone)) => {}
            other => panic!("Undelete version error {other:?}"),
        }
        session.close(deadline()).unwrap();
        drop(session);
        store.shutdown(deadline()).unwrap();
    }
}

#[test]
fn ordinary_delete_cold_keys_do_not_read_old_records_and_can_still_obscure_disk_values() {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-delete-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let reads = Arc::new(AtomicUsize::new(0));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 16384;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(CountingFactory(reads.clone())))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..800 {
        let result = session.upsert(Serial(key), Request(key)).unwrap();
        assert!(matches!(take(&mut session, result), Outcome::Success(n) if n == key));
    }
    // Use reality first Read Proves that the target has left memory;Read caching is not enabled by default.
    let before = reads.load(Ordering::SeqCst);
    let result = session
        .read(Serial(800), Request(0), Default::default())
        .unwrap();
    assert!(matches!(result, Submission::Pending(_)));
    assert!(matches!(take(&mut session, result), Outcome::Success(0)));
    assert!(reads.load(Ordering::SeqCst) > before);
    let before = reads.load(Ordering::SeqCst);
    let result = session
        .delete(Serial(801), Request(0), Default::default())
        .unwrap();
    assert!(matches!(
        take(&mut session, result),
        Outcome::Success(DeleteOutcome::TombstoneWritten)
    ));
    assert_eq!(
        reads.load(Ordering::SeqCst),
        before,
        "Delete cannot read old records for existence judgment"
    );
    let result = session
        .read(Serial(802), Request(0), Default::default())
        .unwrap();
    assert!(matches!(take(&mut session, result), Outcome::NotFound));
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
}
