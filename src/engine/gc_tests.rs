//! Expose logical truncation and work segment deletion:independent checkpoint,Retries and memory loops use real engines.
use super::*;
use crate::api::maintenance::{
    CompactionAlgorithm, CompactionOptions, GcReport, PhysicalReclamation,
};
use crate::schema::KeyCodec;
fn gc(store: &RasterKV<Schema>, session: &mut Session<Schema>, begin: LogAddress) -> GcReport {
    let ticket = store.maintenance().shift_begin(begin).unwrap();
    let result = session.wait_maintenance(&ticket, deadline()).unwrap();
    result.as_ref().as_ref().unwrap().clone()
}
fn compact(store: &RasterKV<Schema>, session: &mut Session<Schema>, until: LogAddress) {
    let ticket = store
        .maintenance()
        .compact(CompactionOptions {
            algorithm: CompactionAlgorithm::Lookup,
            until,
            workers: 1,
            shift_begin: false,
            checkpoint: false,
        })
        .unwrap();
    session
        .wait_maintenance(&ticket, deadline())
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
}
#[test]
fn after_compression_non_page_aligned_truncation_deletes_old_segments_and_both_generations_of_checkpoints_are_recoverable()
 {
    let root = Directory(
        std::env::temp_dir().join(format!("raster-gc-{:x?}", StoreId::generate().unwrap().0)),
    );
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 8192;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.index.buckets = 16;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config.clone())
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    match session
        .delete(Serial(400), Delete(6), Default::default())
        .unwrap()
    {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
    let old_ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let old = wait(&mut session, &old_ticket);
    // The end address of the page is taken from the end of a real record;Subsequently added keys are independent of the old range that needs to be relocated.
    put(&mut session, 401, 400);
    let begin = store.inner.log.frontiers().unwrap().tail;
    assert_ne!(begin.0 % 4096, 0);
    compact(&store, &mut session, begin);
    let report = gc(&store, &mut session, begin);
    assert_eq!(report.begin, begin);
    assert!(report.index_cleaned);
    assert!(report.deleted_segments > 0);
    assert!(matches!(report.physical, PhysicalReclamation::Completed));
    assert!(
        !root
            .0
            .join("segments/0000000000000000-0000000000000000.log")
            .exists()
    );
    assert!(matches!(
        store.maintenance().checkpoint(CheckpointKind::Log),
        Err(Error::InvalidState(_))
    ));
    assert_eq!(gc(&store, &mut session, begin).deleted_segments, 0);
    for key in 0..401 {
        assert_eq!(
            read_value(&mut session, 402 + key, key),
            if key == 6 { None } else { Some(key) }
        );
    }
    let current_ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let current = wait(&mut session, &current_ticket);
    assert_eq!(current.begin, begin);
    let sets = [old, current].map(|report| crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    });
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    drop(store);
    for (generation, set) in sets.into_iter().enumerate() {
        let (store, _) = recover_store(config.clone(), set).unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        for key in 0..401 {
            assert_eq!(
                read_value(&mut session, key, key),
                if key == 6 || generation == 0 && key == 400 {
                    None
                } else {
                    Some(key)
                }
            );
        }
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}
#[test]
fn memory_truncation_clears_old_keys_to_release_the_entire_page_and_can_continue_to_write_in_a_loop()
 {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let mut serial = 0;
    for cycle in 0..6 {
        for key in cycle * 40..(cycle + 1) * 40 {
            put(&mut session, serial, key);
            serial += 1;
        }
        let until = store.inner.log.frontiers().unwrap().tail;
        let report = gc(&store, &mut session, until);
        assert!(matches!(report.physical, PhysicalReclamation::Completed));
        assert_eq!(report.deleted_segments, 0);
        for key in cycle * 40..(cycle + 1) * 40 {
            assert_eq!(read_value(&mut session, serial, key), None);
            serial += 1;
        }
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn truncation_boundary_errors_have_no_side_effects_and_cleanup_covers_all_new_buckets_after_expansion()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let source = store
        .inner
        .resolve_index(U64Key.hash(&7), &7u64.to_le_bytes())
        .unwrap()
        .head
        .unwrap();
    assert!(matches!(
        store
            .maintenance()
            .shift_begin(source.checked_add(1).unwrap()),
        Err(Error::InvalidFormat(_))
    ));
    let until = store.inner.log.frontiers().unwrap().tail;
    assert!(
        store
            .maintenance()
            .shift_begin(until.checked_add(1).unwrap())
            .is_err()
    );
    assert_eq!(store.inner.log.frontiers().unwrap().begin, LogAddress(0));
    let growth = store.maintenance().grow_index().unwrap();
    session
        .wait_maintenance(&growth, deadline())
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
    assert_eq!(
        store.inner.index.bucket_count().unwrap(),
        store.inner.config.index.buckets * 2
    );
    gc(&store, &mut session, until);
    assert!(store.inner.index.snapshot().unwrap().entries.is_empty());
    assert_eq!(read_value(&mut session, 1, 7), None);
    assert!(store.maintenance().shift_begin(LogAddress(0)).is_err());
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

fn native_gc() -> (Directory, RasterKV<Schema>) {
    native_gc_with_device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 16,
    }))
}

fn native_gc_with_device(factory: Box<dyn DeviceFactory>) -> (Directory, RasterKV<Schema>) {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-gc-native-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 4096;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.index.buckets = 16;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(factory)
        .create()
        .unwrap();
    (root, store)
}

struct DelayedCompletionFactory(std::sync::Arc<std::sync::atomic::AtomicBool>);
struct DelayedCompletions {
    inner: Box<dyn Device>,
    held: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl DeviceFactory for DelayedCompletionFactory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(DelayedCompletions {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }
            .open(options)?,
            held: self.0.clone(),
        }))
    }
}
impl Device for DelayedCompletions {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        self.inner.submit(request)
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        use std::sync::atomic::Ordering;
        // Native devices still actually perform I/O;Not available yet,Simulating async results are not yet visible.
        if self.held.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.inner.poll(budget, output)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}

#[test]
fn public_truncation_invalidates_old_scans_and_pending_reads_of_protected_segments_can_be_deferred_before_recycling()
 {
    use crate::api::scan::{Buffering, ScanOptions};
    use crate::api::{completion::TicketState, maintenance::PhysicalReclamation};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let hold_completions = Arc::new(AtomicBool::new(false));
    let (_root, store) =
        native_gc_with_device(Box::new(DelayedCompletionFactory(hold_completions.clone())));
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let mut scanner = store
        .scan(ScanOptions {
            begin: LogAddress(0),
            end: store.inner.log.frontiers().unwrap().tail,
            buffering: Buffering::DoublePage,
        })
        .unwrap();
    assert_eq!(scanner.next_record().unwrap().unwrap().key, 0);
    let Submission::Pending(mut read) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("Reading needs to be suspended")
    };
    // Fixed presence of real background writes,GC The task must be cleared;Can't just delay with GC irrelevant completion.
    let tail = store.inner.log.pad_tail().unwrap();
    store.inner.log.advance_read_only(tail).unwrap();
    hold_completions.store(true, Ordering::SeqCst);
    store.inner.progress_storage().unwrap();
    assert!(store.inner.storage_progress.lock().unwrap().has_flush());
    let begin = LogAddress(3 * 4096);
    let ticket = store.maintenance().shift_begin(begin).unwrap();
    let budget = PollBudget(std::num::NonZeroUsize::new(1).unwrap());
    // Observe the real thing first I/O wait,Explicitly release the gate again;Don't create scheduling load with lots of empty polls.
    store.maintenance().poll(budget).unwrap();
    assert!(ticket.try_report().unwrap().is_none());
    assert!(store.inner.storage_progress.lock().unwrap().has_flush());
    hold_completions.store(false, Ordering::SeqCst);
    let end = deadline();
    let report = loop {
        if let Some(report) = ticket.try_report().unwrap() {
            break report;
        }
        assert!(
            !end.expired(),
            "GC Should be terminated within the existing deadline"
        );
        store.maintenance().poll(budget).unwrap();
        std::thread::yield_now();
    };
    let report = report.as_ref().as_ref().unwrap();
    assert_eq!(report.begin, begin);
    assert!(report.index_cleaned);
    assert!(matches!(
        report.physical,
        PhysicalReclamation::DeferredByRuntime { .. }
    ));
    assert!(matches!(read.try_take().unwrap(), TicketState::Pending));
    assert!(matches!(scanner.next_record(), Err(Error::RangeTruncated)));
    scanner.close().unwrap();
    assert!(matches!(
        session.wait(&mut read, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::NotFound
    ));
    let ticket = store.maintenance().shift_begin(begin).unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(matches!(
        report.as_ref().as_ref().unwrap().physical,
        PhysicalReclamation::Completed
    ));
    assert!(store.inner.storage.resolve(LogAddress(0)).is_err());
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

mod failure {
    use super::*;
    use std::{
        collections::BTreeSet,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };
    struct Control {
        mode: usize,
        armed: AtomicBool,
        file: Mutex<Option<FileId>>,
        path: Mutex<PathBuf>,
        injected: Mutex<BTreeSet<IoId>>,
        counts: Mutex<[usize; 3]>,
    }
    struct Factory(Arc<Control>);
    struct FaultDevice {
        inner: Box<dyn Device>,
        control: Arc<Control>,
    }
    impl DeviceFactory for Factory {
        fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
            Ok(Box::new(FaultDevice {
                inner: device::thread_pool::ThreadPoolDeviceFactory {
                    workers: 2,
                    queue_capacity: 16,
                }
                .open(options)?,
                control: self.0.clone(),
            }))
        }
    }
    impl Device for FaultDevice {
        fn capabilities(&self) -> DeviceCapabilities {
            self.inner.capabilities()
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            if self.control.mode == 3
                && self.control.armed.load(Ordering::SeqCst)
                && matches!(request.operation, IoOperation::Write { .. })
            {
                return Err(RejectedIo {
                    request,
                    reason: Error::InvalidState("Recycling cannot rewrite discarded pages"),
                });
            }
            let stage = match &request.operation {
                IoOperation::Close(file) if Some(*file) == *self.control.file.lock().unwrap() => {
                    Some(0)
                }
                IoOperation::RemoveFile(path) if *path == *self.control.path.lock().unwrap() => {
                    Some(1)
                }
                IoOperation::SyncDirectory(path) if path == &PathBuf::from("segments") => Some(2),
                _ => None,
            };
            let result = self.inner.submit(request);
            if let (Ok(id), Some(stage)) = (&result, stage) {
                self.control.counts.lock().unwrap()[stage] += 1;
                if stage == self.control.mode && self.control.armed.swap(false, Ordering::SeqCst) {
                    self.control.injected.lock().unwrap().insert(*id);
                }
            }
            result
        }
        fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
            let start = output.len();
            self.inner.poll(budget, output)?;
            for completion in &mut output[start..] {
                if self.control.injected.lock().unwrap().remove(&completion.id) {
                    assert!(
                        matches!(completion.result, Ok(IoOutcome::Done)),
                        "The injection point must have actually been executed successfully"
                    );
                    completion.result = Err(Error::Io(std::io::Error::other(
                        "Completion report failed after injection execution",
                    )));
                }
            }
            Ok(())
        }
        fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
            self.inner.shutdown(deadline)
        }
    }
    #[test]
    fn if_native_shutdown_deletion_and_directory_synchronization_fail_after_execution_you_can_safely_retry_from_the_original_steps()
     {
        for mode in 0..3 {
            let root = Directory(std::env::temp_dir().join(format!(
                "raster-gc-fault-{:x?}",
                StoreId::generate().unwrap().0
            )));
            let control = Arc::new(Control {
                mode,
                armed: false.into(),
                file: Mutex::new(None),
                path: Mutex::new(PathBuf::new()),
                injected: Mutex::new(BTreeSet::new()),
                counts: Mutex::new([0; 3]),
            });
            let mut config = Config::default();
            config.storage.root = root.0.clone();
            config.storage.segment_bytes = 4096;
            config.log.page_bytes = 4096;
            config.log.memory_pages = 4;
            config.index.buckets = 16;
            let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
                .config(config.clone())
                .device(Box::new(Factory(control.clone())))
                .create()
                .unwrap();
            let mut session = store.start_session(Default::default()).unwrap();
            for key in 0..400 {
                put(&mut session, key, key);
            }
            let begin = LogAddress(3 * 4096);
            *control.file.lock().unwrap() = Some(
                store
                    .inner
                    .storage
                    .resolve(LogAddress(2 * 4096))
                    .unwrap()
                    .file,
            );
            *control.path.lock().unwrap() = store.inner.storage.segment_path(2, Generation(0));
            control.armed.store(true, Ordering::SeqCst);
            let ticket = store.maintenance().shift_begin(begin).unwrap();
            let result = session.wait_maintenance(&ticket, deadline()).unwrap();
            assert!(
                matches!(&*result, Err(Error::GcFailed { begin: actual, index_cleaned: true, deleted_segments: 0, cause }) if *actual == begin && matches!(&**cause, Error::Io(_)))
            );
            let counts = *control.counts.lock().unwrap();
            for _ in 0..20 {
                store.maintenance().poll(PollBudget::default()).unwrap();
            }
            assert_eq!(
                *control.counts.lock().unwrap(),
                counts,
                "Failure will not automatically retry"
            );
            assert!(!store.inner.failed.load(Ordering::SeqCst));
            if mode != 0 {
                assert!(
                    !root
                        .0
                        .join(store.inner.storage.segment_path(2, Generation(0)))
                        .exists()
                );
            }
            // Delete intent holds independent route,A new checkpoint can still be completed after ending the failed action.
            let checkpoint = store
                .maintenance()
                .checkpoint(CheckpointKind::Full)
                .unwrap();
            let checkpoint = wait(&mut session, &checkpoint);
            assert_eq!(checkpoint.begin, begin);
            let report = gc(&store, &mut session, begin);
            assert!(matches!(report.physical, PhysicalReclamation::Completed));
            assert_eq!(report.deleted_segments, 3);
            let counts = *control.counts.lock().unwrap();
            assert_eq!(counts[0], if mode == 0 { 2 } else { 1 });
            assert_eq!(counts[1], if mode == 1 { 2 } else { 1 });
            assert_eq!(counts[2], if mode == 2 { 4 } else { 3 });
            for number in 0..3 {
                assert!(
                    !root
                        .0
                        .join(store.inner.storage.segment_path(number, Generation(0)))
                        .exists()
                );
            }
            let set = crate::api::maintenance::RecoverySet {
                store: store.id(),
                index: checkpoint.token,
                log: checkpoint.token,
            };
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
            drop(store);
            let (store, _) = recover_store(config, set).unwrap();
            let mut session = store.start_session(Default::default()).unwrap();
            assert_eq!(read_value(&mut session, 0, 0), None);
            assert_eq!(read_value(&mut session, 1, 399), Some(399));
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
        }
    }
    #[test]
    fn recycling_unflushed_disk_prefixes_does_not_produce_old_page_writes_and_the_new_checkpoint_skips_old_records_in_the_page()
     {
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-gc-discard-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let control = Arc::new(Control {
            mode: 3,
            armed: false.into(),
            file: Mutex::new(None),
            path: Mutex::new(PathBuf::new()),
            injected: Mutex::new(BTreeSet::new()),
            counts: Mutex::new([0; 3]),
        });
        let mut config = Config::default();
        config.storage.root = root.0.clone();
        config.storage.segment_bytes = 4096;
        config.log.page_bytes = 4096;
        config.log.memory_pages = 4;
        config.index.buckets = 16;
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config.clone())
            .device(Box::new(Factory(control.clone())))
            .create()
            .unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        for key in 0..80 {
            put(&mut session, key, key);
        }
        assert!(store.inner.storage.bound_files().unwrap().is_empty());
        let begin = store.inner.log.frontiers().unwrap().tail;
        control.armed.store(true, Ordering::SeqCst);
        let report = gc(&store, &mut session, begin);
        assert!(matches!(report.physical, PhysicalReclamation::Completed));
        assert_eq!(report.deleted_segments, 0);
        assert!(store.inner.storage.bound_files().unwrap().is_empty());
        control.armed.store(false, Ordering::SeqCst);
        put(&mut session, 80, 999);
        let ticket = store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap();
        let checkpoint = wait(&mut session, &ticket);
        let mut scan = store
            .scan(crate::api::scan::ScanOptions {
                begin,
                end: checkpoint.end,
                buffering: crate::api::scan::Buffering::SinglePage,
            })
            .unwrap();
        assert_eq!(scan.next_record().unwrap().unwrap().key, 999);
        assert!(scan.next_record().unwrap().is_none());
        scan.close().unwrap();
        drop(scan);
        let set = crate::api::maintenance::RecoverySet {
            store: store.id(),
            index: checkpoint.token,
            log: checkpoint.token,
        };
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
        drop(store);
        let (store, _) = recover_store(config, set).unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        assert_eq!(read_value(&mut session, 0, 79), None);
        assert_eq!(read_value(&mut session, 1, 999), Some(999));
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}

#[test]
fn when_the_cold_boundary_cuts_into_the_record_zero_effect_is_reported_and_the_action_is_released_and_legal_recycling_excludes_other_maintenance()
 {
    let (_root, store) = native_gc();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let before = store.inner.log.frontiers().unwrap();
    let ticket = store.maintenance().shift_begin(LogAddress(1)).unwrap();
    let result = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*result, Err(Error::GcFailed { begin: LogAddress(0), index_cleaned: false, deleted_segments: 0, cause }) if matches!(&**cause, Error::InvalidFormat(_)))
    );
    assert_eq!(store.inner.log.frontiers().unwrap().begin, LogAddress(0));
    assert_eq!(store.inner.log.frontiers().unwrap().tail, before.tail);
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    let begin = LogAddress(4096);
    let ticket = store.maintenance().shift_begin(begin).unwrap();
    assert!(matches!(
        store.maintenance().shift_begin(begin),
        Err(Error::Busy)
    ));
    assert!(matches!(store.maintenance().grow_index(), Err(Error::Busy)));
    assert!(matches!(
        store.maintenance().checkpoint(CheckpointKind::Full),
        Err(Error::Busy)
    ));
    assert!(matches!(
        store.maintenance().compact(CompactionOptions {
            algorithm: CompactionAlgorithm::Lookup,
            until: before.tail,
            workers: 1,
            shift_begin: false,
            checkpoint: false
        }),
        Err(Error::Busy)
    ));
    session
        .wait_maintenance(&ticket, deadline())
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
    assert_eq!(read_value(&mut session, 401, 0), None);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn late_cold_reads_cannot_cache_truncated_old_keys_in_the_chain_when_the_same_label_chain_head_is_retained()
 {
    use crate::{engine::io_hub::CompletionHub, log::lookup::LookupStep};
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-gc-cache-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 4096;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.index.buckets = 1;
    config.cache.enabled = true;
    config.cache.capacity_bytes = 8192;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 8969);
    for key in 1..400 {
        put(&mut session, key, key);
    }
    put(&mut session, 400, 9239);
    let hash = U64Key.hash(&8969);
    assert_eq!(hash.tag(), U64Key.hash(&9239).tag());
    let expected = store.inner.index.prepare(hash).unwrap();
    assert_eq!(
        expected,
        store.inner.index.prepare(U64Key.hash(&9239)).unwrap()
    );
    let route = store.inner.io.reserve(session.id()).unwrap();
    let mut lookup = store
        .inner
        .log
        .lookup(
            &store.inner.storage,
            8969u64.to_le_bytes().to_vec(),
            store
                .inner
                .resolve_index(hash, &8969u64.to_le_bytes())
                .unwrap()
                .head,
            CompletionHub::route(route),
        )
        .unwrap();
    let limit = deadline();
    loop {
        assert!(!limit.expired(), "Cold read not finalized");
        store.maintenance().poll(PollBudget::default()).unwrap();
        if let Some(completion) = store.inner.io.take(route).unwrap() {
            lookup.accept(&store.inner.storage, completion).unwrap();
        }
        match lookup
            .step(
                &store.inner.log,
                &store.inner.storage,
                PollBudget::default(),
            )
            .unwrap()
        {
            LookupStep::Continue | LookupStep::AwaitingIo => std::thread::yield_now(),
            LookupStep::Decoded(_) => break,
            _ => panic!("Should get the disk value of the old key"),
        }
    }
    store.inner.io.release(route).unwrap();
    let begin = LogAddress(4096);
    assert!(lookup.cache_record(8192).unwrap().unwrap().0 < begin);
    let ticket = store.maintenance().shift_begin(begin).unwrap();
    // Cannot be installed again when maintenance has been accepted;After completion, the source needs to be verified,rather than just relying on header snapshots.
    store.inner.populate_cache(hash, expected, &lookup).unwrap();
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    session
        .wait_maintenance(&ticket, deadline())
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
    assert_eq!(store.inner.index.prepare(hash).unwrap(), expected);
    store.inner.populate_cache(hash, expected, &lookup).unwrap();
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    assert_eq!(read_value(&mut session, 401, 8969), None);
    assert_eq!(read_value(&mut session, 402, 9239), Some(9239));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn modify_requests_pending_before_truncation_are_rechecked_for_missing_keys_and_initialized_only_once()
 {
    use crate::{api::operation::RmwOperation, schema::ValueRead};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Debug)]
    struct Create(Arc<AtomicUsize>);
    impl Keyed<Schema> for Create {
        fn key(&self) -> &u64 {
            &0
        }
    }
    impl RmwOperation<Schema> for Create {
        type Output = u64;
        fn initial(&mut self) -> Result<(u64, u64), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok((999, 999))
        }
        fn copy_update(&mut self, _: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
            panic!("Truncated recheck cannot use old values")
        }
        fn update_in_place(
            &mut self,
            _: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<u64>, Error> {
            panic!("Truncated values cannot be updated in place")
        }
    }
    let (_root, store) = native_gc();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let Submission::Pending(mut pending) = session
        .rmw(Serial(400), Create(calls.clone()), Default::default())
        .unwrap()
    else {
        panic!("Modification of old pages should be suspended first")
    };
    let begin = LogAddress(12288);
    let ticket = store.maintenance().shift_begin(begin).unwrap();
    let limit = deadline();
    while ticket.try_report().unwrap().is_none() {
        assert!(
            !limit.expired(),
            "The recovery was not terminated as per the lease extension"
        );
        store.maintenance().poll(PollBudget::default()).unwrap();
        std::thread::yield_now();
    }
    let report = ticket.try_report().unwrap().unwrap();
    assert!(matches!(
        report.as_ref().as_ref().unwrap().physical,
        PhysicalReclamation::DeferredByRuntime { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        session.wait(&mut pending, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(999)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(read_value(&mut session, 401, 0), Some(999));
    assert_eq!(read_value(&mut session, 402, 1), None);
    assert!(matches!(
        gc(&store, &mut session, begin).physical,
        PhysicalReclamation::Completed
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn after_in_page_truncation_the_old_key_of_the_same_label_must_be_appended_instead_of_updating_the_invalid_record_in_place()
 {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Debug)]
    struct Replace {
        calls: Arc<AtomicUsize>,
    }
    impl Keyed<Schema> for Replace {
        fn key(&self) -> &u64 {
            &8969
        }
    }
    impl UpsertOperation<Schema> for Replace {
        type Output = ();
        fn replacement(&mut self) -> Result<(u64, ()), Error> {
            Ok((999, ()))
        }
        fn update_in_place(
            &mut self,
            mut value: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<()>, Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            value.view_mut().store(999, Ordering::SeqCst);
            Ok(UpdateDecision::Updated(()))
        }
    }
    let mut config = Config::default();
    config.index.buckets = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 8969);
    let begin = store.inner.log.frontiers().unwrap().tail;
    put(&mut session, 1, 9239);
    assert_eq!(
        store.inner.index.prepare(U64Key.hash(&8969)).unwrap(),
        store.inner.index.prepare(U64Key.hash(&9239)).unwrap()
    );
    gc(&store, &mut session, begin);
    assert_eq!(read_value(&mut session, 2, 8969), None);
    let calls = Arc::new(AtomicUsize::new(0));
    let Submission::Ready(result) = session
        .upsert(
            Serial(3),
            Replace {
                calls: calls.clone(),
            },
        )
        .unwrap()
    else {
        panic!("There is space at the end of the memory")
    };
    result.unwrap();
    assert_eq!(read_value(&mut session, 4, 8969), Some(999));
    assert_eq!(read_value(&mut session, 5, 9239), Some(9239));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
