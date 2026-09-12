//! Real engine diagnostics,sampling boundary,Preallocation and restoration;Metrics must be consistent with operational results and resource lifecycle.
use super::*;
use crate::api::{Outcome, TicketState};

#[test]
fn both_preallocation_creation_and_recovery_reserve_page_capacity_but_do_not_advance_the_logical_tail_early()
 {
    for preallocate in [false, true] {
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-preallocate-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let mut config = Config::default();
        config.storage.root = root.0.clone();
        config.log.page_bytes = 4096;
        config.log.memory_pages = 4;
        config.storage.pre_allocate_log = preallocate;
        config.cache.enabled = true;
        config.cache.capacity_bytes = 8192;
        config.cache.pre_allocate = preallocate;
        config.statistics.enabled = true;
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config.clone())
            .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 64,
            }))
            .create()
            .unwrap();
        let empty = store.diagnostics().unwrap();
        assert_eq!(
            empty.log_allocated_bytes,
            if preallocate { 16384 } else { 0 }
        );
        assert_eq!(empty.log_resident_pages, 0);
        assert_eq!(empty.cached_bytes, 0);
        assert_eq!(
            empty.cache_reserved_bytes,
            if preallocate { 8192 } else { 0 }
        );
        assert_eq!(empty.tail, LogAddress(0));
        let mut session = store.start_session(Default::default()).unwrap();
        for key in 0..400 {
            put(&mut session, key, key);
        }
        let loaded = store.diagnostics().unwrap();
        assert_eq!(loaded.active_sessions, 1);
        assert_eq!(loaded.pending_requests, 0);
        assert_eq!(loaded.active_requests, 0);
        assert!(loaded.log_span_bytes > 16384);
        assert!(loaded.log_resident_pages <= 4);
        assert!(loaded.log_allocated_bytes <= 16384);
        assert!(loaded.bucket_distribution.iter().sum::<u64>() > 0);
        assert_eq!(store.statistics().upserts.completed, 400);
        let checkpoint = store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap();
        let report = wait(&mut session, &checkpoint);
        let set = crate::api::maintenance::RecoverySet {
            store: store.id(),
            index: report.token,
            log: report.token,
        };
        let session_id = session.id();
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
        let (reader, recovery) = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 64,
            }))
            .recover(set)
            .unwrap();
        let recovered = reader.diagnostics().unwrap();
        assert_eq!(recovered.log_resident_pages, 0);
        assert_eq!(
            recovered.log_allocated_bytes,
            if preallocate { 16384 } else { 0 }
        );
        assert_eq!(recovered.tail, report.end);
        assert_eq!(recovered.active_sessions, 0);
        assert_eq!(
            recovered.cache_reserved_bytes,
            if preallocate { 8192 } else { 0 }
        );
        assert_eq!(reader.statistics().upserts.accepted, 0);
        assert_eq!(recovery.sessions[0].serial, Serial(399));
        let mut resumed = reader.continue_session(session_id).unwrap().session;
        assert_eq!(read_value(&mut resumed, 400, 399), Some(399));
        assert!(reader.diagnostics().unwrap().cached_bytes > 0);
        assert_eq!(reader.statistics().cache.insertions, 1);
        assert_eq!(read_value(&mut resumed, 401, 399), Some(399));
        assert_eq!(reader.statistics().cache.hits, 1);
        put(&mut resumed, 402, 400);
        resumed.close(deadline()).unwrap();
        reader.shutdown(deadline()).unwrap();
    }
}

#[derive(Debug)]
struct Modify(u64, bool);
impl Keyed<Schema> for Modify {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl crate::api::operation::RmwOperation<Schema> for Modify {
    type Output = ();
    fn initial(&mut self) -> Result<(u64, ()), Error> {
        Ok((1, ()))
    }
    fn copy_update(
        &mut self,
        old: crate::schema::ValueRead<'_, Schema>,
    ) -> Result<(u64, ()), Error> {
        if self.1 {
            return Err(Error::Codec("Test calculation failed"));
        }
        Ok((old.view().wrapping_add(1), ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        if self.1 {
            return Ok(UpdateDecision::Append);
        }
        value
            .view_mut()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(UpdateDecision::Updated(()))
    }
}
#[derive(Debug)]
struct Delete(u64, bool);
impl Keyed<Schema> for Delete {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl crate::api::operation::DeleteOperation<Schema> for Delete {
    type Output = ();
    fn complete(self, _: crate::api::operation::DeleteOutcome) {
        assert!(!self.1, "Test the output panic after deletion takes effect");
    }
}
#[test]
fn the_four_operation_statistics_distinguish_between_missing_abort_failure_and_panic_after_deletion_takes_effect_and_refuse_to_be_counted()
 {
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    assert!(matches!(
        session
            .rmw(Serial(1), Modify(7, false), Default::default())
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(())))
    ));
    assert!(matches!(
        session
            .rmw(
                Serial(2),
                Modify(8, false),
                crate::api::operation::RmwOptions {
                    create_if_missing: false
                }
            )
            .unwrap(),
        Submission::Ready(Ok(Outcome::NotFound))
    ));
    assert!(matches!(
        session
            .rmw(Serial(3), Modify(7, true), Default::default())
            .unwrap(),
        Submission::Ready(Err(_))
    ));
    assert_eq!(read_value(&mut session, 4, 7), Some(8));
    assert!(matches!(
        session
            .delete(
                Serial(5),
                Delete(7, false),
                crate::api::operation::DeleteOptions {
                    force_tombstone: true
                }
            )
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(())))
    ));
    assert!(matches!(
        session
            .read(
                Serial(6),
                Read(7),
                crate::api::operation::ReadOptions {
                    abort_if_tombstone: true
                }
            )
            .unwrap(),
        Submission::Ready(Ok(Outcome::Aborted(_)))
    ));
    let before = store.statistics();
    assert!(session.upsert(Serial(6), Put(9)).is_err());
    assert_eq!(store.statistics().upserts.accepted, before.upserts.accepted);
    put(&mut session, 7, 9);
    let Submission::Ready(Err(failure)) = session
        .delete(Serial(8), Delete(9, true), Default::default())
        .unwrap()
    else {
        panic!("Output panic should return operation failed");
    };
    assert_eq!(failure.effect, Effect::Applied);
    let stats = store.statistics();
    assert_eq!((stats.upserts.accepted, stats.upserts.success), (2, 2));
    assert_eq!(
        (
            stats.rmw.accepted,
            stats.rmw.success,
            stats.rmw.not_found,
            stats.rmw.failed
        ),
        (3, 1, 1, 1)
    );
    assert_eq!((stats.reads.success, stats.reads.aborted), (1, 1));
    assert_eq!(
        (stats.deletes.completed, stats.deletes.failed_after_applied),
        (2, 1)
    );
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    assert!(store.diagnostics().unwrap().failed);
    drop(session);
    store.shutdown(deadline()).unwrap();
}

#[cfg(feature = "config-toml")]
#[test]
fn profile_driven_native_creation_of_pre_allocated_cache_statistics_and_shutdown_of_instances() {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-config-engine-{:x?}",
        StoreId::generate().unwrap().0
    )));
    std::fs::create_dir_all(&root.0).unwrap();
    let file = root.0.join("config.toml");
    std::fs::write(&file, "[raster.storage]\npre_allocate_log=true\n[raster.log]\npage_bytes=4096\nmemory_pages=4\n[raster.cache]\nenabled=true\ncapacity_bytes=8192\npre_allocate=true\n[raster.statistics]\nenabled=true").unwrap();
    let mut config = Config::from_toml_file_at(&file, &["raster"]).unwrap();
    config.storage.root = root.0.join("data");
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 64,
        }))
        .create()
        .unwrap();
    let snapshot = store.diagnostics().unwrap();
    assert_eq!(
        (snapshot.log_allocated_bytes, snapshot.cache_reserved_bytes),
        (16384, 8192)
    );
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    assert_eq!(read_value(&mut session, 401, 0), Some(0));
    assert_eq!(store.statistics().cache.hits, 1);
    assert_eq!(store.statistics().upserts.completed, 400);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn sampling_is_turned_off_without_hiding_in_transit_requests_and_existing_request_statistics_are_completely_completed_and_can_be_output()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    assert_eq!(store.statistics().upserts.accepted, 0);
    store.enable_stats_collection();
    let Submission::Pending(mut pending) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("Cold reads should be suspended");
    };
    let active = store.diagnostics().unwrap();
    assert_eq!(active.active_requests, 1);
    assert_eq!(active.pending_requests, 1);
    assert_eq!(store.statistics().reads.accepted, 1);
    assert_eq!(store.statistics().reads.completed, 0);
    store.disable_stats_collection();
    let end = deadline();
    while store.diagnostics().unwrap().pending_requests != 0 {
        assert!(!end.expired());
        session.poll(PollBudget::default()).unwrap();
    }
    let stats = store.statistics();
    assert!(!stats.enabled);
    assert!(stats.measurements_complete && !stats.saturated);
    assert_eq!(stats.reads.completed, 1);
    assert_eq!(stats.reads.success, 1);
    assert_eq!(stats.reads.synchronous, 0);
    assert!(stats.reads.io_completions > 0);
    assert_eq!(stats.reads.io_per_request.iter().sum::<u64>(), 1);
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    assert!(matches!(
        pending.try_take().unwrap(),
        TicketState::Ready(Ok(Outcome::Success(0)))
    ));
    assert_eq!(read_value(&mut session, 401, 1), Some(1));
    assert_eq!(store.statistics().reads.accepted, 1);
    store.enable_stats_collection();
    assert_eq!(read_value(&mut session, 402, 9999), None);
    let stats = store.statistics();
    assert_eq!(stats.reads.accepted, 2);
    assert_eq!(stats.reads.not_found, 1);
    let before = stats.upserts.accepted;
    assert!(session.upsert(Serial(401), Put(999)).is_err());
    assert_eq!(store.statistics().upserts.accepted, before);
    let mut text = Vec::new();
    store.write_statistics(&mut text).unwrap();
    let text = String::from_utf8(text).unwrap();
    assert!(
        text.contains("per request I/O completed") && text.contains("Statistics collection:true")
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn diagnosis_during_expansion_retains_the_true_distribution_of_the_two_tables_and_observations_will_not_change_the_statistics()
 {
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..40 {
        put(&mut session, key, key);
    }
    let old = store.diagnostics().unwrap();
    let ticket = store.maintenance().grow_index().unwrap();
    let end = deadline();
    let growing = loop {
        session.poll(PollBudget::default()).unwrap();
        store
            .maintenance()
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
        let snapshot = store.diagnostics().unwrap();
        if snapshot.growing_index.is_some() {
            break snapshot;
        }
        assert!(!end.expired());
    };
    assert_eq!(growing.table_generation, old.table_generation);
    assert_eq!(
        growing.growing_index.unwrap().bucket_distribution.len(),
        old.bucket_distribution.len() * 2
    );
    let before = store.statistics();
    store.diagnostics().unwrap();
    let after = store.statistics();
    assert_eq!(before.index.lookups, after.index.lookups);
    assert_eq!(
        before.index.publication_attempts,
        after.index.publication_attempts
    );
    session
        .wait_maintenance(&ticket, deadline())
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap();
    let grown = store.diagnostics().unwrap();
    assert!(grown.growing_index.is_none());
    assert_eq!(
        grown.bucket_distribution.len(),
        old.bucket_distribution.len() * 2
    );
    assert_ne!(grown.table_generation, old.table_generation);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn the_actual_number_of_sessions_and_requests_can_be_observed_across_threads_during_synchronous_callback_execution()
 {
    use std::sync::mpsc;
    struct Blocked {
        key: u64,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Keyed<Schema> for Blocked {
        fn key(&self) -> &u64 {
            &self.key
        }
    }
    impl crate::api::operation::ReadOperation<Schema> for Blocked {
        type Output = u64;
        fn read(&mut self, value: crate::schema::ValueRead<'_, Schema>) -> Result<u64, Error> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(10)).unwrap();
            Ok(*value.view())
        }
    }
    let (_root, store) = setup(None);
    let mut writer = store.start_session(Default::default()).unwrap();
    put(&mut writer, 0, 7);
    writer.close(deadline()).unwrap();
    store.enable_stats_collection();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let mut reader = store.start_session(Default::default()).unwrap();
            let result = reader
                .read(
                    Serial(0),
                    Blocked {
                        key: 7,
                        entered: entered_tx,
                        release: release_rx,
                    },
                    Default::default(),
                )
                .unwrap_or_else(|_| panic!("Synchronous read rejected"));
            assert!(matches!(result, Submission::Ready(Ok(Outcome::Success(7)))));
            reader.close(deadline()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let observed = store.diagnostics().unwrap();
        assert_eq!(
            (
                observed.active_sessions,
                observed.active_requests,
                observed.pending_requests
            ),
            (1, 1, 0)
        );
        assert_eq!(
            (
                store.statistics().reads.accepted,
                store.statistics().reads.completed
            ),
            (1, 0)
        );
        store.disable_stats_collection();
        release_tx.send(()).unwrap();
        worker.join().unwrap();
    });
    let observed = store.diagnostics().unwrap();
    assert_eq!((observed.active_sessions, observed.active_requests), (0, 0));
    assert_eq!(
        (
            store.statistics().reads.completed,
            store.statistics().reads.synchronous
        ),
        (1, 1)
    );
    store.shutdown(deadline()).unwrap();
}

#[test]
fn after_the_atomic_modification_the_panic_record_has_an_unknown_impact_once_and_fails_to_close_and_no_longer_receives_statistics()
 {
    #[derive(Debug)]
    struct Panic(u64);
    impl Keyed<Schema> for Panic {
        fn key(&self) -> &u64 {
            &self.0
        }
    }
    impl UpsertOperation<Schema> for Panic {
        type Output = ();
        fn replacement(&mut self) -> Result<(u64, ()), Error> {
            panic!("Expected path in place");
        }
        fn update_in_place(
            &mut self,
            mut value: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<()>, Error> {
            value
                .view_mut()
                .store(99, std::sync::atomic::Ordering::SeqCst);
            panic!("Panic after testing atomic value modification");
        }
    }
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let Submission::Ready(Err(error)) = session.upsert(Serial(1), Panic(7)).unwrap() else {
        panic!("It should be a failure after modification.");
    };
    assert_eq!(error.effect, Effect::Unknown);
    assert!(session.upsert(Serial(2), Put(8)).is_err());
    let stats = store.statistics();
    assert_eq!(
        (
            stats.upserts.accepted,
            stats.upserts.completed,
            stats.upserts.failed_with_unknown_effect
        ),
        (2, 2, 1)
    );
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    drop(session);
    store.shutdown(deadline()).unwrap();
}
