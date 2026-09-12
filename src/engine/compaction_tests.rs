//! Exposing physical scans of single storage compression,Two algorithms,Error impact and recovery.
use super::*;
use crate::{
    api::{
        maintenance::{CompactionAlgorithm, CompactionOptions},
        scan::{Buffering, ScanOptions},
    },
    schema::KeyCodec,
};
fn options(algorithm: CompactionAlgorithm, until: LogAddress) -> CompactionOptions {
    CompactionOptions {
        algorithm,
        until,
        workers: 1,
        shift_begin: false,
        checkpoint: false,
    }
}
#[test]
fn two_compression_algorithms_retain_current_value_tombstones_and_session_progress_across_cold_pages_and_recover_independently()
 {
    for algorithm in [CompactionAlgorithm::Lookup, CompactionAlgorithm::ScanDedup] {
        let (_root, store) = setup(None);
        let mut session = store.start_session(Default::default()).unwrap();
        for key in 0..400 {
            put(&mut session, key, key);
        }
        put(&mut session, 400, 7);
        match session
            .delete(Serial(401), Delete(6), Default::default())
            .unwrap()
        {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
        let before = store.inner.log.frontiers().unwrap();
        let ticket = store
            .maintenance()
            .compact(options(algorithm, before.tail))
            .unwrap();
        assert!(matches!(
            store.maintenance().compact(options(algorithm, before.tail)),
            Err(Error::Busy)
        ));
        assert!(matches!(store.maintenance().grow_index(), Err(Error::Busy)));
        assert!(matches!(
            store.maintenance().checkpoint(CheckpointKind::Full),
            Err(Error::Busy)
        ));
        let report = session.wait_maintenance(&ticket, deadline()).unwrap();
        let report = report.as_ref().as_ref().unwrap();
        assert_eq!(report.copied, 400);
        assert_eq!(report.until, before.tail);
        assert!(report.gc.is_none() && report.checkpoint.is_none());
        assert_eq!(store.inner.log.frontiers().unwrap().begin, before.begin);
        assert_eq!(
            store.inner.coordinator.last_accepted(session.id()).unwrap(),
            Some(Serial(401))
        );
        let mut scan = store
            .scan(ScanOptions {
                begin: before.tail,
                end: store.inner.log.frontiers().unwrap().tail,
                buffering: Buffering::DoublePage,
            })
            .unwrap();
        let mut copied = std::collections::BTreeMap::new();
        while let Some(record) = scan.next_record().unwrap() {
            assert!(!record.invalid);
            assert!(copied.insert(record.key, record.value).is_none());
        }
        assert_eq!(copied.len(), 400);
        for key in 0..400 {
            assert_eq!(copied[&key], if key == 6 { None } else { Some(key) });
        }
        scan.close().unwrap();
        drop(scan);
        let checkpoint = store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap();
        let report = wait(&mut session, &checkpoint);
        assert_eq!(
            report
                .sessions
                .iter()
                .find(|cut| cut.session == session.id())
                .unwrap()
                .serial,
            Serial(401)
        );
        let config = store.inner.config.clone();
        let set = crate::api::maintenance::RecoverySet {
            store: store.id(),
            index: report.token,
            log: report.token,
        };
        let session_id = session.id();
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
        drop(store);
        let (store, _) = recover_store(config, set).unwrap();
        let mut session = store.continue_session(session_id).unwrap().session;
        for key in 0..400 {
            assert_eq!(
                read_value(&mut session, 402 + key, key),
                if key == 6 { None } else { Some(key) }
            );
        }
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}
#[test]
fn fails_first_and_does_not_migrate_or_move_when_the_cold_end_boundary_is_in_the_middle_of_the_recordbegin()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 0);
    let first = store
        .inner
        .resolve_index(U64Key.hash(&0), &0u64.to_le_bytes())
        .unwrap()
        .head
        .unwrap();
    for key in 1..400 {
        put(&mut session, key, key);
    }
    for algorithm in [CompactionAlgorithm::Lookup, CompactionAlgorithm::ScanDedup] {
        let before = store.inner.log.frontiers().unwrap();
        let until = first.checked_add(1).unwrap();
        let ticket = store
            .maintenance()
            .compact(options(algorithm, until))
            .unwrap();
        let report = session.wait_maintenance(&ticket, deadline()).unwrap();
        assert!(
            matches!(&*report, Err(Error::CompactionFailed { copied: 0, cause, .. }) if matches!(&**cause, Error::InvalidFormat(_)))
        );
        assert_eq!(store.inner.log.frontiers().unwrap().tail, before.tail);
        assert_eq!(store.inner.log.frontiers().unwrap().begin, before.begin);
        assert!(!store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    }
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn the_scanning_deduplication_budget_is_insufficient_zero_migration_is_terminated_and_the_empty_range_and_subsequent_actions_are_executable()
 {
    let mut config = Config::default();
    config.maintenance.max_compaction_keys = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 1);
    put(&mut session, 1, 2);
    let until = store.inner.log.frontiers().unwrap().tail;
    let ticket = store
        .maintenance()
        .compact(options(CompactionAlgorithm::ScanDedup, until))
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*report, Err(Error::CompactionFailed { copied: 0, cause, .. }) if matches!(&**cause, Error::CapacityExceeded))
    );
    assert_eq!(store.inner.log.frontiers().unwrap().tail, until);
    let empty = store
        .maintenance()
        .compact(options(CompactionAlgorithm::Lookup, LogAddress(0)))
        .unwrap();
    assert_eq!(
        session
            .wait_maintenance(&empty, deadline())
            .unwrap()
            .as_ref()
            .as_ref()
            .unwrap()
            .copied,
        0
    );
    let ticket = store
        .maintenance()
        .compact(options(CompactionAlgorithm::Lookup, until))
        .unwrap();
    assert_eq!(
        session
            .wait_maintenance(&ticket, deadline())
            .unwrap()
            .as_ref()
            .as_ref()
            .unwrap()
            .copied,
        2
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn memory_capacity_exhaustion_reports_migrated_data_and_source_data_is_retained_but_new_tasks_are_not_implicitly_migratedbegin()
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
    for key in 0..90 {
        put(&mut session, key, key);
    }
    let before = store.inner.log.frontiers().unwrap();
    let ticket = store
        .maintenance()
        .compact(options(CompactionAlgorithm::Lookup, before.tail))
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    let copied = match &*report {
        Err(Error::CompactionFailed { copied, cause, .. }) => {
            assert!(matches!(&**cause, Error::CapacityExceeded));
            *copied
        }
        _ => panic!("Insufficient capacity must report partial failure"),
    };
    assert!(copied > 0 && copied < 90);
    let mut scan = store
        .scan(ScanOptions {
            begin: before.tail,
            end: store.inner.log.frontiers().unwrap().tail,
            buffering: Buffering::Unbuffered,
        })
        .unwrap();
    let mut actual = 0;
    while scan.next_record().unwrap().is_some() {
        actual += 1;
    }
    assert_eq!(actual, copied);
    scan.close().unwrap();
    assert_eq!(store.inner.log.frontiers().unwrap().begin, before.begin);
    assert!(!store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    for key in 0..90 {
        assert_eq!(read_value(&mut session, 90 + key, key), Some(key));
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
