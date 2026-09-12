use super::*;
use crate::{
    device::memory::MemoryDevice, schema::builtin::AtomicU64Value, storage::SegmentedStorage,
};

fn owners() -> (HybridLog<AtomicU64Value>, SegmentedStorage) {
    let log = HybridLog::new(
        LogConfig {
            page_bytes: 256,
            memory_pages: 2,
            mutable_fraction: 0.5,
        },
        Arc::new(AtomicU64Value),
    )
    .unwrap();
    let storage =
        SegmentedStorage::new(Arc::new(MemoryDevice::new(16, 4096).unwrap()), 4096).unwrap();
    (log, storage)
}

#[test]
fn resident_lookup_identity_does_not_retain_shared_control_owners() {
    let (log, storage) = owners();
    let address = log
        .finish_initialization(log.reserve_record(b"key", None, 7).unwrap())
        .unwrap();
    let counts = (
        Arc::strong_count(&log.state),
        Arc::strong_count(&storage.identity),
    );
    let mut lookup = log
        .lookup_deferred(
            &storage,
            b"key".to_vec(),
            Some(address),
            crate::device::CompletionRoute(7),
        )
        .unwrap();
    let observed = (
        Arc::strong_count(&log.state),
        Arc::strong_count(&storage.identity),
    );
    match lookup.step(&log, &storage, PollBudget::default()).unwrap() {
        lookup::LookupStep::Resident(lease) => assert_eq!(lease.read(|v| v).unwrap(), 7),
        _ => panic!("Expected an actual resident lookup"),
    }
    drop(lookup);
    assert_eq!(
        (
            Arc::strong_count(&log.state),
            Arc::strong_count(&storage.identity)
        ),
        counts
    );
    assert_eq!(
        observed, counts,
        "Identity checks must not increment shared reference counts for each query"
    );
}

#[test]
fn foreign_owners_are_rejected_without_consuming_the_lookup_or_completion() {
    let (log, storage) = owners();
    let (other_log, other_storage) = owners();
    let address = log
        .finish_initialization(log.reserve_record(b"key", None, 7).unwrap())
        .unwrap();
    other_log
        .finish_initialization(other_log.reserve_record(b"key", None, 99).unwrap())
        .unwrap();
    let mut query = log
        .lookup_deferred(
            &storage,
            b"key".to_vec(),
            Some(address),
            crate::device::CompletionRoute(7),
        )
        .unwrap();
    for (log, storage) in [
        (&other_log, &storage),
        (&log, &other_storage),
        (&other_log, &other_storage),
    ] {
        assert!(matches!(
            query.step(log, storage, PollBudget::default()),
            Err(Error::InvalidState("Query attribution does not match"))
        ));
        assert!(matches!(
            query.with_matched_record::<_, ()>(log, storage, |_| panic!(
                "A foreign owner cannot reach a record callback"
            )),
            Err(Error::InvalidState(
                "Source record query attribution does not match"
            ))
        ));
    }
    let completion = crate::device::IoCompletion {
        id: IoId(99),
        route: crate::device::CompletionRoute(7),
        result: Err(Error::RangeTruncated),
        buffer: None,
    };
    let rejected = query.accept(&other_storage, completion).unwrap_err();
    assert_eq!(rejected.request.id, IoId(99));
    assert!(matches!(
        rejected.reason,
        Error::InvalidState("Query belongs to other storage")
    ));
    match query.step(&log, &storage, PollBudget::default()).unwrap() {
        lookup::LookupStep::Resident(lease) => assert_eq!(lease.read(|value| value).unwrap(), 7),
        _ => panic!("Rejected foreign calls must leave the original query usable"),
    }
}

#[test]
fn dropped_instance_ids_never_match_new_owners_and_old_value_leases_remain_valid() {
    let (log, storage) = owners();
    let address = log
        .finish_initialization(log.reserve_record(b"key", None, 7).unwrap())
        .unwrap();
    let mut query = log
        .lookup_deferred(
            &storage,
            b"key".to_vec(),
            Some(address),
            crate::device::CompletionRoute(7),
        )
        .unwrap();
    let log_id = log.state.identity;
    let storage_id = *storage.identity;
    let lease = log.lease(address).unwrap();
    drop((log, storage));
    for _ in 0..256 {
        let (log, storage) = owners();
        assert_ne!(log.state.identity, log_id);
        assert_ne!(*storage.identity, storage_id);
        assert!(matches!(
            query.step(&log, &storage, PollBudget::default()),
            Err(Error::InvalidState("Query attribution does not match"))
        ));
        assert!(matches!(
            query.with_matched_record::<_, ()>(&log, &storage, |_| panic!(
                "An expired query must not match a replacement instance"
            )),
            Err(Error::InvalidState(
                "Source record query attribution does not match"
            ))
        ));
    }
    assert_eq!(lease.read(|value| value).unwrap(), 7);
}
