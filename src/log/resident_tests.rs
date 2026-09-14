use super::*;
use crate::{epoch::EpochManager, schema::builtin::AtomicU64Value};
use resident::{ReadHint, ResidentHints};
fn log(epoch: &Arc<EpochManager>) -> HybridLog<AtomicU64Value> {
    let mut log = HybridLog::new(
        crate::config::LogConfig {
            page_bytes: 4096,
            memory_pages: 4,
            ..crate::config::Config::default().log
        },
        Arc::new(AtomicU64Value),
    )
    .unwrap();
    log.bind_epoch(epoch.clone()).unwrap();
    log
}
fn insert(log: &HybridLog<AtomicU64Value>, value: u64) -> LogAddress {
    log.finish_initialization(log.reserve_record(b"key", None, value).unwrap())
        .unwrap()
}
fn read(
    log: &HybridLog<AtomicU64Value>,
    address: LogAddress,
    guard: &crate::epoch::EpochGuard<'_>,
    cache: &mut ResidentHints<AtomicU64Value>,
) -> Result<Option<u64>, Error> {
    let mut hint = ReadHint { guard, cache };
    let value = log.resident_head_borrowed(b"key", Some(address), &mut hint)?;
    value
        .map(|value| match value.try_read_live(|value| value)? {
            ValueAccess::Ready(Some(value)) => Ok(value),
            _ => panic!("expected a live, uncontended value"),
        })
        .transpose()
}
#[test]
fn retired_hints_are_invalid_before_collection_and_never_dereferenced_after_collection() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    let log = log(&epoch);
    let address = insert(&log, 7);
    let weak = Arc::downgrade(log.records.lock().unwrap().get(&address).unwrap());
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(read(&log, address, &guard, &mut hints).unwrap(), Some(7));
    log.retire(address).unwrap();
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        read(&log, address, &guard, &mut hints),
        Err(Error::RangeTruncated)
    ));
    drop(guard);
    assert_eq!(log.collect_retired().unwrap(), 1);
    assert!(weak.upgrade().is_none());
    drop(weak); // Ensure the stale hint is the only remaining address, not an Arc control-block pin.
    let guard = epoch.enter_records(&participant).unwrap();
    assert!(matches!(
        read(&log, address, &guard, &mut hints),
        Err(Error::RangeTruncated)
    ));
    drop(guard);
    epoch.unregister(&participant).unwrap();
}
#[test]
fn hints_cannot_alias_equal_addresses_in_distinct_logs_or_use_a_foreign_guard() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    let first = log(&epoch);
    let second = log(&epoch);
    let address = insert(&first, 7);
    assert_eq!(insert(&second, 42), address);
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(read(&first, address, &guard, &mut hints).unwrap(), Some(7));
    assert_eq!(
        read(&second, address, &guard, &mut hints).unwrap(),
        Some(42)
    );
    let other = EpochManager::new().unwrap();
    let wrong = other.register().unwrap();
    let foreign = other.enter_records(&wrong).unwrap();
    assert!(read(&second, address, &foreign, &mut hints).is_err());
    drop(foreign);
    other.unregister(&wrong).unwrap();
    drop(guard);
    epoch.unregister(&participant).unwrap();
}
#[test]
fn directory_poisoning_rejects_a_warmed_hint() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    let log = log(&epoch);
    let address = insert(&log, 7);
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(read(&log, address, &guard, &mut hints).unwrap(), Some(7));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lock = log.records.lock().unwrap();
            panic!("injected directory poison");
        }))
        .is_err()
    );
    assert!(read(&log, address, &guard, &mut hints).is_err());
    drop(guard);
    epoch.unregister(&participant).unwrap();
}
#[test]
fn eviction_waits_for_the_old_epoch_and_rejects_hints_to_recycled_pages() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    // A single physical slot forces reuse after the old epoch exits.
    let mut log = HybridLog::new(
        crate::config::LogConfig {
            page_bytes: 4096,
            memory_pages: 1,
            ..crate::config::Config::default().log
        },
        Arc::new(AtomicU64Value),
    )
    .unwrap();
    log.bind_epoch(epoch.clone()).unwrap();
    let address = insert(&log, 7);
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(read(&log, address, &guard, &mut hints).unwrap(), Some(7));
    log.pad_tail().unwrap();
    // Model a completed flush; full engine tests perform the actual device I/O.
    {
        let mut state = log.state.write().unwrap();
        state.frontiers.read_only = LogAddress(4096);
        state.frontiers.safe_read_only = LogAddress(4096);
        state.frontiers.flushed_until = LogAddress(4096);
    }
    assert!(!log.evict_next().unwrap().phase_advanced);
    assert_eq!(log.frontiers().unwrap().safe_head, LogAddress(0));
    assert!(read(&log, address, &guard, &mut hints).unwrap().is_none());
    drop(guard);
    assert!(log.evict_next().unwrap().phase_advanced);
    assert_eq!(log.frontiers().unwrap().safe_head, LogAddress(4096));
    let new_address = insert(&log, 42);
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(
        read(&log, new_address, &guard, &mut hints).unwrap(),
        Some(42)
    );
    assert!(read(&log, address, &guard, &mut hints).unwrap().is_none());
    drop(guard);
    epoch.unregister(&participant).unwrap();
}

#[test]
fn resident_floor_preserves_read_only_records_and_rejects_the_truncated_prefix() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    let log = log(&epoch);
    let first = insert(&log, 7);
    let second = insert(&log, 42);
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(read(&log, first, &guard, &mut hints).unwrap(), Some(7));
    let end = log.pad_tail().unwrap();
    log.advance_read_only(end).unwrap();
    assert_eq!(read(&log, first, &guard, &mut hints).unwrap(), Some(7));
    let reservation = log.reserve_record(b"reserved", None, 99).unwrap();
    assert!(matches!(log.publish_begin(second), Err(Error::Busy)));
    assert_eq!(read(&log, first, &guard, &mut hints).unwrap(), Some(7));
    drop(reservation);
    log.publish_begin(second).unwrap();
    assert_eq!(read(&log, first, &guard, &mut hints).unwrap(), None);
    assert_eq!(read(&log, second, &guard, &mut hints).unwrap(), Some(42));
    assert!(
        log.publish_begin(log.pool.tail().unwrap().checked_add(8).unwrap())
            .is_err()
    );
    assert_eq!(read(&log, second, &guard, &mut hints).unwrap(), Some(42));
    drop(guard);
    epoch.unregister(&participant).unwrap();
}
#[test]
fn boundary_poisoning_rejects_a_warmed_hint() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    let log = log(&epoch);
    let address = insert(&log, 7);
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(read(&log, address, &guard, &mut hints).unwrap(), Some(7));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lock = log.state.write().unwrap();
            panic!("injected log boundary poison");
        }))
        .is_err()
    );
    assert!(read(&log, address, &guard, &mut hints).is_err());
    drop(guard);
    epoch.unregister(&participant).unwrap();
}
#[test]
fn restored_log_excludes_its_cold_prefix_and_accepts_new_resident_records() {
    let epoch = Arc::new(EpochManager::new().unwrap());
    let participant = epoch.register().unwrap();
    let mut log = HybridLog::from_checkpoint(
        crate::config::LogConfig {
            page_bytes: 4096,
            memory_pages: 4,
            ..crate::config::Config::default().log
        },
        Arc::new(AtomicU64Value),
        LogAddress(64),
        LogAddress(4096),
    )
    .unwrap();
    log.bind_epoch(epoch.clone()).unwrap();
    let mut hints = ResidentHints::default();
    let guard = epoch.enter_records(&participant).unwrap();
    assert_eq!(
        read(&log, LogAddress(64), &guard, &mut hints).unwrap(),
        None
    );
    let address = insert(&log, 42);
    assert_eq!(address, LogAddress(4096));
    assert_eq!(read(&log, address, &guard, &mut hints).unwrap(), Some(42));
    drop(guard);
    epoch.unregister(&participant).unwrap();
}
