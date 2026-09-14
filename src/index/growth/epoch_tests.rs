use super::*;
use crate::epoch::{DeferredAction, EpochManager};

fn budget(n: usize) -> PollBudget {
    PollBudget(std::num::NonZeroUsize::new(n).unwrap())
}

#[test]
fn stable_route_rejects_foreign_guards_and_poisoned_state() {
    let epoch = EpochManager::new().unwrap();
    let other = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let foreign = other.register().unwrap();
    let foreign_guard = other.enter(&foreign).unwrap();
    assert!(index.prepare_guarded(KeyHash(0), &foreign_guard).is_err());
    let participant = epoch.register().unwrap();
    let guard = epoch.enter(&participant).unwrap();
    assert_eq!(
        index.prepare_guarded(KeyHash(0), &guard).unwrap().head,
        IndexHead::Empty
    );
    assert!(
        std::panic::catch_unwind(|| {
            let _state = index.state.write().unwrap();
            panic!("injected routing poison");
        })
        .is_err()
    );
    assert!(index.prepare_guarded(KeyHash(0), &guard).is_err());
}

#[test]
fn old_route_survives_concurrent_migration_until_the_reader_exits() {
    let epoch = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let entry = index.prepare(KeyHash(0)).unwrap();
    index
        .compare_publish(entry, IndexHead::Log(LogAddress(64)))
        .unwrap();
    let old = Arc::downgrade(&index.state.read().unwrap().active);
    let participant = epoch.register().unwrap();
    let loaded = std::sync::Barrier::new(2);
    let migrated = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let guard = epoch.enter(&participant).unwrap();
            let pointer = index.stable.load(Ordering::SeqCst);
            assert!(!pointer.is_null());
            loaded.wait();
            migrated.wait();
            // SAFETY: This is the pointer obtained under the still-live guard
            // from the bound manager. Migration must retain it until guard exit.
            let snapshot = unsafe { &*pointer }.prepare(KeyHash(0)).unwrap();
            assert_eq!(snapshot.table_generation, Generation(0));
            assert_eq!(snapshot.head, IndexHead::Log(LogAddress(64)));
            let current = index.prepare_guarded(KeyHash(0), &guard).unwrap();
            assert_eq!(current.table_generation, Generation(1));
            assert!(matches!(
                index
                    .compare_publish(snapshot, IndexHead::Log(LogAddress(128)))
                    .unwrap(),
                PublishResult::Conflict(_)
            ));
            drop(guard);
        });
        loaded.wait();
        index.begin_growth().unwrap();
        assert!(index.stable.load(Ordering::SeqCst).is_null());
        assert!(index.grow_step(budget(1)).unwrap().complete);
        epoch
            .defer(DeferredAction::ReleaseIndex(Generation(0)))
            .unwrap();
        epoch.advance().unwrap();
        assert!(epoch.collect().unwrap().is_empty());
        assert!(old.upgrade().is_some());
        migrated.wait();
        reader.join().unwrap();
    });
    let actions = epoch.collect().unwrap();
    assert_eq!(actions, vec![DeferredAction::ReleaseIndex(Generation(0))]);
    // SAFETY: The bound manager collected the post-migration retirement after
    // the only old reader exited and its thread joined.
    unsafe { index.release_retired(Generation(0)) }.unwrap();
    assert!(old.upgrade().is_none());
    epoch.unregister(&participant).unwrap();
}

#[test]
fn partial_migration_failure_keeps_canonical_routing_for_both_tables() {
    let epoch = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 2 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let first = index.prepare(KeyHash(0)).unwrap();
    index
        .compare_publish(first, IndexHead::Log(LogAddress(64)))
        .unwrap();
    let second = index.prepare(KeyHash(1)).unwrap();
    index
        .compare_publish(second, IndexHead::Cache(CacheAddress(128)))
        .unwrap();
    index.begin_growth().unwrap();
    assert!(index.grow_step(budget(2)).is_err());
    assert!(index.stable.load(Ordering::SeqCst).is_null());
    let participant = epoch.register().unwrap();
    let guard = epoch.enter(&participant).unwrap();
    let first = index.prepare_guarded(KeyHash(0), &guard).unwrap();
    let second = index.prepare_guarded(KeyHash(1), &guard).unwrap();
    assert_eq!(
        (first.table_generation, first.head),
        (Generation(1), IndexHead::Log(LogAddress(64)))
    );
    assert_eq!(
        (second.table_generation, second.head),
        (Generation(0), IndexHead::Cache(CacheAddress(128)))
    );
}

#[test]
fn restore_replaces_the_cached_table_before_guarded_reads_resume() {
    let epoch = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let old = Arc::downgrade(&index.state.read().unwrap().active);
    index
        .restore(crate::format::IndexSnapshot {
            buckets: 1,
            generation: Generation(8),
            entries: vec![crate::format::IndexEntry {
                bucket: 0,
                tag: KeyHash(0).tag(),
                address: LogAddress(256),
            }],
        })
        .unwrap();
    assert!(old.upgrade().is_none());
    let participant = epoch.register().unwrap();
    let guard = epoch.enter(&participant).unwrap();
    let current = index.prepare_guarded(KeyHash(0), &guard).unwrap();
    assert_eq!(
        (current.table_generation, current.head),
        (Generation(8), IndexHead::Log(LogAddress(256)))
    );
}

#[test]
fn a_loaded_route_is_rechecked_after_migration_and_publication() {
    let epoch = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    index.bind_epoch(&epoch).unwrap();
    let entry = index.prepare(KeyHash(0)).unwrap();
    index
        .compare_publish(entry, IndexHead::Log(LogAddress(64)))
        .unwrap();
    let participant = epoch.register().unwrap();
    let guard = epoch.enter(&participant).unwrap();
    let old = index.state.read().unwrap().active.clone();
    index.begin_growth().unwrap();
    assert!(index.grow_step(budget(1)).unwrap().complete);
    let current = index.prepare(KeyHash(0)).unwrap();
    index
        .compare_publish(current, IndexHead::Log(LogAddress(128)))
        .unwrap();
    // The extra Arc isolates route freshness here. The concurrent test above
    // separately proves lifetime protection using only the actual epoch guard.
    let resolved = index.prepare_loaded(KeyHash(0), &old).unwrap();
    assert_eq!(resolved.table_generation, Generation(1));
    assert_eq!(resolved.head, IndexHead::Log(LogAddress(128)));
    drop(guard);
    epoch.unregister(&participant).unwrap();
}

#[test]
fn binding_and_reclamation_require_the_same_live_manager() {
    let epoch = EpochManager::new().unwrap();
    let other = EpochManager::new().unwrap();
    let mut index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
    let participant = epoch.register().unwrap();
    let guard = epoch.enter(&participant).unwrap();
    assert!(index.prepare_guarded(KeyHash(0), &guard).is_err());
    index.bind_epoch(&epoch).unwrap();
    assert!(index.bind_epoch(&other).is_err());
    assert!(index.validate_epoch(&other).is_err());
    index.validate_epoch(&epoch).unwrap();
    assert!(index.prepare_guarded(KeyHash(0), &guard).is_ok());
    assert!(
        std::panic::catch_unwind(|| {
            epoch
                .retain_unlinked::<()>(&[], || panic!("injected epoch poison"))
                .unwrap();
        })
        .is_err()
    );
    assert!(index.prepare_guarded(KeyHash(0), &guard).is_err());
}
