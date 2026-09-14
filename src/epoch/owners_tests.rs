use super::*;
#[test]
fn owned_retirements_wait_for_old_readers_without_consuming_index_actions() {
    let manager = EpochManager::new().unwrap();
    let first = manager.register().unwrap();
    let second = manager.register().unwrap();
    let guard = manager.enter_records(&first).unwrap();
    let value = Arc::new(42u64);
    let weak = Arc::downgrade(&value);
    manager
        .defer(DeferredAction::ReleaseIndex(Generation(9)))
        .unwrap();
    manager
        .retain_unlinked(std::slice::from_ref(&value), || {})
        .unwrap();
    drop(value);
    let newer = manager.enter_records(&second).unwrap();
    assert_eq!(manager.collect_owners().unwrap(), 0);
    assert!(manager.collect().unwrap().is_empty());
    assert!(weak.upgrade().is_some());
    drop(guard);
    assert_eq!(manager.collect_owners().unwrap(), 1);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        manager.collect().unwrap(),
        [DeferredAction::ReleaseIndex(Generation(9))]
    );
    drop(newer);
    manager.unregister(&first).unwrap();
    manager.unregister(&second).unwrap();
}
#[test]
fn retirement_overflow_leaves_visibility_and_ownership_untouched() {
    let manager = EpochManager::new().unwrap();
    manager.state.lock().unwrap().current = EpochVersion(u64::MAX);
    manager.current.store(u64::MAX, Ordering::SeqCst);
    let value = Arc::new(42u64);
    let mut unlinked = false;
    assert!(matches!(
        manager.retain_unlinked(std::slice::from_ref(&value), || unlinked = true),
        Err(Error::CapacityExceeded)
    ));
    assert!(!unlinked);
    assert_eq!(Arc::strong_count(&value), 1);
    assert!(!manager.retained_pending.load(Ordering::Acquire));
}
#[test]
fn failed_unlink_retains_owners_and_prevents_collection() {
    let manager = EpochManager::new().unwrap();
    let value = Arc::new(42u64);
    let weak = Arc::downgrade(&value);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            manager
                .retain_unlinked(std::slice::from_ref(&value), || {
                    panic!("injected unlink panic")
                })
                .unwrap();
        }))
        .is_err()
    );
    drop(value);
    assert!(manager.collect_owners().is_err());
    assert!(weak.upgrade().is_some());
    drop(manager);
    assert!(weak.upgrade().is_none());
}
#[test]
fn owner_destructors_run_outside_the_epoch_control_lock() {
    struct Owner {
        manager: std::sync::Weak<EpochManager>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            let manager = self.manager.upgrade().unwrap();
            assert!(manager.state.try_lock().is_ok());
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    let manager = Arc::new(EpochManager::new().unwrap());
    let dropped = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(Owner {
        manager: Arc::downgrade(&manager),
        dropped: dropped.clone(),
    });
    manager
        .retain_unlinked(std::slice::from_ref(&owner), || {})
        .unwrap();
    drop(owner);
    assert_eq!(manager.collect_owners().unwrap(), 1);
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn index_only_scopes_cannot_borrow_records_or_delay_record_collection() {
    let manager = EpochManager::new().unwrap();
    let participant = manager.register().unwrap();
    let guard = manager.enter(&participant).unwrap();
    assert!(guard.protects(&manager).is_err());
    let value = Arc::new(42u64);
    let weak = Arc::downgrade(&value);
    manager
        .defer(DeferredAction::ReleaseIndex(Generation(9)))
        .unwrap();
    manager
        .retain_unlinked(std::slice::from_ref(&value), || {})
        .unwrap();
    drop(value);
    assert_eq!(manager.collect_owners().unwrap(), 1);
    assert!(weak.upgrade().is_none());
    assert!(manager.collect().unwrap().is_empty());
    drop(guard);
    assert_eq!(
        manager.collect().unwrap(),
        [DeferredAction::ReleaseIndex(Generation(9))]
    );
    manager.unregister(&participant).unwrap();
}
