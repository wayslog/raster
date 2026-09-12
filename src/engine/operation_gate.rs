//! Bounded admission contention retries; no user callbacks or serial acceptance.
use std::sync::{Mutex, MutexGuard, TryLockError, TryLockResult};

#[inline]
pub(super) fn try_lock(gate: &Mutex<()>) -> TryLockResult<MutexGuard<'_, ()>> {
    match gate.try_lock() {
        Err(TryLockError::WouldBlock) => {}
        result => return result,
    }
    // Six retries and 63 processor hints at most. Long-running callbacks still
    // produce a rejection; never park a submitting thread behind an owner.
    for step in 0..6 {
        for _ in 0..(1 << step) {
            std::hint::spin_loop();
        }
        match gate.try_lock() {
            Err(TryLockError::WouldBlock) => {}
            result => return result,
        }
    }
    Err(TryLockError::WouldBlock)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contention_remains_bounded_and_does_not_release_another_owner() {
        let gate = Mutex::new(());
        let owner = gate.lock().unwrap();
        assert!(matches!(try_lock(&gate), Err(TryLockError::WouldBlock)));
        assert!(matches!(gate.try_lock(), Err(TryLockError::WouldBlock)));
        drop(owner);
        drop(try_lock(&gate).unwrap());
    }
    #[test]
    fn poisoning_is_propagated_instead_of_retried_as_contention() {
        let gate = Mutex::new(());
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = gate.lock().unwrap();
                panic!("injected operation lock poison");
            })
            .is_err()
        );
        assert!(matches!(try_lock(&gate), Err(TryLockError::Poisoned(_))));
    }
}
