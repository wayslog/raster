//! Shared in-place updates exclude structural publication on the same stripe.
use crate::types::Error;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError, TryLockResult};

#[derive(Default)]
pub(crate) struct OperationGate(RwLock<()>);
impl OperationGate {
    pub fn try_lock(&self) -> TryLockResult<RwLockWriteGuard<'_, ()>> {
        self.0.try_write()
    }
    pub fn is_poisoned(&self) -> bool {
        self.0.is_poisoned()
    }
    #[cfg(test)]
    pub fn lock(&self) -> std::sync::LockResult<RwLockWriteGuard<'_, ()>> {
        self.0.write()
    }
    pub(super) fn initial(&self, shared: bool) -> Result<InitialPermit<'_>, Error> {
        let held = if shared {
            bounded(|| self.0.try_read())
                .map(|guard| Held::Shared { _guard: guard })
                .map_err(reason)?
        } else {
            try_lock(self)
                .map(|guard| Held::Structural { _guard: guard })
                .map_err(reason)?
        };
        Ok(InitialPermit {
            gate: self,
            held: Some(held),
        })
    }
}
enum Held<'a> {
    Shared { _guard: RwLockReadGuard<'a, ()> },
    Structural { _guard: RwLockWriteGuard<'a, ()> },
}
pub(super) struct InitialPermit<'a> {
    gate: &'a OperationGate,
    held: Option<Held<'a>>,
}
impl InitialPermit<'_> {
    pub fn is_shared(&self) -> bool {
        matches!(self.held, Some(Held::Shared { .. }))
    }
    pub fn make_structural(&mut self) -> Result<(), Error> {
        // Never upgrade while holding a shared permit. The caller must resolve
        // the head and source again after this unprotected interval.
        self.held.take();
        self.held = Some(Held::Structural {
            _guard: try_lock(self.gate).map_err(reason)?,
        });
        Ok(())
    }
}
pub(super) enum WriteAttempt<T> {
    Ready(T),
    Retry,
    Structural,
}
fn reason<T>(error: TryLockError<T>) -> Error {
    match error {
        TryLockError::WouldBlock => Error::Busy,
        TryLockError::Poisoned(_) => Error::InvalidState("Operation arbitration lock poisoning"),
    }
}
#[inline]
fn bounded<T>(mut attempt: impl FnMut() -> TryLockResult<T>) -> TryLockResult<T> {
    match attempt() {
        Err(TryLockError::WouldBlock) => {}
        result => return result,
    }
    // Retain the existing bounded admission policy.
    for _ in 0..6 {
        std::hint::spin_loop();
        match attempt() {
            Err(TryLockError::WouldBlock) => {}
            result => return result,
        }
    }
    Err(TryLockError::WouldBlock)
}
#[inline]
pub(super) fn try_lock(gate: &OperationGate) -> TryLockResult<RwLockWriteGuard<'_, ()>> {
    bounded(|| gate.try_lock())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contention_remains_bounded_and_does_not_release_another_owner() {
        let gate = OperationGate::default();
        let owner = gate.lock().unwrap();
        assert!(matches!(try_lock(&gate), Err(TryLockError::WouldBlock)));
        assert!(matches!(gate.try_lock(), Err(TryLockError::WouldBlock)));
        drop(owner);
        drop(try_lock(&gate).unwrap());
    }
    #[test]
    fn poisoning_is_propagated_instead_of_retried_as_contention() {
        let gate = OperationGate::default();
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
