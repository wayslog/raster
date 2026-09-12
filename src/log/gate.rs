//! Atomic updates are shareable,Replacement and normal byte access must be exclusive;All operations attempt only one state transition.
use crate::{
    sync::{AtomicU64, PUBLISH_ORDER},
    types::Error,
};
use std::{marker::PhantomData, rc::Rc};
const EXCLUSIVE: u64 = u64::MAX;

pub(crate) struct MutationGate {
    state: AtomicU64,
}
impl Default for MutationGate {
    fn default() -> Self {
        Self {
            state: AtomicU64::new(0),
        }
    }
}
pub(crate) struct ReplacementPermit<'a> {
    gate: &'a MutationGate,
    local: PhantomData<Rc<()>>,
}
pub(crate) struct SharedUpdatePermit<'a> {
    gate: &'a MutationGate,
    local: PhantomData<Rc<()>>,
}
impl MutationGate {
    /// Only allow atomic layouts that can be accessed safely concurrently;Do not grant mutable references to ordinary values.
    pub fn try_update(&self) -> Result<SharedUpdatePermit<'_>, Error> {
        let state = self.state.load(PUBLISH_ORDER);
        if state >= EXCLUSIVE - 1 {
            return Err(Error::Busy);
        }
        self.state
            .compare_exchange(state, state + 1, PUBLISH_ORDER, PUBLISH_ORDER)
            .map_err(|_| Error::Busy)?;
        Ok(SharedUpdatePermit {
            gate: self,
            local: PhantomData,
        })
    }
    /// Upgrading with a shared license is not supported;Read ordinary bytes,Use this entrance to write ordinary values and replace them..
    pub fn try_replace(&self) -> Result<ReplacementPermit<'_>, Error> {
        self.state
            .compare_exchange(0, EXCLUSIVE, PUBLISH_ORDER, PUBLISH_ORDER)
            .map_err(|_| Error::Busy)?;
        Ok(ReplacementPermit {
            gate: self,
            local: PhantomData,
        })
    }
}
impl Drop for SharedUpdatePermit<'_> {
    fn drop(&mut self) {
        self.gate.state.fetch_sub(1, PUBLISH_ORDER);
    }
}
impl Drop for ReplacementPermit<'_> {
    fn drop(&mut self) {
        self.gate.state.store(0, PUBLISH_ORDER);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_updates_are_mutually_compatible_but_mutually_exclusive_with_replace() {
        let gate = MutationGate::default();
        let a = gate.try_update().unwrap();
        let b = gate.try_update().unwrap();
        assert!(gate.try_replace().is_err());
        drop(a);
        assert!(gate.try_replace().is_err());
        drop(b);
        let exclusive = gate.try_replace().unwrap();
        assert!(gate.try_replace().is_err());
        assert!(gate.try_update().is_err());
        drop(exclusive);
        assert!(gate.try_replace().is_ok());
    }
    #[test]
    fn panic_unfolding_release_permission_and_forgetting_will_not_open_arbitration() {
        let gate = MutationGate::default();
        let result = std::panic::catch_unwind(|| {
            let _p = gate.try_replace().unwrap();
            panic!("Simulate user panic");
        });
        assert!(result.is_err());
        assert!(gate.try_update().is_ok());
        std::mem::forget(gate.try_update().unwrap());
        assert!(gate.try_replace().is_err());
        // Only the arbitration license release is verified here;The impact of business errors and engine failure are still caused by P3 realize.
    }
    #[test]
    fn shared_updates_and_replacements_of_real_threads_cannot_overlap() {
        let gate = MutationGate::default();
        let entered = std::sync::Barrier::new(2);
        let exit = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let _p = gate.try_update().unwrap();
                entered.wait();
                exit.wait();
            });
            entered.wait();
            assert!(gate.try_replace().is_err());
            let second = gate.try_update().unwrap();
            drop(second);
            exit.wait();
        });
        assert!(gate.try_replace().is_ok());
    }
    #[test]
    fn update_count_cap_cannot_be_turned_into_an_exclusive_tag() {
        let gate = MutationGate {
            state: AtomicU64::new(EXCLUSIVE - 1),
        };
        assert!(gate.try_update().is_err());
        assert_eq!(gate.state.load(PUBLISH_ORDER), EXCLUSIVE - 1);
    }
}
