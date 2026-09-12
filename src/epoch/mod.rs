//! Stable participant slots protect access without locking the global registry.
//! Retirement and collection remain serialized; reclamation runs outside locks.
use crate::{sync::Mutex, types::*};
use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[derive(Clone, Debug)]
pub(crate) struct ParticipantId {
    owner: u64,
    slot: usize,
    generation: Generation,
    entry: Arc<ParticipantSlot>,
}
pub(crate) struct EpochGuard<'a> {
    manager: &'a EpochManager,
    participant: &'a ParticipantId,
    local: PhantomData<Rc<()>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeferredAction {
    ReleaseIndex(Generation),
}
#[derive(Debug)]
struct Slot {
    generation: Generation,
    registered: bool,
    active: usize,
    epoch: EpochVersion,
}
#[repr(align(64))]
#[derive(Debug)]
struct ParticipantSlot {
    state: Mutex<Slot>,
}
struct PoisonNotification<'a> {
    state: &'a Mutex<Slot>,
    failed: &'a AtomicBool,
}
impl Drop for PoisonNotification<'_> {
    fn drop(&mut self) {
        if self.state.is_poisoned() {
            self.failed.store(true, Ordering::SeqCst);
        }
    }
}
impl ParticipantSlot {
    fn with_state<R>(
        &self,
        failed: &AtomicBool,
        use_state: impl FnOnce(&mut Slot) -> Result<R, Error>,
    ) -> Result<R, Error> {
        // Drop the real guard first so Mutex itself distinguishes a new panic
        // from cleanup performed during an already active unwind.
        let _notification = PoisonNotification {
            state: &self.state,
            failed,
        };
        let mut state = self.state.lock().map_err(|_| {
            failed.store(true, Ordering::SeqCst);
            Error::InvalidState("epoch participant lock poisoned")
        })?;
        if failed.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("epoch manager failed closed"));
        }
        use_state(&mut state)
    }
}
struct State {
    current: EpochVersion,
    slots: Vec<Arc<ParticipantSlot>>,
    deferred: Vec<(EpochVersion, DeferredAction)>,
}
static NEXT_MANAGER: crate::sync::AtomicU64 = crate::sync::AtomicU64::new(0);
pub(crate) struct EpochManager {
    owner: u64,
    current: AtomicU64,
    failed: AtomicBool,
    state: Mutex<State>,
}
impl EpochManager {
    pub fn new() -> Result<Self, Error> {
        let owner = NEXT_MANAGER
            .fetch_update(
                crate::sync::PUBLISH_ORDER,
                crate::sync::PUBLISH_ORDER,
                |n| n.checked_add(1),
            )
            .map_err(|_| Error::CapacityExceeded)?;
        Ok(Self {
            owner,
            current: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            state: Mutex::new(State {
                current: EpochVersion(0),
                slots: Vec::new(),
                deferred: Vec::new(),
            }),
        })
    }
}
impl EpochManager {
    fn healthy(&self) -> Result<(), Error> {
        if self.failed.load(Ordering::SeqCst) || self.state.is_poisoned() {
            return Err(Error::InvalidState("epoch manager failed closed"));
        }
        Ok(())
    }
    fn lock(&self) -> Result<crate::sync::MutexGuard<'_, State>, Error> {
        self.healthy()?;
        self.state
            .lock()
            .map_err(|_| Error::InvalidState("epoch Control lock poisoning"))
    }
    pub fn register(&self) -> Result<ParticipantId, Error> {
        let mut state = self.lock()?;
        for (slot, entry) in state.slots.iter().enumerate() {
            let generation = entry.with_state(&self.failed, |entry| {
                if !entry.registered && entry.generation.0 < u64::MAX {
                    entry.generation.0 += 1;
                    entry.registered = true;
                    Ok(Some(entry.generation))
                } else {
                    Ok(None)
                }
            })?;
            if let Some(generation) = generation {
                return Ok(ParticipantId {
                    owner: self.owner,
                    slot,
                    generation,
                    entry: entry.clone(),
                });
            }
        }
        state.slots.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        let entry = Arc::new(ParticipantSlot {
            state: Mutex::new(Slot {
                generation: Generation(0),
                registered: true,
                active: 0,
                epoch: EpochVersion(0),
            }),
        });
        let id = ParticipantId {
            owner: self.owner,
            slot: state.slots.len(),
            generation: Generation(0),
            entry: entry.clone(),
        };
        state.slots.push(entry);
        Ok(id)
    }
    fn with_slot<R>(
        &self,
        id: &ParticipantId,
        use_slot: impl FnOnce(&mut Slot) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if id.owner != self.owner {
            return Err(Error::InvalidState("Participants belong to other managers"));
        }
        self.healthy()?;
        id.entry.with_state(&self.failed, |slot| {
            self.healthy()?;
            if !slot.registered || slot.generation != id.generation {
                return Err(Error::InvalidState("Participant generation expired"));
            }
            use_slot(slot)
        })
    }
    pub fn enter<'a>(&'a self, id: &'a ParticipantId) -> Result<EpochGuard<'a>, Error> {
        self.with_slot(id, |slot| {
            let active = slot.active.checked_add(1).ok_or(Error::CapacityExceeded)?;
            if slot.active == 0 {
                slot.epoch = EpochVersion(self.current.load(Ordering::SeqCst));
            }
            slot.active = active;
            Ok(())
        })?;
        Ok(EpochGuard {
            manager: self,
            participant: id,
            local: PhantomData,
        })
    }
    /// The caller first removes the object from the visible structure,Queue again;It is not allowed to pass in old data by yourself epoch.
    pub fn defer(&self, action: DeferredAction) -> Result<(), Error> {
        let mut state = self.lock()?;
        let current = state.current;
        state
            .deferred
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        state.deferred.push((current, action));
        Ok(())
    }
    pub fn advance(&self) -> Result<EpochVersion, Error> {
        let mut state = self.lock()?;
        state.current = EpochVersion(
            state
                .current
                .0
                .checked_add(1)
                .ok_or(Error::CapacityExceeded)?,
        );
        self.current.store(state.current.0, Ordering::SeqCst);
        Ok(state.current)
    }
    /// Only deliver actions that have crossed the safety boundary,The actual destruction is performed outside the control lock.
    pub fn collect(&self) -> Result<Vec<DeferredAction>, Error> {
        let mut state = self.lock()?;
        // A reader that could observe a retired object entered before it was
        // unlinked. Its slot retains that epoch until its last nested guard exits.
        // Scanning slots individually may retain extra actions, but cannot miss
        // such a still-active reader. New readers must acquire objects only after
        // entering, and cannot obtain objects already removed from visibility.
        let mut oldest: Option<EpochVersion> = None;
        for entry in &state.slots {
            entry.with_state(&self.failed, |slot| {
                if slot.active != 0 && oldest.is_none_or(|old| slot.epoch.0 < old.0) {
                    oldest = Some(slot.epoch);
                }
                Ok(())
            })?;
        }
        self.healthy()?;
        let ready = |epoch: EpochVersion| oldest.is_none_or(|e| epoch.0 < e.0);
        let mut actions = Vec::new();
        actions
            .try_reserve_exact(state.deferred.iter().filter(|(e, _)| ready(*e)).count())
            .map_err(|_| Error::OutOfMemory)?;
        state.deferred.retain(|(epoch, action)| {
            if ready(*epoch) {
                actions.push(*action);
                false
            } else {
                true
            }
        });
        Ok(actions)
    }
    pub fn unregister(&self, id: &ParticipantId) -> Result<(), Error> {
        if id.owner != self.owner {
            return Err(Error::InvalidState("Participants belong to other managers"));
        }
        let state = self.lock()?;
        if !state
            .slots
            .get(id.slot)
            .is_some_and(|entry| Arc::ptr_eq(entry, &id.entry))
        {
            return Err(Error::InvalidState("Participant slot does not exist"));
        }
        self.with_slot(id, |slot| {
            if slot.active != 0 {
                return Err(Error::Busy);
            }
            slot.registered = false;
            Ok(())
        })
    }
}
impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        // Poisoning conservatively retains counts and prevents all reclamation.
        let _ = self.manager.with_slot(self.participant, |slot| {
            slot.active -= 1;
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const ACTION: DeferredAction = DeferredAction::ReleaseIndex(Generation(4));
    #[test]
    fn readers_in_distinct_slots_release_only_the_retirements_older_than_their_epochs() {
        let manager = EpochManager::new().unwrap();
        // Scan the newer reader first, so registry order differs from age order.
        let newer = manager.register().unwrap();
        let older = manager.register().unwrap();
        let old_ready = std::sync::Barrier::new(2);
        let old_exit = std::sync::Barrier::new(2);
        let new_ready = std::sync::Barrier::new(2);
        let new_exit = std::sync::Barrier::new(2);
        let later = DeferredAction::ReleaseIndex(Generation(5));
        let observations = std::thread::scope(|scope| {
            let old = scope.spawn(|| {
                let guard = manager.enter(&older).unwrap();
                old_ready.wait();
                old_exit.wait();
                drop(guard);
            });
            old_ready.wait();
            manager.defer(ACTION).unwrap();
            manager.advance().unwrap();
            let new = scope.spawn(|| {
                let guard = manager.enter(&newer).unwrap();
                new_ready.wait();
                new_exit.wait();
                drop(guard);
            });
            new_ready.wait();
            manager.defer(later).unwrap();
            manager.advance().unwrap();
            let both = manager.collect();
            old_exit.wait();
            old.join().unwrap();
            let only_new = manager.collect();
            new_exit.wait();
            new.join().unwrap();
            (both, only_new, manager.collect())
        });
        assert!(observations.0.unwrap().is_empty());
        assert_eq!(observations.1.unwrap(), vec![ACTION]);
        assert_eq!(observations.2.unwrap(), vec![later]);
        manager.unregister(&older).unwrap();
        manager.unregister(&newer).unwrap();
    }
    #[test]
    fn registry_poisoning_disables_cached_participants_and_retains_active_counts() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        let guard = manager.enter(&id).unwrap();
        assert!(
            std::panic::catch_unwind(|| {
                let _registry = manager.lock().unwrap();
                panic!("injected registry poison");
            })
            .is_err()
        );
        assert!(manager.enter(&id).is_err());
        drop(guard);
        assert_eq!(id.entry.state.lock().unwrap().active, 1);
        assert!(manager.collect().is_err());
    }
    #[test]
    fn poisoning_a_participant_closes_other_participants_and_reclamation() {
        let manager = EpochManager::new().unwrap();
        let a = manager.register().unwrap();
        let b = manager.register().unwrap();
        let guard = manager.enter(&b).unwrap();
        manager.defer(ACTION).unwrap();
        assert!(
            std::panic::catch_unwind(|| {
                let _ = manager.with_slot::<()>(&a, |_| panic!("injected participant poison"));
            })
            .is_err()
        );
        assert!(manager.enter(&b).is_err());
        assert!(manager.register().is_err());
        assert!(manager.collect().is_err());
        drop(guard);
        assert_eq!(b.entry.state.lock().unwrap().active, 1);
        assert!(manager.unregister(&b).is_err());
    }
    #[test]
    fn existing_unwind_releases_its_guard_without_poisoning_the_manager() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        manager.defer(ACTION).unwrap();
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = manager.enter(&id).unwrap();
                panic!("unrelated client panic");
            })
            .is_err()
        );
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        manager.unregister(&id).unwrap();
    }
    #[test]
    fn exhausted_activity_is_rejected_without_changing_the_registered_epoch() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        manager
            .with_slot(&id, |slot| {
                slot.active = usize::MAX;
                slot.epoch = EpochVersion(0);
                Ok(())
            })
            .unwrap();
        manager.advance().unwrap();
        assert!(matches!(manager.enter(&id), Err(Error::CapacityExceeded)));
        manager
            .with_slot(&id, |slot| {
                assert_eq!(slot.active, usize::MAX);
                assert_eq!(slot.epoch, EpochVersion(0));
                slot.active = 0;
                Ok(())
            })
            .unwrap();
        manager.unregister(&id).unwrap();
    }
    #[test]
    fn stable_handles_survive_registry_growth_but_not_reused_generations() {
        let manager = EpochManager::new().unwrap();
        let old = manager.register().unwrap();
        let duplicate = old.clone();
        let more: Vec<_> = (0..512).map(|_| manager.register().unwrap()).collect();
        let first = manager.enter(&old).unwrap();
        let second = manager.enter(&duplicate).unwrap();
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        drop(first);
        assert!(manager.collect().unwrap().is_empty());
        drop(second);
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        manager.unregister(&old).unwrap();
        let replacement = manager.register().unwrap();
        assert_eq!(replacement.slot, old.slot);
        assert!(manager.enter(&duplicate).is_err());
        drop(manager.enter(&replacement).unwrap());
        for id in &more {
            manager.unregister(id).unwrap();
        }
        manager.unregister(&replacement).unwrap();
    }
    #[test]
    fn registered_participants_can_enter_without_waiting_for_registry_inspection() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        let registry = manager.lock().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let entered = std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let guard = manager.enter(&id).unwrap();
                sent.send(()).unwrap();
                drop(guard);
            });
            let entered = received
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok();
            // Always release the inspection lock before joining, including on failure.
            drop(registry);
            worker.join().unwrap();
            entered
        });
        assert!(
            entered,
            "a registered participant waited for the global registry lock"
        );
        manager.unregister(&id).unwrap();
    }
    #[test]
    fn the_action_is_delivered_after_the_managers_identity_is_isolated_and_the_real_thread_exits() {
        let manager = EpochManager::new().unwrap();
        let other = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        let _other_id = other.register().unwrap();
        assert!(other.enter(&id).is_err());
        assert!(other.unregister(&id).is_err());
        let entered = std::sync::Barrier::new(2);
        let leave = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let guard = manager.enter(&id).unwrap();
                entered.wait();
                leave.wait();
                drop(guard);
            });
            entered.wait();
            manager.defer(ACTION).unwrap();
            manager.advance().unwrap();
            assert!(manager.collect().unwrap().is_empty());
            leave.wait();
        });
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        manager.unregister(&id).unwrap();
    }

    #[test]
    fn slowest_readers_and_nested_permissions_are_not_recycled_before_exiting() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        let first = manager.enter(&id).unwrap();
        manager.advance().unwrap();
        let second = manager.enter(&id).unwrap();
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        assert!(manager.collect().unwrap().is_empty());
        drop(first);
        assert!(manager.collect().unwrap().is_empty());
        assert!(manager.unregister(&id).is_err());
        drop(second);
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        assert!(manager.collect().unwrap().is_empty());
        manager.unregister(&id).unwrap();
    }
    #[test]
    fn new_readers_do_not_block_old_objects_and_old_slots_cannot_be_reused() {
        let manager = EpochManager::new().unwrap();
        let old = manager.register().unwrap();
        manager.unregister(&old).unwrap();
        let new = manager.register().unwrap();
        assert_eq!(old.slot, new.slot);
        assert_ne!(old.generation, new.generation);
        assert!(manager.enter(&old).is_err());
        assert!(manager.unregister(&old).is_err());
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        let guard = manager.enter(&new).unwrap();
        assert_eq!(manager.collect().unwrap(), vec![ACTION]);
        drop(guard);
    }
    #[test]
    fn the_forgetting_permission_only_blocks_recycling_and_does_not_wrap_around_generations() {
        let manager = EpochManager::new().unwrap();
        let id = manager.register().unwrap();
        std::mem::forget(manager.enter(&id).unwrap());
        manager.defer(ACTION).unwrap();
        manager.advance().unwrap();
        assert!(manager.collect().unwrap().is_empty());
        assert!(manager.unregister(&id).is_err());
        let other = manager.register().unwrap();
        manager.unregister(&other).unwrap();
        manager.lock().unwrap().slots[other.slot]
            .state
            .lock()
            .unwrap()
            .generation = Generation(u64::MAX);
        assert_ne!(manager.register().unwrap().slot, other.slot);
        manager.lock().unwrap().current = EpochVersion(u64::MAX);
        manager.current.store(u64::MAX, Ordering::SeqCst);
        assert!(manager.advance().is_err());
        assert!(manager.collect().unwrap().is_empty());
    }
    #[test]
    fn exhaustive_linearized_interleaving_of_two_actors_and_recyclers() {
        // Enumerate whole-operation interleavings through the real manager.
        // Separate concurrent tests cover registry/slot overlap and reader exit.
        fn schedules(prefix: &mut Vec<usize>, left: &mut [usize; 3], all: &mut Vec<Vec<usize>>) {
            if left.iter().all(|n| *n == 0) {
                all.push(prefix.clone());
                return;
            }
            for i in 0..3 {
                if left[i] != 0 {
                    left[i] -= 1;
                    prefix.push(i);
                    schedules(prefix, left, all);
                    prefix.pop();
                    left[i] += 1;
                }
            }
        }
        let mut all = Vec::new();
        schedules(&mut Vec::new(), &mut [2, 2, 3], &mut all);
        assert_eq!(all.len(), 210);
        for sequence in all {
            let manager = EpochManager::new().unwrap();
            let ids = [manager.register().unwrap(), manager.register().unwrap()];
            let mut guards = [None, None];
            let mut steps = [0; 3];
            let mut readers_at_retirement = [false; 2];
            let mut retired = false;
            let mut delivered = 0;
            for actor in sequence {
                if actor < 2 {
                    if steps[actor] == 0 {
                        guards[actor] = Some(manager.enter(&ids[actor]).unwrap());
                    } else {
                        guards[actor].take();
                        readers_at_retirement[actor] = false;
                    }
                } else {
                    match steps[actor] {
                        0 => {
                            manager.defer(ACTION).unwrap();
                            retired = true;
                            readers_at_retirement = [guards[0].is_some(), guards[1].is_some()];
                        }
                        1 => {
                            manager.advance().unwrap();
                        }
                        _ => {}
                    }
                }
                steps[actor] += 1;
                let ready = manager.collect().unwrap();
                if !ready.is_empty() {
                    assert!(retired);
                    assert!(!readers_at_retirement.iter().any(|x| *x));
                    delivered += ready.len();
                }
            }
            delivered += manager.collect().unwrap().len();
            assert_eq!(delivered, 1);
        }
    }
}
