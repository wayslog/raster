//! Light-epoch protection (FASTER `light_epoch.h`).
//!
//! Sessions protect once and advance with `refresh`; operations never enter or
//! leave the epoch. Deferred actions run once every protected slot has moved
//! past the epoch in which they were registered.
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

pub const MAX_SLOTS: usize = 128;
const DRAIN_SLOTS: usize = 256;
const UNPROTECTED: u64 = 0;
const FREE: u64 = u64::MAX;
const LOCKED: u64 = u64::MAX - 1;
const NO_PHASE: u8 = u8::MAX;

type Action = Box<dyn FnOnce() + Send>;

#[repr(align(128))]
struct Slot {
    local_epoch: AtomicU64,
    used: AtomicBool,
    /// Last state-machine phase this slot's session finished.
    phase_finished: AtomicU8,
    /// Sessions take part in phase barriers; internal helpers do not.
    session: AtomicBool,
}

#[repr(align(64))]
struct DrainEntry {
    epoch: AtomicU64,
    action: UnsafeCell<Option<Action>>,
}

#[repr(align(128))]
struct Padded<T>(T);

pub struct LightEpoch {
    current: Padded<AtomicU64>,
    safe_to_reclaim: Padded<AtomicU64>,
    drain_count: Padded<AtomicU32>,
    slots: Box<[Slot]>,
    drain: Box<[DrainEntry]>,
}

// SAFETY: `DrainEntry::action` is only accessed by the thread that moved the
// entry's `epoch` word to `LOCKED`, which grants exclusive access.
unsafe impl Sync for LightEpoch {}
// SAFETY: actions are `Send`; all other fields are atomics.
unsafe impl Send for LightEpoch {}

/// Index of a reserved epoch slot, owned by one session.
#[derive(Debug)]
pub struct SlotId(usize);

impl Default for LightEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl LightEpoch {
    pub fn new() -> Self {
        Self {
            current: Padded(AtomicU64::new(1)),
            safe_to_reclaim: Padded(AtomicU64::new(0)),
            drain_count: Padded(AtomicU32::new(0)),
            slots: (0..MAX_SLOTS)
                .map(|_| Slot {
                    local_epoch: AtomicU64::new(UNPROTECTED),
                    used: AtomicBool::new(false),
                    phase_finished: AtomicU8::new(NO_PHASE),
                    session: AtomicBool::new(false),
                })
                .collect(),
            drain: (0..DRAIN_SLOTS)
                .map(|_| DrainEntry {
                    epoch: AtomicU64::new(FREE),
                    action: UnsafeCell::new(None),
                })
                .collect(),
        }
    }

    /// Reserves a slot for a session, which takes part in phase barriers.
    pub fn reserve(&self) -> Option<SlotId> {
        self.reserve_slot(true)
    }

    /// Reserves a slot for an internal helper (e.g. a log scanner) that only
    /// needs memory protection and never acknowledges state-machine phases.
    pub fn reserve_helper(&self) -> Option<SlotId> {
        self.reserve_slot(false)
    }

    /// A point-in-time count of user sessions, excluding internal helpers.
    pub fn num_active_sessions(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| {
                slot.used.load(Ordering::Acquire) && slot.session.load(Ordering::Acquire)
            })
            .count()
    }

    fn reserve_slot(&self, session: bool) -> Option<SlotId> {
        let index = self.slots.iter().position(|slot| {
            slot.used
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        })?;
        self.slots[index].session.store(session, Ordering::Release);
        Some(SlotId(index))
    }

    pub fn release(&self, id: SlotId) {
        self.unprotect(&id);
        self.slots[id.0]
            .phase_finished
            .store(NO_PHASE, Ordering::Release);
        self.slots[id.0].used.store(false, Ordering::Release);
    }

    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.current.0.load(Ordering::Acquire)
    }

    #[inline]
    pub fn safe_to_reclaim(&self) -> u64 {
        self.safe_to_reclaim.0.load(Ordering::Acquire)
    }

    /// Publishes the current epoch for this slot. The SeqCst store orders the
    /// publication before any later load of protected boundaries.
    #[inline]
    pub fn protect(&self, id: &SlotId) -> u64 {
        let epoch = self.current_epoch();
        self.slots[id.0].local_epoch.store(epoch, Ordering::SeqCst);
        epoch
    }

    #[inline]
    pub fn protect_and_drain(&self, id: &SlotId) -> u64 {
        let epoch = self.protect(id);
        if self.drain_count.0.load(Ordering::Acquire) > 0 {
            self.drain(epoch);
        }
        epoch
    }

    #[inline]
    pub fn unprotect(&self, id: &SlotId) {
        self.slots[id.0]
            .local_epoch
            .store(UNPROTECTED, Ordering::Release);
    }

    #[inline]
    pub fn is_protected(&self, id: &SlotId) -> bool {
        self.slots[id.0].local_epoch.load(Ordering::Relaxed) != UNPROTECTED
    }

    pub fn bump_current_epoch(&self) -> u64 {
        let next = self.current.0.fetch_add(1, Ordering::SeqCst) + 1;
        if self.drain_count.0.load(Ordering::Acquire) > 0 {
            self.drain(next);
        }
        next
    }

    /// Bumps the epoch and runs `action` once every slot has left the prior epoch.
    pub fn bump_with_action(&self, action: impl FnOnce() + Send + 'static) -> u64 {
        let next = self.current.0.fetch_add(1, Ordering::SeqCst) + 1;
        let prior = next - 1;
        let mut action: Option<Action> = Some(Box::new(action));
        let mut attempts = 0u32;
        while action.is_some() {
            for entry in self.drain.iter() {
                let epoch = entry.epoch.load(Ordering::Acquire);
                if epoch == FREE
                    && entry
                        .epoch
                        .compare_exchange(FREE, LOCKED, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    // SAFETY: this thread holds the entry lock.
                    unsafe { *entry.action.get() = action.take() };
                    self.drain_count.0.fetch_add(1, Ordering::AcqRel);
                    entry.epoch.store(prior, Ordering::Release);
                    break;
                }
                if epoch <= prior {
                    self.drain(next);
                }
            }
            attempts += 1;
            if action.is_some() && attempts > 500 {
                std::thread::yield_now();
            }
        }
        self.drain(next);
        next
    }

    /// Runs every registered action regardless of epochs. Only for teardown,
    /// when no session can still be protected.
    pub fn drain_all(&self) {
        for entry in self.drain.iter() {
            let epoch = entry.epoch.load(Ordering::Acquire);
            if epoch < LOCKED
                && entry
                    .epoch
                    .compare_exchange(epoch, LOCKED, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                // SAFETY: this thread holds the entry lock.
                let action = unsafe { (*entry.action.get()).take() };
                entry.epoch.store(FREE, Ordering::Release);
                self.drain_count.0.fetch_sub(1, Ordering::AcqRel);
                if let Some(action) = action {
                    action();
                }
            }
        }
    }

    /// Forgets every slot's finished phase before a new state-machine run.
    pub fn reset_phase_finished(&self) {
        for slot in self.slots.iter() {
            slot.phase_finished.store(NO_PHASE, Ordering::Release);
        }
    }

    /// Marks `phase` finished for this slot; returns true if every protected
    /// slot has finished it, so the caller should advance the global state.
    pub fn finish_thread_phase(&self, id: &SlotId, phase: u8) -> bool {
        self.slots[id.0]
            .phase_finished
            .store(phase, Ordering::SeqCst);
        self.slots.iter().all(|slot| {
            !slot.session.load(Ordering::Acquire)
                || slot.local_epoch.load(Ordering::SeqCst) == UNPROTECTED
                || slot.phase_finished.load(Ordering::SeqCst) == phase
        })
    }

    pub fn has_thread_finished_phase(&self, id: &SlotId, phase: u8) -> bool {
        self.slots[id.0].phase_finished.load(Ordering::Acquire) == phase
    }

    /// Recomputes the safe-to-reclaim epoch from all protected slots.
    pub fn compute_safe_to_reclaim(&self, current: u64) -> u64 {
        let oldest = self
            .slots
            .iter()
            .map(|slot| slot.local_epoch.load(Ordering::SeqCst))
            .filter(|&epoch| epoch != UNPROTECTED)
            .fold(current, u64::min);
        let safe = oldest - 1;
        self.safe_to_reclaim.0.fetch_max(safe, Ordering::AcqRel);
        safe
    }

    fn drain(&self, current: u64) {
        let safe = self.compute_safe_to_reclaim(current);
        for entry in self.drain.iter() {
            let epoch = entry.epoch.load(Ordering::Acquire);
            if epoch <= safe
                && entry
                    .epoch
                    .compare_exchange(epoch, LOCKED, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                // SAFETY: this thread holds the entry lock.
                let action = unsafe { (*entry.action.get()).take() };
                entry.epoch.store(FREE, Ordering::Release);
                if self.drain_count.0.fetch_sub(1, Ordering::AcqRel) == 1 {
                    if let Some(action) = action {
                        action();
                    }
                    return;
                }
                if let Some(action) = action {
                    action();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn action_waits_for_protected_slot() {
        let epoch = LightEpoch::new();
        let reader = epoch.reserve().unwrap();
        let writer = epoch.reserve().unwrap();
        epoch.protect(&reader);
        epoch.protect(&writer);
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        epoch.bump_with_action(move || flag.store(true, Ordering::SeqCst));
        assert!(!ran.load(Ordering::SeqCst));
        epoch.protect_and_drain(&writer);
        assert!(
            !ran.load(Ordering::SeqCst),
            "reader still holds the old epoch"
        );
        epoch.protect_and_drain(&reader);
        assert!(ran.load(Ordering::SeqCst));
        epoch.release(reader);
        epoch.release(writer);
    }

    #[test]
    fn released_slots_are_reused() {
        let epoch = LightEpoch::new();
        let ids: Vec<_> = (0..MAX_SLOTS).map(|_| epoch.reserve().unwrap()).collect();
        assert!(epoch.reserve().is_none());
        ids.into_iter().for_each(|id| epoch.release(id));
        assert!(epoch.reserve().is_some());
    }
}
