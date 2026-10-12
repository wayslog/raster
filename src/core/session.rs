//! Session execution context (FASTER `ExecutionContext`).
//!
//! A session protects the epoch for its whole life and advances it with
//! `refresh`. Operations touch only session-owned fields; they never enter or
//! leave the epoch.
use super::epoch::SlotId;
use super::kv::{DeleteContext, Key, ReadContext, RmwContext, UpsertContext, Value};
use super::store::{FasterKv, Status};

pub struct Session<'a, K: Key, V: Value> {
    store: &'a FasterKv<K, V>,
    slot: Option<SlotId>,
    serial: u64,
}

impl<'a, K: Key, V: Value> Session<'a, K, V> {
    pub(crate) fn new(store: &'a FasterKv<K, V>) -> Self {
        let slot = store.epoch.reserve().expect("all epoch slots are in use");
        store.epoch.protect_and_drain(&slot);
        Self {
            store,
            slot: Some(slot),
            serial: 0,
        }
    }

    #[inline(always)]
    fn slot(&self) -> &SlotId {
        // SAFETY of unwrap: only `drop` takes the slot.
        self.slot.as_ref().expect("live session")
    }

    /// Serial number of the last operation issued by this session.
    pub fn serial(&self) -> u64 {
        self.serial
    }

    #[inline]
    pub fn read<C: ReadContext<K, V>>(&mut self, ctx: &mut C, serial: u64) -> Status {
        let status = self.store.internal_read(ctx);
        self.serial = serial;
        status
    }

    #[inline]
    pub fn upsert<C: UpsertContext<K, V>>(&mut self, ctx: &mut C, serial: u64) -> Status {
        let status = self.store.internal_upsert(self.slot(), ctx);
        self.serial = serial;
        status
    }

    #[inline]
    pub fn rmw<C: RmwContext<K, V>>(&mut self, ctx: &mut C, serial: u64) -> Status {
        let status = self.store.internal_rmw(self.slot(), ctx);
        self.serial = serial;
        status
    }

    #[inline]
    pub fn delete<C: DeleteContext<K>>(&mut self, ctx: &C, serial: u64) -> Status {
        let status = self.store.internal_delete(self.slot(), ctx);
        self.serial = serial;
        status
    }

    /// Advances this session's epoch and runs any ready deferred actions.
    #[inline]
    pub fn refresh(&mut self) {
        self.store.epoch.protect_and_drain(self.slot());
    }

    /// Completes pending operations; returns true when none remain.
    pub fn complete_pending(&mut self, _wait: bool) -> bool {
        self.refresh();
        true
    }
}

impl<K: Key, V: Value> Drop for Session<'_, K, V> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.store.epoch.release(slot);
        }
    }
}
