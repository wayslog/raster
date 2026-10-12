//! Store and internal operations (FASTER `faster.h`), REST phase, in memory.
use super::address::Address;
use super::epoch::{LightEpoch, SlotId};
use super::hlog::{Allocation, HybridLog, LogConfig};
use super::index::HashIndex;
use super::kv::{DeleteContext, Key, ReadContext, RmwContext, UpsertContext, Value};
use super::record::{Layout, Record, RecordInfo};
use super::session::Session;
use std::marker::PhantomData;
use std::sync::Arc;

#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Ok,
    Pending,
    NotFound,
    Aborted,
    OutOfMemory,
    IoError,
    Corruption,
}

#[derive(Clone, Copy, Debug)]
pub struct StoreConfig {
    pub index_buckets: u64,
    pub log: LogConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            index_buckets: 1 << 20,
            log: LogConfig::default(),
        }
    }
}

pub struct FasterKv<K: Key, V: Value> {
    // Declared before `epoch` so the log drops while pending actions are inert.
    pub(crate) hlog: HybridLog,
    pub(crate) index: HashIndex,
    pub(crate) epoch: Arc<LightEpoch>,
    _types: PhantomData<Types<K, V>>,
}

/// Keeps the store `Send + Sync` regardless of `K` and `V` auto traits.
type Types<K, V> = fn(K, V) -> (K, V);

/// Outcome of an internal attempt that may need to be re-run.
enum Attempt {
    Done(Status),
    Retry,
}

impl<K: Key, V: Value> FasterKv<K, V> {
    pub fn new(config: StoreConfig) -> Self {
        let epoch = Arc::new(LightEpoch::new());
        Self {
            hlog: HybridLog::new(config.log, epoch.clone()),
            index: HashIndex::new(config.index_buckets),
            epoch,
            _types: PhantomData,
        }
    }

    /// Starts a session; panics if all epoch slots are in use.
    pub fn start_session(&self) -> Session<'_, K, V> {
        Session::new(self)
    }

    #[inline(always)]
    fn record(&self, address: Address) -> Record<K, V> {
        // SAFETY: callers only pass addresses in [head, tail) under epoch
        // protection; such addresses are 8-byte aligned record starts.
        unsafe { Record::from_ptr(self.hlog.get(address)) }
    }

    /// Follows `previous` links from `address` while it is in memory.
    #[inline(always)]
    fn trace_back(&self, key: &K, mut address: Address, head: Address) -> Address {
        while address >= head {
            let record = self.record(address);
            // SAFETY: published records have initialized keys.
            if unsafe { record.key() } == key {
                break;
            }
            address = record.info().previous();
        }
        address
    }

    fn block_allocate(&self, slot: &SlotId, size: u64) -> Address {
        loop {
            match self.hlog.allocate(size) {
                Allocation::Ok(address) => return address,
                Allocation::PageFull(page) => {
                    while !self.hlog.new_page(page) {
                        self.epoch.protect_and_drain(slot);
                    }
                }
                Allocation::Retry => {
                    self.epoch.protect_and_drain(slot);
                }
            }
        }
    }

    #[inline]
    pub(crate) fn internal_read<C: ReadContext<K, V>>(&self, ctx: &mut C) -> Status {
        let key = *ctx.key();
        let Some(slot) = self.index.find(key.hash()) else {
            return Status::NotFound;
        };
        let head = self.hlog.head.load();
        let address = self.trace_back(&key, slot.entry.address(), head);
        if address >= head {
            let record = self.record(address);
            if record.info().tombstone() {
                return Status::NotFound;
            }
            // SAFETY: published record; the value lives while we are protected.
            let value = unsafe { record.value() };
            if address >= self.hlog.safe_read_only.load() {
                ctx.get_atomic(value);
            } else {
                ctx.get(value);
            }
            Status::Ok
        } else if address < self.hlog.begin.load() {
            Status::NotFound
        } else {
            // Disk reads arrive in phase B.
            Status::IoError
        }
    }

    #[inline]
    pub(crate) fn internal_upsert<C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
    ) -> Status {
        loop {
            if let Attempt::Done(status) = self.upsert_attempt(slot_id, ctx) {
                return status;
            }
        }
    }

    #[inline(always)]
    fn upsert_attempt<C: UpsertContext<K, V>>(&self, slot_id: &SlotId, ctx: &mut C) -> Attempt {
        let key = *ctx.key();
        let slot = self.index.find_or_create(key.hash());
        self.hlog.prefetch_for_write(slot.entry.address());
        let head = self.hlog.head.load();
        let address = self.trace_back(&key, slot.entry.address(), head);
        if address >= self.hlog.read_only.load() {
            let record = self.record(address);
            // SAFETY: published record in the mutable region.
            if !record.info().tombstone() && ctx.put_atomic(unsafe { record.value() }) {
                return Attempt::Done(Status::Ok);
            }
        }
        let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
        let record = self.record(new_address);
        let info = RecordInfo::new(slot.entry.address(), 0, false);
        // SAFETY: freshly allocated and not yet published.
        unsafe { record.initialize(info, key, ctx.put()) };
        if slot.try_update(new_address) {
            Attempt::Done(Status::Ok)
        } else {
            record.set_invalid();
            Attempt::Retry
        }
    }

    #[inline]
    pub(crate) fn internal_rmw<C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
    ) -> Status {
        loop {
            if let Attempt::Done(status) = self.rmw_attempt(slot_id, ctx) {
                return status;
            }
        }
    }

    #[inline(always)]
    fn rmw_attempt<C: RmwContext<K, V>>(&self, slot_id: &SlotId, ctx: &mut C) -> Attempt {
        let key = *ctx.key();
        let slot = self.index.find_or_create(key.hash());
        self.hlog.prefetch_for_write(slot.entry.address());
        let head = self.hlog.head.load();
        let address = self.trace_back(&key, slot.entry.address(), head);
        let read_only = self.hlog.read_only.load();
        if address >= read_only {
            let record = self.record(address);
            // SAFETY: published record in the mutable region.
            if !record.info().tombstone() && ctx.rmw_atomic(unsafe { record.value() }) {
                return Attempt::Done(Status::Ok);
            }
        } else if address >= self.hlog.safe_read_only.load() {
            // Fuzzy region: another session may still update in place.
            self.epoch.protect_and_drain(slot_id);
            return Attempt::Retry;
        } else if address >= self.hlog.begin.load() && address < head {
            // Disk reads arrive in phase B.
            return Attempt::Done(Status::IoError);
        }
        let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
        let record = self.record(new_address);
        let info = RecordInfo::new(slot.entry.address(), 0, false);
        let value = if address >= self.hlog.head.load() {
            let old = self.record(address);
            if old.info().tombstone() {
                ctx.rmw_initial()
            } else {
                // SAFETY: published record still in memory under protection.
                ctx.rmw_copy(unsafe { old.value() })
            }
        } else if address < self.hlog.begin.load() {
            ctx.rmw_initial()
        } else {
            // The source was evicted while allocating; abandon and retry.
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_tombstone(info, key) };
            record.set_invalid();
            return Attempt::Retry;
        };
        // SAFETY: freshly allocated and not yet published.
        unsafe { record.initialize(info, key, value) };
        if slot.try_update(new_address) {
            Attempt::Done(Status::Ok)
        } else {
            record.set_invalid();
            Attempt::Retry
        }
    }

    #[inline]
    pub(crate) fn internal_delete<C: DeleteContext<K>>(&self, slot_id: &SlotId, ctx: &C) -> Status {
        loop {
            let key = *ctx.key();
            let Some(slot) = self.index.find(key.hash()) else {
                return Status::NotFound;
            };
            let head = self.hlog.head.load();
            let address = self.trace_back(&key, slot.entry.address(), head);
            if address >= self.hlog.read_only.load() {
                self.record(address).set_tombstone();
                return Status::Ok;
            }
            if address < self.hlog.begin.load() {
                return Status::NotFound;
            }
            let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
            let record = self.record(new_address);
            let info = RecordInfo::new(slot.entry.address(), 0, true);
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_tombstone(info, key) };
            if slot.try_update(new_address) {
                return Status::Ok;
            }
            record.set_invalid();
        }
    }
}
