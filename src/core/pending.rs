//! Pending operations (FASTER `AsyncIOContext`, retry requests and
//! `InternalContinuePending*`). They run only on the owning session thread.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{DeleteContext, Key, ReadContext, RmwContext, UpsertContext, Value};
use super::record::{Layout, Record};
use super::store::{FasterKv, OpMode, Outcome, Status};

/// A record read from the device, copied into 8-byte aligned memory.
pub(crate) struct DiskRecord {
    words: Vec<u64>,
}

impl DiskRecord {
    pub fn new(bytes: &[u8]) -> Self {
        let mut words = vec![0u64; bytes.len().div_ceil(8)];
        // SAFETY: `words` has at least `bytes.len()` bytes and does not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), words.as_mut_ptr().cast(), bytes.len())
        };
        Self { words }
    }

    fn record<K, V>(&mut self) -> Option<Record<K, V>> {
        if self.words.len() * 8 < Layout::<K, V>::SIZE {
            return None;
        }
        // SAFETY: aligned buffer of at least `Layout::SIZE` bytes owned by self.
        Some(unsafe { Record::from_ptr(self.words.as_mut_ptr().cast()) })
    }
}

pub(crate) enum Step {
    Done,
    /// Read the record at this address next.
    Read {
        address: Address,
        marked: bool,
    },
    /// Re-run the operation after the session refreshes.
    RetryLater {
        marked: bool,
    },
}

impl Step {
    fn from_outcome(outcome: Outcome, done: impl FnOnce(Status)) -> Self {
        match outcome {
            Outcome::Done(status) => {
                done(status);
                Step::Done
            }
            Outcome::Disk {
                address, marked, ..
            } => Step::Read { address, marked },
            Outcome::RetryLater { marked } => Step::RetryLater { marked },
            // Marked requests never shift; be conservative if one does.
            Outcome::Shift => Step::RetryLater { marked: false },
        }
    }
}

pub(crate) trait PendingOp<K: Key, V: Value> {
    fn hash(&self) -> u64;
    /// Continues with the record read at the requested address, or `None` on I/O failure.
    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
        mode: OpMode,
        record: Option<DiskRecord>,
    ) -> Step;
    /// Re-runs an operation queued with `Step::RetryLater`.
    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step;
}

/// Where a disk lookup for `key` landed.
enum Lookup<'r, V> {
    Found(&'r V, bool),
    Previous(Address),
    Missing,
    Failed(Status),
}

fn lookup<'r, K: Key, V: Value>(
    store: &FasterKv<K, V>,
    key: &K,
    disk: &'r mut Option<DiskRecord>,
) -> Lookup<'r, V> {
    let Some(record) = disk.as_mut().and_then(DiskRecord::record::<K, V>) else {
        return Lookup::Failed(if disk.is_some() {
            Status::Corruption
        } else {
            Status::IoError
        });
    };
    let info = record.info();
    if info.is_null() {
        return Lookup::Failed(Status::Corruption);
    }
    // SAFETY: the record was fully written before its page was flushed.
    if unsafe { record.key() } == key {
        // SAFETY: as above; the buffer lives as long as `disk`.
        let value: &'r V = unsafe { &*(record.value() as *const V) };
        return Lookup::Found(value, info.tombstone());
    }
    let previous = info.previous();
    if previous >= store.hlog.begin.load() {
        Lookup::Previous(previous)
    } else {
        Lookup::Missing
    }
}

pub(crate) struct PendingRead<C> {
    pub ctx: C,
}

impl<K: Key, V: Value, C: ReadContext<K, V>> PendingOp<K, V> for PendingRead<C> {
    fn hash(&self) -> u64 {
        self.ctx.key().hash()
    }

    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        _: &SlotId,
        mode: OpMode,
        mut disk: Option<DiskRecord>,
    ) -> Step {
        let key = *self.ctx.key();
        let status = match lookup(store, &key, &mut disk) {
            Lookup::Found(_, true) | Lookup::Missing => Status::NotFound,
            Lookup::Found(value, false) => {
                self.ctx.get(value);
                Status::Ok
            }
            Lookup::Previous(address) => {
                return Step::Read {
                    address,
                    marked: mode.holds_old,
                };
            }
            Lookup::Failed(status) => status,
        };
        self.ctx.on_complete(status);
        Step::Done
    }

    fn retry(&mut self, store: &FasterKv<K, V>, _: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_read(&mut self.ctx, mode);
        Step::from_outcome(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingRmw<C> {
    pub ctx: C,
    pub observed: Address,
}

impl<K: Key, V: Value, C: RmwContext<K, V>> PendingOp<K, V> for PendingRmw<C> {
    fn hash(&self) -> u64 {
        self.ctx.key().hash()
    }

    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
        mode: OpMode,
        mut disk: Option<DiskRecord>,
    ) -> Step {
        let key = *self.ctx.key();
        let outcome = match lookup(store, &key, &mut disk) {
            Lookup::Found(value, tombstone) => {
                let source = (!tombstone).then_some(value);
                store.rmw_from_disk(slot, &mut self.ctx, mode, self.observed, source)
            }
            Lookup::Missing => store.rmw_from_disk(slot, &mut self.ctx, mode, self.observed, None),
            Lookup::Previous(address) => {
                return Step::Read {
                    address,
                    marked: mode.holds_old,
                };
            }
            Lookup::Failed(status) => Outcome::Done(status),
        };
        self.step(outcome)
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_rmw(slot, &mut self.ctx, mode);
        self.step(outcome)
    }
}

impl<C> PendingRmw<C> {
    fn step<K: Key, V: Value>(&mut self, outcome: Outcome) -> Step
    where
        C: RmwContext<K, V>,
    {
        if let Outcome::Disk { observed, .. } = outcome {
            self.observed = observed;
        }
        Step::from_outcome(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingUpsert<C> {
    pub ctx: C,
}

impl<K: Key, V: Value, C: UpsertContext<K, V>> PendingOp<K, V> for PendingUpsert<C> {
    fn hash(&self) -> u64 {
        self.ctx.key().hash()
    }

    fn resume(&mut self, _: &FasterKv<K, V>, _: &SlotId, _: OpMode, _: Option<DiskRecord>) -> Step {
        unreachable!("blind upserts never read the device")
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_upsert(slot, &mut self.ctx, mode);
        Step::from_outcome(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingDelete<C> {
    pub ctx: C,
}

impl<K: Key, V: Value, C: DeleteContext<K>> PendingOp<K, V> for PendingDelete<C> {
    fn hash(&self) -> u64 {
        self.ctx.key().hash()
    }

    fn resume(&mut self, _: &FasterKv<K, V>, _: &SlotId, _: OpMode, _: Option<DiskRecord>) -> Step {
        unreachable!("deletes never read the device")
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_delete(slot, &self.ctx, mode);
        Step::from_outcome(outcome, |status| self.ctx.on_complete(status))
    }
}
