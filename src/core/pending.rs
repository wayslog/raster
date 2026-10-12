//! Pending operations that wait for record reads (FASTER `AsyncIOContext` and
//! `InternalContinuePending*`). They run only on the owning session thread.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{Key, ReadContext, RmwContext, Value};
use super::record::{Layout, Record};
use super::store::{FasterKv, Outcome, Status};

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
    Read(Address),
}

pub(crate) trait PendingOp<K: Key, V: Value> {
    /// Continues with the record read at the requested address, or `None` on I/O failure.
    fn resume(&mut self, store: &FasterKv<K, V>, slot: &SlotId, record: Option<DiskRecord>)
    -> Step;
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
    fn resume(&mut self, store: &FasterKv<K, V>, _: &SlotId, mut disk: Option<DiskRecord>) -> Step {
        let key = *self.ctx.key();
        let status = match lookup(store, &key, &mut disk) {
            Lookup::Found(_, true) | Lookup::Missing => Status::NotFound,
            Lookup::Found(value, false) => {
                self.ctx.get(value);
                Status::Ok
            }
            Lookup::Previous(address) => return Step::Read(address),
            Lookup::Failed(status) => status,
        };
        self.ctx.on_complete(status);
        Step::Done
    }
}

pub(crate) struct PendingRmw<C> {
    pub ctx: C,
    pub observed: Address,
}

impl<K: Key, V: Value, C: RmwContext<K, V>> PendingOp<K, V> for PendingRmw<C> {
    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
        mut disk: Option<DiskRecord>,
    ) -> Step {
        let key = *self.ctx.key();
        let outcome = match lookup(store, &key, &mut disk) {
            Lookup::Found(value, tombstone) => {
                let source = (!tombstone).then_some(value);
                store.rmw_from_disk(slot, &mut self.ctx, self.observed, source)
            }
            Lookup::Missing => store.rmw_from_disk(slot, &mut self.ctx, self.observed, None),
            Lookup::Previous(address) => return Step::Read(address),
            Lookup::Failed(status) => Outcome::Done(status),
        };
        match outcome {
            Outcome::Done(status) => {
                self.ctx.on_complete(status);
                Step::Done
            }
            Outcome::Disk { address, observed } => {
                self.observed = observed;
                Step::Read(address)
            }
        }
    }
}
