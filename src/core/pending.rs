//! Pending operations (FASTER `AsyncIOContext`, retry requests and
//! `InternalContinuePending*`). They run only on the owning session thread.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{DeleteContext, Key, KeyContext, ReadContext, RmwContext, UpsertContext, Value};
use super::record::{Layout, Record};
use super::store::{FasterKv, OpMode, Outcome, Status};

/// A record read from the device, copied into 8-byte aligned memory.
pub(crate) struct DiskRecord {
    words: Vec<u64>,
    len: usize,
    pub address: Address,
}

impl DiskRecord {
    pub fn new(bytes: &[u8], address: Address) -> Self {
        let mut words = vec![0u64; bytes.len().div_ceil(8)];
        // SAFETY: `words` has at least `bytes.len()` bytes and does not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), words.as_mut_ptr().cast(), bytes.len())
        };
        Self {
            words,
            len: bytes.len(),
            address,
        }
    }

    pub(crate) fn record<K: Key, V: Value>(
        &mut self,
        page_size: u64,
    ) -> Result<Record<K, V>, DiskError> {
        let limit = page_size.saturating_sub(self.address.offset()) as usize;
        if self.len < 8 {
            return Err(DiskError::Corruption);
        }
        // SAFETY: the aligned buffer owns len bytes; subsequent access is staged.
        let record = unsafe { Record::<K, V>::from_ptr(self.words.as_mut_ptr().cast()) };
        if record.info().is_null() {
            return Err(DiskError::Corruption);
        }
        // SAFETY: required_bytes accesses only headers present in len bytes.
        let needed =
            unsafe { record.required_bytes(self.len, limit) }.map_err(|_| DiskError::Corruption)?;
        if needed > self.len {
            return Err(DiskError::NeedMore(needed));
        }
        Ok(record)
    }
}

pub(crate) enum DiskError {
    NeedMore(usize),
    Corruption,
}

pub(crate) enum Step {
    Done,
    /// Read the record at this address next.
    Read {
        address: Address,
        len: usize,
        marked: bool,
    },
    /// Re-run the operation after the session refreshes.
    RetryLater {
        marked: bool,
    },
}

impl Step {
    fn from_outcome<K, V>(outcome: Outcome, done: impl FnOnce(Status)) -> Self {
        match outcome {
            Outcome::Done(status) => {
                done(status);
                Step::Done
            }
            Outcome::Disk {
                address, marked, ..
            } => Step::Read {
                address,
                len: Layout::<K, V>::min_io_size(),
                marked,
            },
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
    NeedMore(usize),
    Failed(Status),
}

fn lookup<'r, K: Key, V: Value, C: KeyContext<K>>(
    store: &FasterKv<K, V>,
    ctx: &C,
    disk: &'r mut Option<DiskRecord>,
) -> Lookup<'r, V> {
    let record = match disk.as_mut() {
        None => return Lookup::Failed(Status::IoError),
        Some(disk) => match disk.record::<K, V>(store.hlog.page_size()) {
            Ok(record) => record,
            Err(DiskError::NeedMore(len)) => return Lookup::NeedMore(len),
            Err(DiskError::Corruption) => return Lookup::Failed(Status::Corruption),
        },
    };
    let info = record.info();
    if info.is_null() {
        return Lookup::Failed(Status::Corruption);
    }
    // SAFETY: the record was fully written before its page was flushed.
    if ctx.key_equals(unsafe { record.key() }) {
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
    /// Index entry seen when the read went to the device; the record found
    /// is cached only if the entry is still unchanged.
    pub observed: Address,
    /// Begin-address shifts seen when the read was issued. If the chain walk
    /// runs off the log after a shift, compaction may have moved the record
    /// to the tail, so the read restarts from the index.
    pub truncations: u64,
}

impl<K: Key, V: Value, C: ReadContext<K, V>> PendingOp<K, V> for PendingRead<C> {
    fn hash(&self) -> u64 {
        self.ctx.key_hash()
    }

    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        _: &SlotId,
        mode: OpMode,
        mut disk: Option<DiskRecord>,
    ) -> Step {
        let address = disk.as_ref().map_or(Address::INVALID, |disk| disk.address);
        let lookup = lookup(store, &self.ctx, &mut disk);
        if matches!(lookup, Lookup::Missing | Lookup::Failed(Status::IoError)) {
            let truncations = store.truncations.load(std::sync::atomic::Ordering::Acquire);
            if truncations != self.truncations {
                self.truncations = truncations;
                return Step::RetryLater {
                    marked: mode.holds_old,
                };
            }
        }
        let status = match lookup {
            Lookup::Found(_, true) | Lookup::Missing => Status::NotFound,
            Lookup::Found(value, false) => {
                self.ctx.get(value);
                if store.read_cache.is_some()
                    && let Some(record) = disk
                        .as_mut()
                        .and_then(|disk| disk.record::<K, V>(store.hlog.page_size()).ok())
                {
                    let _ = store.insert_cached(self.observed, &record);
                }
                Status::Ok
            }
            Lookup::Previous(address) => {
                return Step::Read {
                    address,
                    len: Layout::<K, V>::min_io_size(),
                    marked: mode.holds_old,
                };
            }
            Lookup::NeedMore(len) => {
                return Step::Read {
                    address,
                    len,
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
        if let Outcome::Disk { observed, .. } = outcome {
            self.observed = observed;
        }
        Step::from_outcome::<K, V>(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingRmw<C> {
    pub ctx: C,
    pub observed: Address,
}

impl<K: Key, V: Value, C: RmwContext<K, V>> PendingOp<K, V> for PendingRmw<C> {
    fn hash(&self) -> u64 {
        self.ctx.key_hash()
    }

    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
        mode: OpMode,
        mut disk: Option<DiskRecord>,
    ) -> Step {
        let address = disk.as_ref().map_or(Address::INVALID, |disk| disk.address);
        let outcome = match lookup(store, &self.ctx, &mut disk) {
            Lookup::Found(value, tombstone) => {
                let source = (!tombstone).then_some(value);
                store.rmw_from_disk(slot, &mut self.ctx, mode, self.observed, source)
            }
            Lookup::Missing => store.rmw_from_disk(slot, &mut self.ctx, mode, self.observed, None),
            Lookup::Previous(address) => {
                return Step::Read {
                    address,
                    len: Layout::<K, V>::min_io_size(),
                    marked: mode.holds_old,
                };
            }
            Lookup::NeedMore(len) => {
                return Step::Read {
                    address,
                    len,
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
        Step::from_outcome::<K, V>(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingUpsert<C> {
    pub ctx: C,
}

impl<K: Key, V: Value, C: UpsertContext<K, V>> PendingOp<K, V> for PendingUpsert<C> {
    fn hash(&self) -> u64 {
        self.ctx.key_hash()
    }

    fn resume(&mut self, _: &FasterKv<K, V>, _: &SlotId, _: OpMode, _: Option<DiskRecord>) -> Step {
        unreachable!("blind upserts never read the device")
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_upsert(slot, &mut self.ctx, mode);
        Step::from_outcome::<K, V>(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingDelete<C> {
    pub ctx: C,
}

impl<K: Key, V: Value, C: DeleteContext<K>> PendingOp<K, V> for PendingDelete<C> {
    fn hash(&self) -> u64 {
        self.ctx.key_hash()
    }

    fn resume(&mut self, _: &FasterKv<K, V>, _: &SlotId, _: OpMode, _: Option<DiskRecord>) -> Step {
        unreachable!("deletes never read the device")
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_delete(slot, &self.ctx, mode);
        Step::from_outcome::<K, V>(outcome, |status| self.ctx.on_complete(status))
    }
}
