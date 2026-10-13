//! Pending operations (FASTER `AsyncIOContext`, retry requests and
//! `InternalContinuePending*`). They run only on the owning session thread.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{DeleteContext, Key, KeyContext, ReadContext, RmwContext, UpsertContext, Value};
use super::record::{Layout, Record};
use super::store::{FasterKv, OpMode, Outcome, Status};

/// Owned device bytes with enough initialized padding for record copies.
struct DiskBuffer(Vec<u8>);

impl DiskBuffer {
    fn from_bytes(mut bytes: Vec<u8>) -> Self {
        let padded = bytes.len().next_multiple_of(8);
        bytes.resize(padded, 0);
        if !bytes.as_ptr().cast::<u64>().is_aligned() {
            // Reserve enough initialized space to align within the same allocation.
            bytes.resize(padded + 7, 0);
            let offset = bytes.as_ptr().align_offset(8);
            bytes.copy_within(..padded, offset);
        }
        Self(bytes)
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        let ptr = self.0.as_mut_ptr();
        ptr.wrapping_add(ptr.align_offset(8))
    }
}

/// A record read from the device, owned in 8-byte aligned, padded memory.
pub(crate) struct DiskRecord {
    buffer: DiskBuffer,
    len: usize,
    pub address: Address,
}

impl DiskRecord {
    pub fn new(bytes: Vec<u8>, address: Address) -> Self {
        let len = bytes.len();
        Self {
            buffer: DiskBuffer::from_bytes(bytes),
            len,
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
        let record = unsafe { Record::<K, V>::from_ptr(self.buffer.as_mut_ptr()) };
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

pub(crate) struct PendingRead<C, const ABORT: bool> {
    pub ctx: C,
    /// Index entry seen when the read went to the device; the record found
    /// is cached only if the entry is still unchanged.
    pub observed: Address,
    /// Begin-address shifts seen when the read was issued. If the chain walk
    /// runs off the log after a shift, compaction may have moved the record
    /// to the tail, so the read restarts from the index.
    pub truncations: u64,
}

impl<K: Key, V: Value, C: ReadContext<K, V>, const ABORT: bool> PendingOp<K, V>
    for PendingRead<C, ABORT>
{
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
            Lookup::Found(_, true) => {
                if ABORT {
                    Status::Aborted
                } else {
                    Status::NotFound
                }
            }
            Lookup::Missing => Status::NotFound,
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
        let outcome = store.internal_read::<ABORT, C>(&mut self.ctx, mode);
        if let Outcome::Disk { observed, .. } = outcome {
            self.observed = observed;
        }
        Step::from_outcome::<K, V>(outcome, |status| self.ctx.on_complete(status))
    }
}

pub(crate) struct PendingRmw<C, const CREATE: bool> {
    pub ctx: C,
    pub observed: Address,
}

impl<K: Key, V: Value, C: RmwContext<K, V>, const CREATE: bool> PendingOp<K, V>
    for PendingRmw<C, CREATE>
{
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
                store.rmw_from_disk::<CREATE, C>(slot, &mut self.ctx, mode, self.observed, source)
            }
            Lookup::Missing => {
                store.rmw_from_disk::<CREATE, C>(slot, &mut self.ctx, mode, self.observed, None)
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
            Lookup::Failed(status) => Outcome::Done(status),
        };
        self.step(outcome)
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_rmw::<CREATE, C>(slot, &mut self.ctx, mode);
        self.step(outcome)
    }
}

impl<C, const CREATE: bool> PendingRmw<C, CREATE> {
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

pub(crate) struct PendingDelete<C, const FORCE: bool> {
    pub ctx: C,
}

impl<K: Key, V: Value, C: DeleteContext<K>, const FORCE: bool> PendingOp<K, V>
    for PendingDelete<C, FORCE>
{
    fn hash(&self) -> u64 {
        self.ctx.key_hash()
    }

    fn resume(&mut self, _: &FasterKv<K, V>, _: &SlotId, _: OpMode, _: Option<DiskRecord>) -> Step {
        unreachable!("deletes never read the device")
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        let outcome = store.internal_delete::<FORCE, C>(slot, &self.ctx, mode);
        Step::from_outcome::<K, V>(outcome, |status| self.ctx.on_complete(status))
    }
}

#[cfg(test)]
mod disk_record_tests {
    use super::*;

    fn short_record() -> Vec<u8> {
        let mut bytes = vec![0xff; 24];
        bytes[..8].copy_from_slice(&(1u64 << 48).to_le_bytes());
        bytes[8..16].copy_from_slice(&42u64.to_le_bytes());
        bytes[16] = 7;
        bytes.truncate(17);
        bytes
    }

    #[test]
    fn owned_device_bytes_zero_padding_before_full_record_copy() {
        let mut disk = DiskRecord::new(short_record(), Address::new(0, 64));
        let record = match disk.record::<u64, u8>(4096) {
            Ok(record) => record,
            Err(_) => panic!("complete device record must be accepted"),
        };
        // SAFETY: record() verified all headers and payload, with initialized padding.
        unsafe {
            assert_eq!(*record.key(), 42);
            assert_eq!(*record.value(), 7);
            assert_eq!(record.size(), 24);
            assert_eq!(*disk.buffer.as_mut_ptr().cast::<u64>().add(2), 7);
        }
    }

    #[test]
    fn initialized_padding_does_not_count_as_received_device_bytes() {
        let mut disk = DiskRecord::new(short_record(), Address::new(0, 64));
        assert!(matches!(
            disk.record::<u64, u32>(4096),
            Err(DiskError::NeedMore(20))
        ));
        let mut empty = DiskRecord::new(Vec::new(), Address::new(0, 64));
        assert!(matches!(
            empty.record::<u64, u8>(4096),
            Err(DiskError::Corruption)
        ));
    }
}
