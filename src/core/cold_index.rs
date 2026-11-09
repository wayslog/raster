//! A disk-backed index of record-chain heads, stored in a nested FasterKv.

use super::address::Address;
use super::cpr::{IndexCallback, LogCallback};
use super::guid::Guid;
use super::hlog::{FIRST_VALID_OFFSET, LogConfig};
use super::recovery::RecoveryInfo;
use super::{
    FasterKv, Key, KeyContext, ReadContext, RmwContext, RmwOptions, Session, Status, StoreConfig,
    Value, ValueSlot,
};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const SLOTS: usize = 32;
const OUTER_TAG_BITS: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColdIndexEntry {
    Empty,
    Reserved,
    Published(Address),
}

impl ColdIndexEntry {
    fn word(self) -> Result<u64, Status> {
        match self {
            Self::Empty => Ok(0),
            Self::Reserved => Ok(Address::INVALID.control()),
            Self::Published(address)
                if address >= Address::new(0, FIRST_VALID_OFFSET)
                    && address <= Address::MAX
                    && address.offset().is_multiple_of(8) =>
            {
                Ok(address.control())
            }
            Self::Published(_) => Err(Status::InvalidArgument),
        }
    }

    fn decode(word: u64) -> Result<Self, Status> {
        if word == 0 {
            return Ok(Self::Empty);
        }
        if word == Address::INVALID.control() {
            return Ok(Self::Reserved);
        }
        let entry = Self::Published(Address::from_control(word));
        if entry.word() == Ok(word) {
            Ok(entry)
        } else {
            Err(Status::Corruption)
        }
    }
}

/// An update to one slot. `Set` is the unconditional update used by replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColdIndexUpdate {
    Reserve,
    CompareExchange {
        expected: ColdIndexEntry,
        address: Address,
    },
    Set(Address),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColdIndexOutcome {
    Entry(ColdIndexEntry),
    Conflict(ColdIndexEntry),
}

pub type ColdIndexResult = Result<ColdIndexOutcome, Status>;

pub struct ColdIndex {
    store: FasterKv<ChunkKey, Chunk>,
}

impl ColdIndex {
    /// Opens the index's own log and checkpoint directories beneath `root`.
    pub fn open(root: &Path, buckets: u64, log: LogConfig) -> io::Result<Self> {
        if !buckets.is_power_of_two() || buckets > i32::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold index bucket count must be a power of two below 2^31",
            ));
        }
        Ok(Self {
            store: FasterKv::open(StoreConfig {
                index_buckets: buckets,
                filepath: Some(root.join("log")),
                checkpoint_dir: Some(root.join("checkpoints")),
                log,
                ..StoreConfig::default()
            })?,
        })
    }

    pub fn try_start_session(&self) -> Option<ColdIndexSession<'_>> {
        Some(ColdIndexSession {
            session: self.store.try_start_session()?,
            mask: self.store.index.buckets() - 1,
        })
    }

    pub fn continue_session(&self, guid: Guid) -> Option<(ColdIndexSession<'_>, u64)> {
        let (session, serial) = self.store.continue_session(guid)?;
        Some((
            ColdIndexSession {
                session,
                mask: self.store.index.buckets() - 1,
            },
            serial,
        ))
    }

    pub fn checkpoint(
        &self,
        index: Option<IndexCallback>,
        log: Option<LogCallback>,
    ) -> Option<Guid> {
        self.store.checkpoint(index, log)
    }

    pub fn recover(&mut self, token: Guid) -> Result<RecoveryInfo, Status> {
        self.store.recover(token, token)
    }
}

pub struct ColdIndexSession<'a> {
    session: Session<'a, ChunkKey, Chunk>,
    mask: u64,
}

impl<'a> ColdIndexSession<'a> {
    pub fn guid(&self) -> Guid {
        self.session.guid()
    }

    pub fn refresh(&mut self) {
        self.session.refresh();
    }

    pub(crate) fn park_epoch(&self) {
        self.session.park_epoch();
    }

    pub(crate) fn epoch_scope(&self) -> super::epoch::EpochGuard<'a> {
        self.session.epoch_scope()
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.session.pending_count()
    }

    pub fn complete_pending(&mut self, wait: bool) -> bool {
        self.session.complete_pending(wait)
    }

    /// Returns the outcome immediately or calls `complete` once after Pending.
    pub fn lookup<F>(&mut self, hash: u64, serial: u64, complete: F) -> ColdIndexResult
    where
        F: FnOnce(ColdIndexResult) + Clone + 'static,
    {
        let mut request = Request::new(hash, self.mask, Change::Read, complete);
        let status = self.session.read(&mut request, serial);
        request.result(status)
    }

    /// Uses the same completion contract as lookup. A CAS conflict returns
    /// the entry observed by the update.
    pub fn update<F>(
        &mut self,
        hash: u64,
        update: ColdIndexUpdate,
        serial: u64,
        complete: F,
    ) -> ColdIndexResult
    where
        F: FnOnce(ColdIndexResult) + Clone + 'static,
    {
        let change = match update {
            ColdIndexUpdate::Reserve => Change::Reserve,
            ColdIndexUpdate::CompareExchange { expected, address } => Change::CompareExchange {
                expected: expected.word()?,
                desired: ColdIndexEntry::Published(address).word()?,
            },
            ColdIndexUpdate::Set(Address::INVALID) => Change::Set(Address::INVALID.control()),
            ColdIndexUpdate::Set(address) => {
                Change::Set(ColdIndexEntry::Published(address).word()?)
            }
        };
        let create_if_not_exists =
            !matches!(change, Change::CompareExchange { expected, .. } if expected != 0);
        let mut request = Request::new(hash, self.mask, change, complete);
        let status = self.session.rmw_with_options(
            &mut request,
            serial,
            RmwOptions {
                create_if_not_exists,
            },
        );
        request.result(status)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct ChunkKey(u64);

// SAFETY: A fixed-width integer owns no resources and accepts every bit pattern.
unsafe impl Key for ChunkKey {
    fn hash(&self) -> u64 {
        self.0
    }
}

#[repr(transparent)]
struct Chunk([AtomicU64; SLOTS]);

// SAFETY: The inline array has no padding or owned resources and every word
// pattern is valid. The engine relocates stabilized records bytewise.
unsafe impl Value for Chunk {
    fn initialize_empty(slot: ValueSlot<'_, Self>) {
        slot.write(Self(std::array::from_fn(|_| AtomicU64::new(0))));
    }
}

#[derive(Clone, Copy)]
enum Change {
    Read,
    Reserve,
    CompareExchange { expected: u64, desired: u64 },
    Set(u64),
}

#[derive(Clone)]
struct Request<F> {
    key: ChunkKey,
    slot: usize,
    change: Change,
    observed: u64,
    conflict: bool,
    complete: Option<F>,
}

impl<F: FnOnce(ColdIndexResult)> Request<F> {
    fn new(hash: u64, mask: u64, change: Change, complete: F) -> Self {
        let tag_mask = ((1 << OUTER_TAG_BITS) - 1) << Address::ADDRESS_BITS;
        let slot_shift = Address::ADDRESS_BITS + OUTER_TAG_BITS;
        Self {
            key: ChunkKey(hash & (mask | tag_mask)),
            slot: (hash >> slot_shift) as usize & (SLOTS - 1),
            change,
            observed: 0,
            conflict: false,
            complete: Some(complete),
        }
    }

    fn result(&self, status: Status) -> ColdIndexResult {
        match status {
            Status::Ok if self.conflict => Ok(ColdIndexOutcome::Conflict(ColdIndexEntry::decode(
                self.observed,
            )?)),
            Status::Ok => Ok(ColdIndexOutcome::Entry(ColdIndexEntry::decode(
                self.observed,
            )?)),
            Status::NotFound if matches!(self.change, Change::CompareExchange { .. }) => {
                Ok(ColdIndexOutcome::Conflict(ColdIndexEntry::Empty))
            }
            Status::NotFound => Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Empty)),
            status => Err(status),
        }
    }

    fn finish(&mut self, status: Status) {
        if let Some(complete) = self.complete.take() {
            complete(self.result(status));
        }
    }

    fn apply(&mut self, chunk: &Chunk) {
        let slot = &chunk.0[self.slot];
        self.conflict = false;
        self.observed = match self.change {
            Change::Reserve => slot
                .compare_exchange(
                    0,
                    Address::INVALID.control(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .map_or_else(|entry| entry, |_| Address::INVALID.control()),
            Change::CompareExchange { expected, desired } => {
                match slot.compare_exchange(expected, desired, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => desired,
                    Err(entry) => {
                        self.conflict = true;
                        entry
                    }
                }
            }
            Change::Set(entry) => {
                slot.store(entry, Ordering::Release);
                entry
            }
            Change::Read => unreachable!("read requests do not update chunks"),
        };
    }
}

// SAFETY: The key is a complete immutable integer copied with the request.
unsafe impl<F> KeyContext<ChunkKey> for Request<F> {
    fn key(&self) -> &ChunkKey {
        &self.key
    }
}

impl<F: FnOnce(ColdIndexResult)> ReadContext<ChunkKey, Chunk> for Request<F> {
    fn get(&mut self, chunk: &Chunk) {
        self.observed = chunk.0[self.slot].load(Ordering::Acquire);
    }

    fn on_complete(&mut self, status: Status) {
        self.finish(status);
    }
}

// SAFETY: Both allocation paths initialize all slots. Shared updates only
// modify one atomic word and preserve the allocation size.
unsafe impl<F: FnOnce(ColdIndexResult)> RmwContext<ChunkKey, Chunk> for Request<F> {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, Chunk>) {
        let chunk = Chunk(std::array::from_fn(|_| AtomicU64::new(0)));
        self.apply(&chunk);
        slot.write(chunk);
    }

    fn rmw_copy(&mut self, old: &Chunk, slot: ValueSlot<'_, Chunk>) {
        let chunk = Chunk(std::array::from_fn(|i| {
            AtomicU64::new(old.0[i].load(Ordering::Acquire))
        }));
        self.apply(&chunk);
        slot.write(chunk);
    }

    fn rmw_atomic(&mut self, chunk: &Chunk) -> bool {
        self.apply(chunk);
        true
    }

    fn on_complete(&mut self, status: Status) {
        self.finish(status);
    }
}

#[cfg(test)]
mod tests;
