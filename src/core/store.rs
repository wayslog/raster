//! Store and internal operations (FASTER `faster.h`).
use super::address::Address;
use super::checkpoint::CheckpointDirs;
use super::checkpoint_locks::CheckpointLocks;
use super::cpr::CprRun;
use super::epoch::{LightEpoch, SlotId};
use super::hlog::{Allocation, HybridLog, LogConfig};
use super::index::HashIndex;
use super::kv::{DeleteContext, Key, ReadContext, RmwContext, UpsertContext, Value};
use super::record::{Layout, Record, RecordInfo};
use super::session::Session;
use super::state::{AtomicSystemState, Phase, SystemState, record_newer};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;

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

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub index_buckets: u64,
    pub log: LogConfig,
    /// Root directory for checkpoints; required to checkpoint or recover.
    pub checkpoint_dir: Option<std::path::PathBuf>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            index_buckets: 1 << 20,
            log: LogConfig::default(),
            checkpoint_dir: None,
        }
    }
}

pub struct FasterKv<K: Key, V: Value> {
    // Declared before `epoch` so the log drops while pending actions are inert.
    pub hlog: HybridLog,
    pub(crate) index: HashIndex,
    pub(crate) epoch: Arc<LightEpoch>,
    pub(crate) state: AtomicSystemState,
    /// Allocated while a checkpoint runs; freed by an epoch action at REST.
    pub(crate) checkpoint_locks: AtomicPtr<CheckpointLocks>,
    pub(crate) cpr: std::sync::Mutex<CprRun>,
    pub(crate) gc: std::sync::Mutex<super::gc::GcRun>,
    pub(crate) gc_next_chunk: std::sync::atomic::AtomicU64,
    pub(crate) grow: std::sync::Mutex<super::grow::GrowRun>,
    pub(crate) grow_next_chunk: std::sync::atomic::AtomicU64,
    /// Number of begin-address shifts; pending reads retry if it changes.
    pub(crate) truncations: std::sync::atomic::AtomicU64,
    pub(crate) dirs: Option<CheckpointDirs>,
    /// Persisted serial numbers of recovered sessions, by session GUID.
    pub(crate) continue_tokens: std::sync::Mutex<std::collections::HashMap<super::guid::Guid, u64>>,
    _types: PhantomData<Types<K, V>>,
}

/// Keeps the store `Send + Sync` regardless of `K` and `V` auto traits.
type Types<K, V> = fn(K, V) -> (K, V);

/// Outcome of an internal attempt that may need to be re-run.
enum Attempt {
    Done(Status),
    Retry,
    /// Read from the device: address, observed entry, old lock marked.
    Disk(Address, Address, bool),
    /// Retry after refresh; the flag says whether the old lock is marked.
    RetryLater(bool),
    /// CPR shift detected: the session must move to v+1 and retry.
    Shift,
}

/// Result of an operation that may need a record read from the device.
pub(crate) enum Outcome {
    Done(Status),
    /// Read the record at `address`; `observed` is the index entry seen.
    Disk {
        address: Address,
        observed: Address,
        /// The request holds the old-side checkpoint lock for its key.
        marked: bool,
    },
    /// Retry after the session refreshes (fuzzy region or CPR lock conflict).
    RetryLater {
        marked: bool,
    },
    /// CPR shift detected: refresh into version v+1 and retry.
    Shift,
}

impl<K: Key, V: Value> FasterKv<K, V> {
    pub fn new(config: StoreConfig) -> Self {
        Self::with_device(config, Arc::new(super::device::NullDevice))
    }

    pub fn with_device(config: StoreConfig, device: Arc<dyn super::device::Device>) -> Self {
        let epoch = Arc::new(LightEpoch::new());
        Self {
            hlog: HybridLog::new(config.log, epoch.clone(), device),
            index: HashIndex::new(config.index_buckets),
            epoch,
            state: AtomicSystemState::new(SystemState::rest(1)),
            checkpoint_locks: AtomicPtr::new(std::ptr::null_mut()),
            cpr: Default::default(),
            gc: Default::default(),
            gc_next_chunk: Default::default(),
            grow: Default::default(),
            grow_next_chunk: Default::default(),
            truncations: Default::default(),
            dirs: config.checkpoint_dir.map(|root| CheckpointDirs { root }),
            continue_tokens: Default::default(),
            _types: PhantomData,
        }
    }

    /// Starts a session; panics if all epoch slots are in use.
    pub fn start_session(&self) -> Session<'_, K, V> {
        Session::new(self, super::guid::Guid::new(), 0)
    }

    /// Starts a session unless a state-machine action is running.
    pub fn try_start_session(&self) -> Option<Session<'_, K, V>> {
        Session::try_new(self, super::guid::Guid::new(), 0)
    }

    #[inline(always)]
    pub(crate) fn record(&self, address: Address) -> Record<K, V> {
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

    pub(crate) fn block_allocate(&self, slot: &SlotId, size: u64) -> Address {
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

    /// Version of the newest in-memory record at the index entry, or 0.
    #[inline(always)]
    pub(crate) fn latest_version(&self, entry: Address, head: Address) -> u32 {
        if entry >= head {
            self.record(entry).info().version()
        } else {
            0
        }
    }

    /// CPR rules for an update outside REST (FASTER InternalUpsert/Rmw/Delete).
    #[cold]
    pub(crate) fn cpr_gate(&self, mode: OpMode, hash: u64, latest: u32) -> (Gate, LockGuard<'_>) {
        // The table exists from checkpoint start until every session has left
        // PERSISTENCE_CALLBACK, the only phase that needs no locks.
        let locks = || {
            self.locks()
                .expect("checkpoint locks exist before WAIT_FLUSH")
        };
        let older = record_newer(mode.version, latest);
        match mode.phase {
            Phase::Prepare if mode.holds_old => (Gate::Normal, LockGuard::none()),
            Phase::Prepare => {
                if !locks().try_lock_old(hash) {
                    return (Gate::Shift, LockGuard::none());
                }
                let guard = LockGuard::old(locks(), hash);
                if record_newer(latest, mode.version) {
                    (Gate::Shift, guard)
                } else {
                    (Gate::Normal, guard)
                }
            }
            Phase::InProgress if older => {
                if locks().try_lock_new(hash) {
                    (Gate::ForceRcu, LockGuard::new(locks(), hash))
                } else {
                    (Gate::RetryLater, LockGuard::none())
                }
            }
            Phase::WaitPending if older => {
                if locks().old_locked(hash) {
                    (Gate::RetryLater, LockGuard::none())
                } else {
                    (Gate::ForceRcu, LockGuard::none())
                }
            }
            Phase::WaitFlush | Phase::PersistenceCallback if older => {
                (Gate::ForceRcu, LockGuard::none())
            }
            _ => (Gate::Normal, LockGuard::none()),
        }
    }

    /// Takes the old-side lock for a request about to go pending in PREPARE.
    /// Returns `Some(marked)`, or `None` if the session must shift to v+1.
    #[inline]
    fn mark_pending(&self, mode: OpMode, hash: u64, guard: LockGuard<'_>) -> Option<bool> {
        if mode.phase != Phase::Prepare {
            return Some(mode.holds_old);
        }
        if mode.holds_old || guard.keep_old() {
            return Some(true);
        }
        let locks = self.locks().expect("checkpoint locks exist outside REST");
        locks.try_lock_old(hash).then_some(true)
    }

    #[inline]
    pub(crate) fn internal_read<C: ReadContext<K, V>>(&self, ctx: &mut C, mode: OpMode) -> Outcome {
        let key = *ctx.key();
        let hash = key.hash();
        let Some(slot) = self.index.find(hash) else {
            return Outcome::Done(Status::NotFound);
        };
        let head = self.hlog.head.load();
        if mode.phase == Phase::Prepare
            && !mode.holds_old
            && record_newer(
                self.latest_version(slot.entry.address(), head),
                mode.version,
            )
        {
            return Outcome::Shift;
        }
        let address = self.trace_back(&key, slot.entry.address(), head);
        if address >= head {
            let record = self.record(address);
            if record.info().tombstone() {
                return Outcome::Done(Status::NotFound);
            }
            // SAFETY: published record; the value lives while we are protected.
            let value = unsafe { record.value() };
            if address >= self.hlog.safe_read_only.load() {
                ctx.get_atomic(value);
            } else {
                ctx.get(value);
            }
            Outcome::Done(Status::Ok)
        } else if address < self.hlog.begin.load() {
            Outcome::Done(Status::NotFound)
        } else {
            match self.mark_pending(mode, hash, LockGuard::none()) {
                Some(marked) => Outcome::Disk {
                    address,
                    observed: slot.entry.address(),
                    marked,
                },
                None => Outcome::Shift,
            }
        }
    }

    #[inline]
    pub(crate) fn internal_upsert<C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        loop {
            let attempt = if mode.phase == Phase::Rest {
                self.upsert_attempt::<true, C>(slot_id, ctx, mode)
            } else {
                self.upsert_attempt::<false, C>(slot_id, ctx, mode)
            };
            match attempt {
                Attempt::Done(status) => return Outcome::Done(status),
                Attempt::Retry => {}
                Attempt::Shift => return Outcome::Shift,
                Attempt::RetryLater(marked) => return Outcome::RetryLater { marked },
                Attempt::Disk(..) => unreachable!("blind upserts never read the device"),
            }
        }
    }

    #[inline(always)]
    fn upsert_attempt<const REST: bool, C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Attempt {
        let key = *ctx.key();
        let hash = key.hash();
        let slot = self.index.find_or_create(hash);
        self.hlog.prefetch_for_write(slot.entry.address());
        // Load both boundaries before touching the record, as FASTER does.
        let head = self.hlog.head.load();
        let read_only = self.hlog.read_only.load();
        let (gate, _guard) = if REST {
            (Gate::Normal, LockGuard::none())
        } else {
            self.cpr_gate(mode, hash, self.latest_version(slot.entry.address(), head))
        };
        match gate {
            Gate::Normal => {}
            Gate::ForceRcu => {}
            Gate::Shift => return Attempt::Shift,
            Gate::RetryLater => return Attempt::RetryLater(mode.holds_old),
        }
        let address = self.trace_back(&key, slot.entry.address(), head);
        if gate == Gate::Normal && address >= read_only {
            let record = self.record(address);
            // SAFETY: published record in the mutable region.
            if !record.info().tombstone() && ctx.put_atomic(unsafe { record.value() }) {
                return Attempt::Done(Status::Ok);
            }
        }
        self.upsert_append(slot_id, ctx, slot, key, mode.version)
    }

    /// Appends a new record for an upsert. Kept out of line so the in-place
    /// update stays on the straight-line hot path.
    #[cold]
    #[inline(never)]
    fn upsert_append<C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        slot: super::index::Slot<'_>,
        key: K,
        version: u32,
    ) -> Attempt {
        let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
        let record = self.record(new_address);
        let info = RecordInfo::new(slot.entry.address(), version, false);
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
        mode: OpMode,
    ) -> Outcome {
        loop {
            let attempt = if mode.phase == Phase::Rest {
                self.rmw_attempt::<true, C>(slot_id, ctx, mode)
            } else {
                self.rmw_attempt::<false, C>(slot_id, ctx, mode)
            };
            match attempt {
                Attempt::Done(status) => return Outcome::Done(status),
                Attempt::Disk(address, observed, marked) => {
                    return Outcome::Disk {
                        address,
                        observed,
                        marked,
                    };
                }
                Attempt::RetryLater(marked) => return Outcome::RetryLater { marked },
                Attempt::Shift => return Outcome::Shift,
                Attempt::Retry => {}
            }
        }
    }

    /// Continues an RMW whose source came from the device (`None` if the key
    /// was not found there). Restarts if a newer record was published meanwhile.
    pub(crate) fn rmw_from_disk<C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
        observed: Address,
        source: Option<&V>,
    ) -> Outcome {
        let key = *ctx.key();
        let hash = key.hash();
        let slot = self.index.find_or_create(hash);
        if slot.entry.address() != observed {
            return self.internal_rmw(slot_id, ctx, mode);
        }
        // The request released any checkpoint lock when it went pending, so
        // apply the CPR rules again before appending: a v+1 copy must not
        // overtake a still-pending v request on the same key.
        let (gate, _guard) = if mode.phase == Phase::Rest {
            (Gate::Normal, LockGuard::none())
        } else {
            let head = self.hlog.head.load();
            self.cpr_gate(mode, hash, self.latest_version(observed, head))
        };
        match gate {
            Gate::Normal | Gate::ForceRcu => {}
            Gate::Shift => return Outcome::Shift,
            Gate::RetryLater => {
                return Outcome::RetryLater {
                    marked: mode.holds_old,
                };
            }
        }
        let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
        let record = self.record(new_address);
        let value = match source {
            Some(old) => ctx.rmw_copy(old),
            None => ctx.rmw_initial(),
        };
        let info = RecordInfo::new(observed, mode.version, false);
        // SAFETY: freshly allocated and not yet published.
        unsafe { record.initialize(info, key, value) };
        if slot.try_update(new_address) {
            Outcome::Done(Status::Ok)
        } else {
            record.set_invalid();
            self.internal_rmw(slot_id, ctx, mode)
        }
    }

    #[inline(always)]
    fn rmw_attempt<const REST: bool, C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Attempt {
        let key = *ctx.key();
        let hash = key.hash();
        let slot = self.index.find_or_create(hash);
        self.hlog.prefetch_for_write(slot.entry.address());
        let head = self.hlog.head.load();
        let read_only = self.hlog.read_only.load();
        let (gate, guard) = if REST {
            (Gate::Normal, LockGuard::none())
        } else {
            self.cpr_gate(mode, hash, self.latest_version(slot.entry.address(), head))
        };
        match gate {
            Gate::Normal | Gate::ForceRcu => {}
            Gate::Shift => return Attempt::Shift,
            Gate::RetryLater => return Attempt::RetryLater(mode.holds_old),
        }
        let address = self.trace_back(&key, slot.entry.address(), head);
        if address >= read_only {
            if gate == Gate::Normal {
                let record = self.record(address);
                // SAFETY: published record in the mutable region.
                if !record.info().tombstone() && ctx.rmw_atomic(unsafe { record.value() }) {
                    return Attempt::Done(Status::Ok);
                }
            }
        } else if address >= self.hlog.safe_read_only.load() {
            // Fuzzy region: another session may still update in place, so a
            // copy could lose its update. Retry once safe_read_only advances.
            return match self.mark_pending(mode, hash, guard) {
                Some(marked) => Attempt::RetryLater(marked),
                None => Attempt::Shift,
            };
        } else if address >= self.hlog.begin.load() && address < head {
            return match self.mark_pending(mode, hash, guard) {
                Some(marked) => Attempt::Disk(address, slot.entry.address(), marked),
                None => Attempt::Shift,
            };
        }
        let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
        let record = self.record(new_address);
        let info = RecordInfo::new(slot.entry.address(), mode.version, false);
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
    pub(crate) fn internal_delete<C: DeleteContext<K>>(
        &self,
        slot_id: &SlotId,
        ctx: &C,
        mode: OpMode,
    ) -> Outcome {
        loop {
            let key = *ctx.key();
            let hash = key.hash();
            let Some(slot) = self.index.find(hash) else {
                return Outcome::Done(Status::NotFound);
            };
            let head = self.hlog.head.load();
            let read_only = self.hlog.read_only.load();
            let (gate, _guard) = if mode.phase == Phase::Rest {
                (Gate::Normal, LockGuard::none())
            } else {
                self.cpr_gate(mode, hash, self.latest_version(slot.entry.address(), head))
            };
            match gate {
                Gate::Normal | Gate::ForceRcu => {}
                Gate::Shift => return Outcome::Shift,
                Gate::RetryLater => {
                    return Outcome::RetryLater {
                        marked: mode.holds_old,
                    };
                }
            }
            let address = self.trace_back(&key, slot.entry.address(), head);
            if gate == Gate::Normal && address >= read_only {
                self.record(address).set_tombstone();
                return Outcome::Done(Status::Ok);
            }
            if address < self.hlog.begin.load() {
                return Outcome::Done(Status::NotFound);
            }
            let new_address = self.block_allocate(slot_id, Layout::<K, V>::SIZE as u64);
            let record = self.record(new_address);
            let info = RecordInfo::new(slot.entry.address(), mode.version, true);
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_tombstone(info, key) };
            if slot.try_update(new_address) {
                return Outcome::Done(Status::Ok);
            }
            record.set_invalid();
        }
    }
}

/// How an operation runs relative to the checkpoint state machine.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OpMode {
    pub phase: Phase,
    /// Version stamped on new records and compared with existing ones.
    pub version: u32,
    /// The request already holds the old-side lock for its key: it was
    /// marked in PREPARE and must still complete as a version-v operation.
    pub holds_old: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    Normal,
    ForceRcu,
    Shift,
    RetryLater,
}

/// Releases a checkpoint lock side at the end of an attempt.
pub(crate) struct LockGuard<'a> {
    locks: Option<&'a CheckpointLocks>,
    hash: u64,
    new_side: bool,
}

impl<'a> LockGuard<'a> {
    pub(crate) fn none() -> Self {
        Self {
            locks: None,
            hash: 0,
            new_side: false,
        }
    }
    fn old(locks: &'a CheckpointLocks, hash: u64) -> Self {
        Self {
            locks: Some(locks),
            hash,
            new_side: false,
        }
    }
    fn new(locks: &'a CheckpointLocks, hash: u64) -> Self {
        Self {
            locks: Some(locks),
            hash,
            new_side: true,
        }
    }
    /// Hands a held old-side lock over to a pending request.
    fn keep_old(mut self) -> bool {
        let kept = self.locks.is_some() && !self.new_side;
        if kept {
            self.locks = None;
        }
        kept
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        if let Some(locks) = self.locks {
            if self.new_side {
                locks.unlock_new(self.hash);
            } else {
                locks.unlock_old(self.hash);
            }
        }
    }
}
