//! Store and internal operations (FASTER `faster.h`).
use super::address::Address;
use super::checkpoint::CheckpointDirs;
use super::checkpoint_locks::CheckpointLocks;
use super::cpr::CprRun;
use super::epoch::{LightEpoch, SlotId};
use super::hlog::{Allocation, HybridLog, LogConfig};
use super::index::HashIndex;
use super::kv::{DeleteContext, Key, KeyContext, ReadContext, RmwContext, UpsertContext, Value};
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
    InvalidArgument,
}

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub index_buckets: u64,
    /// Device path used by open; new and with_device retain explicit device selection.
    pub filepath: Option<std::path::PathBuf>,
    /// Allocation hint. Frames are currently allocated eagerly for either value.
    pub pre_allocate: bool,
    pub read_cache: Option<super::read_cache::ReadCacheConfig>,
    /// Background policy to start explicitly with spawn_auto_compaction.
    pub auto_compaction: Option<super::auto_compact::AutoCompactionConfig>,
    /// Zero disables write-side assistance; otherwise writers help active compaction at this budget.
    pub log_size_budget: u64,
    /// Every complete record must fit in one log page; larger records panic.
    pub log: LogConfig,
    /// Root directory for checkpoints; required to checkpoint or recover.
    pub checkpoint_dir: Option<std::path::PathBuf>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            index_buckets: 1 << 20,
            filepath: None,
            pre_allocate: false,
            read_cache: None,
            auto_compaction: None,
            log_size_budget: 0,
            log: LogConfig::default(),
            checkpoint_dir: None,
        }
    }
}

pub struct FasterKv<K: Key, V: Value> {
    // Declared before `epoch` so the log drops while pending actions are inert.
    pub hlog: HybridLog,
    /// Boxed so that a store without a read cache keeps its hot fields packed.
    pub(crate) read_cache: Option<Box<super::read_cache::ReadCache>>,
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
    pub(crate) maintenance: Box<super::maintenance::Maintenance>,
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
    /// Active user sessions; internal scanner helpers are excluded.
    pub fn num_active_sessions(&self) -> usize {
        self.epoch.num_active_sessions()
    }

    /// Histogram: chain-head entries per primary bucket, including overflow.
    /// This is a concurrent snapshot of entries, not a count of live keys.
    /// Returns OutOfMemory when no helper epoch slot is available.
    pub fn index_distribution(&self) -> Result<std::collections::BTreeMap<usize, usize>, Status> {
        let slot = self.epoch.reserve_helper().ok_or(Status::OutOfMemory)?;
        self.epoch.protect_and_drain(&slot);
        let distribution = self.index.table().distribution();
        self.epoch.release(slot);
        Ok(distribution)
    }
    /// Opens buffered local storage for a configured filepath, or a memory-only
    /// store for an empty path. Background compaction is started explicitly.
    pub fn open(config: StoreConfig) -> std::io::Result<Self> {
        let device: Arc<dyn super::device::Device> = match config.filepath.as_ref() {
            Some(path) => Arc::new(super::device::FileDevice::open_with(path, 30, 8, false)?),
            None => Arc::new(super::device::NullDevice),
        };
        Ok(Self::with_device(config, device))
    }

    pub fn new(config: StoreConfig) -> Self {
        Self::with_device(config, Arc::new(super::device::NullDevice))
    }

    pub fn with_device(config: StoreConfig, device: Arc<dyn super::device::Device>) -> Self {
        let read_cache = config.read_cache;
        Self::with_options(config, device, read_cache)
    }

    /// A store that caches records read from the device in memory.
    pub fn with_read_cache(
        config: StoreConfig,
        device: Arc<dyn super::device::Device>,
        read_cache: super::read_cache::ReadCacheConfig,
    ) -> Self {
        Self::with_options(config, device, Some(read_cache))
    }

    fn with_options(
        config: StoreConfig,
        device: Arc<dyn super::device::Device>,
        read_cache: Option<super::read_cache::ReadCacheConfig>,
    ) -> Self {
        let epoch = Arc::new(LightEpoch::new());
        Self {
            hlog: HybridLog::new(config.log, epoch.clone(), device),
            read_cache: read_cache
                .map(|config| Box::new(super::read_cache::ReadCache::new(config, epoch.clone()))),
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
            maintenance: Box::new(super::maintenance::Maintenance::new(config.log_size_budget)),
            _types: PhantomData,
        }
    }

    /// Starts a session; panics if all epoch slots are in use.
    pub fn start_session(&self) -> Session<'_, K, V> {
        Session::new(self, super::guid::Guid::new(), 0)
    }

    /// Starts a session unless an action is running or all epoch slots are used.
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
    fn trace_back<C: KeyContext<K>>(
        &self,
        ctx: &C,
        mut address: Address,
        head: Address,
    ) -> Address {
        while address >= head {
            let record = self.record(address);
            // SAFETY: published records have initialized keys.
            if ctx.key_equals(unsafe { record.key() }) {
                break;
            }
            address = record.info().previous();
        }
        address
    }

    pub(crate) fn block_allocate(&self, slot: &SlotId, size: u64) -> Address {
        loop {
            // SAFETY: the session retains its reserved epoch slot throughout
            // this synchronous call and cannot reserve concurrently with it.
            // Waiting for the closed page also avoids repeated rejected work.
            match unsafe { self.hlog.reserve_for_session(size) } {
                Allocation::Ok(address) => return address,
                Allocation::PageFull(page) | Allocation::Retry(page) => {
                    while !self.hlog.new_page(page) {
                        self.epoch.protect_and_drain(slot);
                    }
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
                // A new-version writer can publish and release its lock after
                // our index snapshot but before we acquire the old side.
                // Recheck while holding the lock before admitting a v request.
                let locked_latest = self.index.find(hash).map_or(0, |slot| {
                    let entry = self.skip_read_cache(slot.entry.address());
                    self.latest_version(entry, self.hlog.head.load())
                });
                if record_newer(latest, mode.version) || record_newer(locked_latest, mode.version) {
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
    pub(crate) fn mark_pending(
        &self,
        mode: OpMode,
        hash: u64,
        guard: LockGuard<'_>,
    ) -> Option<bool> {
        if mode.phase != Phase::Prepare {
            return Some(mode.holds_old);
        }
        if mode.holds_old || guard.keep_old() {
            return Some(true);
        }
        let locks = self.locks().expect("checkpoint locks exist outside REST");
        locks.try_lock_old(hash).then_some(true)
    }

    #[inline(always)]
    pub(crate) fn internal_read<const ABORT: bool, C: ReadContext<K, V>>(
        &self,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        if mode.plain {
            self.read_attempt::<false, ABORT, C>(ctx, mode)
        } else {
            self.read_general::<ABORT, C>(ctx, mode)
        }
    }

    /// Reads outside the plain fast path, kept out of line so the fast path
    /// stays small enough to inline fully.
    #[inline(never)]
    fn read_general<const ABORT: bool, C: ReadContext<K, V>>(
        &self,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        self.read_attempt::<true, ABORT, C>(ctx, mode)
    }

    /// `RC`: index entries may point into the read cache.
    #[inline(always)]
    fn read_attempt<const RC: bool, const ABORT: bool, C: ReadContext<K, V>>(
        &self,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        let hash = ctx.key_hash();
        let Some(slot) = self.index.find(hash) else {
            return Outcome::Done(Status::NotFound);
        };
        let mut entry = slot.entry.address();
        if RC && entry.in_read_cache() {
            match self.read_cached(ctx, slot, entry) {
                Ok(status) => return Outcome::Done(status),
                Err(address) => entry = address,
            }
        }
        let head = self.hlog.head.load();
        if RC
            && mode.phase == Phase::Prepare
            && !mode.holds_old
            && record_newer(self.latest_version(entry, head), mode.version)
        {
            return Outcome::Shift;
        }
        let address = self.trace_back(ctx, entry, head);
        if address >= head {
            let record = self.record(address);
            if record.info().tombstone() {
                return Outcome::Done(if ABORT {
                    Status::Aborted
                } else {
                    Status::NotFound
                });
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
        } else if !RC {
            Outcome::Disk {
                address,
                observed: slot.entry.address(),
                marked: mode.holds_old,
            }
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

    #[inline(always)]
    pub(crate) fn internal_upsert<C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        if mode.plain {
            self.upsert_loop::<true, false, C>(slot_id, ctx, mode)
        } else {
            self.upsert_general(slot_id, ctx, mode)
        }
    }

    /// Upserts outside the plain fast path, kept out of line.
    #[inline(never)]
    fn upsert_general<C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        if mode.phase == Phase::Rest {
            self.upsert_loop::<true, true, C>(slot_id, ctx, mode)
        } else {
            self.upsert_loop::<false, true, C>(slot_id, ctx, mode)
        }
    }

    #[inline(always)]
    fn upsert_loop<const REST: bool, const RC: bool, C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        let hash = ctx.key_hash();
        loop {
            match self.upsert_attempt::<REST, RC, C>(slot_id, ctx, mode, hash) {
                Attempt::Done(status) => return Outcome::Done(status),
                Attempt::Retry => {}
                Attempt::Shift => return Outcome::Shift,
                Attempt::RetryLater(marked) => return Outcome::RetryLater { marked },
                Attempt::Disk(..) => unreachable!("blind upserts never read the device"),
            }
        }
    }

    #[inline(always)]
    fn upsert_attempt<const REST: bool, const RC: bool, C: UpsertContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
        hash: u64,
    ) -> Attempt {
        let slot = self.index.find_or_create(hash);
        self.hlog.prefetch_for_write(slot.entry.address());
        // Load both boundaries before touching the record, as FASTER does.
        let head = self.hlog.head.load();
        let read_only = self.hlog.read_only.load();
        let entry = if RC {
            self.skip_and_invalidate(slot.entry.address(), ctx)
        } else {
            slot.entry.address()
        };
        let (gate, _guard) = if REST {
            (Gate::Normal, LockGuard::none())
        } else {
            self.cpr_gate(mode, hash, self.latest_version(entry, head))
        };
        match gate {
            Gate::Normal => {}
            Gate::ForceRcu => {}
            Gate::Shift => return Attempt::Shift,
            Gate::RetryLater => return Attempt::RetryLater(mode.holds_old),
        }
        let address = self.trace_back(ctx, entry, head);
        if gate == Gate::Normal && address >= read_only {
            let record = self.record(address);
            // SAFETY: published record in the mutable region.
            if !record.info().tombstone() && ctx.put_atomic(unsafe { record.value() }) {
                return Attempt::Done(Status::Ok);
            }
        }
        self.upsert_append(slot_id, ctx, slot, entry, mode.version)
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
        previous: Address,
        version: u32,
    ) -> Attempt {
        let value_size = ctx.value_size();
        let new_address = self.block_allocate(
            slot_id,
            Layout::<K, V>::size(ctx.key_size(), value_size) as u64,
        );
        let record = self.record(new_address);
        let info = RecordInfo::new(previous, version, false);
        // SAFETY: freshly allocated and not yet published.
        unsafe {
            let value = record.initialize_key(ctx, value_size);
            ctx.put(value);
            record.publish(info);
        };
        if slot.try_update(new_address) {
            Attempt::Done(Status::Ok)
        } else {
            record.set_invalid();
            Attempt::Retry
        }
    }

    #[inline(always)]
    pub(crate) fn internal_rmw<const CREATE: bool, C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        if mode.plain {
            self.rmw_loop::<true, false, CREATE, C>(slot_id, ctx, mode)
        } else {
            self.rmw_general::<CREATE, C>(slot_id, ctx, mode)
        }
    }

    /// RMWs outside the plain fast path, kept out of line.
    #[inline(never)]
    fn rmw_general<const CREATE: bool, C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        if mode.phase == Phase::Rest {
            self.rmw_loop::<true, true, CREATE, C>(slot_id, ctx, mode)
        } else {
            self.rmw_loop::<false, true, CREATE, C>(slot_id, ctx, mode)
        }
    }

    #[inline(always)]
    fn rmw_loop<const REST: bool, const RC: bool, const CREATE: bool, C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
    ) -> Outcome {
        let hash = ctx.key_hash();
        loop {
            match self.rmw_attempt::<REST, RC, CREATE, C>(slot_id, ctx, mode, hash) {
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
    pub(crate) fn rmw_from_disk<const CREATE: bool, C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
        observed: Address,
        source: Option<&V>,
    ) -> Outcome {
        let hash = ctx.key_hash();
        let slot = self.index.find_or_create(hash);
        if self.skip_and_invalidate(slot.entry.address(), ctx) != observed {
            return self.internal_rmw::<CREATE, C>(slot_id, ctx, mode);
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
        if !CREATE && source.is_none() {
            return Outcome::Done(Status::NotFound);
        }
        let value_size =
            source.map_or_else(|| ctx.initial_value_size(), |old| ctx.copy_value_size(old));
        let new_address = self.block_allocate(
            slot_id,
            Layout::<K, V>::size(ctx.key_size(), value_size) as u64,
        );
        let record = self.record(new_address);
        let info = RecordInfo::new(observed, mode.version, false);
        // SAFETY: freshly allocated; disk source remains owned by the caller.
        unsafe {
            let value = record.initialize_key(ctx, value_size);
            match source {
                Some(old) => ctx.rmw_copy(old, value),
                None => ctx.rmw_initial(value),
            }
            record.publish(info);
        };
        if slot.try_update(new_address) {
            Outcome::Done(Status::Ok)
        } else {
            record.set_invalid();
            self.internal_rmw::<CREATE, C>(slot_id, ctx, mode)
        }
    }

    #[inline(always)]
    fn rmw_attempt<const REST: bool, const RC: bool, const CREATE: bool, C: RmwContext<K, V>>(
        &self,
        slot_id: &SlotId,
        ctx: &mut C,
        mode: OpMode,
        hash: u64,
    ) -> Attempt {
        let slot = self.index.find_or_create(hash);
        self.hlog.prefetch_for_write(slot.entry.address());
        let head = self.hlog.head.load();
        let read_only = self.hlog.read_only.load();
        let entry = if RC {
            self.skip_and_invalidate(slot.entry.address(), ctx)
        } else {
            slot.entry.address()
        };
        let (gate, guard) = if REST {
            (Gate::Normal, LockGuard::none())
        } else {
            self.cpr_gate(mode, hash, self.latest_version(entry, head))
        };
        match gate {
            Gate::Normal | Gate::ForceRcu => {}
            Gate::Shift => return Attempt::Shift,
            Gate::RetryLater => return Attempt::RetryLater(mode.holds_old),
        }
        let address = self.trace_back(ctx, entry, head);
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
                Some(marked) => Attempt::Disk(address, entry, marked),
                None => Attempt::Shift,
            };
        }
        let source = (address >= head)
            .then(|| self.record(address))
            .filter(|record| !record.info().tombstone());
        if !CREATE && source.is_none() {
            return Attempt::Done(Status::NotFound);
        }
        let value_size = source.as_ref().map_or_else(
            || ctx.initial_value_size(),
            |old| {
                // SAFETY: published source still protected before allocation.
                ctx.copy_value_size(unsafe { old.value() })
            },
        );
        let new_address = self.block_allocate(
            slot_id,
            Layout::<K, V>::size(ctx.key_size(), value_size) as u64,
        );
        let record = self.record(new_address);
        let info = RecordInfo::new(entry, mode.version, false);
        if source.is_some() && address < self.hlog.head.load() {
            // Allocation refreshed the epoch and evicted the source. Write a
            // valid size header so scans can skip this abandoned allocation.
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_tombstone(info, ctx, value_size) };
            record.set_invalid();
            return Attempt::Retry;
        }
        // SAFETY: fresh destination; any source is still resident and protected.
        unsafe {
            let value = record.initialize_key(ctx, value_size);
            match source {
                Some(old) => ctx.rmw_copy(old.value(), value),
                None => ctx.rmw_initial(value),
            }
            record.publish(info);
        };
        if slot.try_update(new_address) {
            Attempt::Done(Status::Ok)
        } else {
            record.set_invalid();
            Attempt::Retry
        }
    }

    #[inline]
    pub(crate) fn internal_delete<const FORCE: bool, C: DeleteContext<K>>(
        &self,
        slot_id: &SlotId,
        ctx: &C,
        mode: OpMode,
    ) -> Outcome {
        let hash = ctx.key_hash();
        loop {
            let Some(slot) = (if FORCE {
                Some(self.index.find_or_create(hash))
            } else {
                self.index.find(hash)
            }) else {
                return Outcome::Done(Status::NotFound);
            };
            let head = self.hlog.head.load();
            let read_only = self.hlog.read_only.load();
            let entry = self.skip_and_invalidate(slot.entry.address(), ctx);
            let (gate, _guard) = if mode.phase == Phase::Rest {
                (Gate::Normal, LockGuard::none())
            } else {
                self.cpr_gate(mode, hash, self.latest_version(entry, head))
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
            let address = self.trace_back(ctx, entry, head);
            if gate == Gate::Normal && address >= read_only {
                self.record(address).set_tombstone();
                return Outcome::Done(Status::Ok);
            }
            if !FORCE && address < self.hlog.begin.load() {
                return Outcome::Done(Status::NotFound);
            }
            let value_size = ctx.value_size().unwrap_or(size_of::<V>() as u32);
            let new_address = self.block_allocate(
                slot_id,
                Layout::<K, V>::size(ctx.key_size(), value_size) as u64,
            );
            let record = self.record(new_address);
            let info = RecordInfo::new(entry, mode.version, true);
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_tombstone(info, ctx, value_size) };
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
    /// REST without a read cache: no CPR rules and no cached entries.
    pub plain: bool,
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

#[cfg(test)]
mod tests {
    use super::super::kv::ValueSlot;
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone)]
    struct Increment(u64);
    // SAFETY: The complete fixed-size key is owned and stable.
    unsafe impl KeyContext<u64> for Increment {
        fn key(&self) -> &u64 {
            &self.0
        }
    }
    // SAFETY: Every destination is fully initialized and shared updates are atomic.
    // Copying an immutable source follows the engine's CPR old/new-side exclusion.
    unsafe impl RmwContext<u64, AtomicU64> for Increment {
        fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
            slot.write(AtomicU64::new(1));
        }
        fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
            slot.write(AtomicU64::new(old.load(Ordering::Relaxed) + 1));
        }
        fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
            value.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    #[test]
    fn prepare_rejects_new_version_published_after_index_snapshot() {
        let store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
            index_buckets: 64,
            log: LogConfig {
                page_bits: 16,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
            ..StoreConfig::default()
        });
        let slot = store.epoch.reserve_helper().unwrap();
        store.epoch.protect_and_drain(&slot);
        let mut op = Increment(0);
        let hash = op.key_hash();
        let rest = OpMode {
            phase: Phase::Rest,
            plain: true,
            version: 1,
            holds_old: false,
        };
        assert!(matches!(
            store.internal_rmw::<true, _>(&slot, &mut op, rest),
            Outcome::Done(Status::Ok)
        ));
        let locks = Box::into_raw(Box::new(CheckpointLocks::new(64)));
        store.checkpoint_locks.store(locks, Ordering::Release);

        // Pause the old request after its index/version snapshot. The new
        // request then publishes under the new-side lock and releases it.
        let observed = store.index.find(hash).unwrap().entry.address();
        let snapshot_version = store.latest_version(observed, store.hlog.head.load());
        assert_eq!(snapshot_version, 1);
        let new = OpMode {
            phase: Phase::InProgress,
            plain: false,
            version: 2,
            holds_old: false,
        };
        assert!(matches!(
            store.internal_rmw::<true, _>(&slot, &mut op, new),
            Outcome::Done(Status::Ok)
        ));
        let newest = store.index.find(hash).unwrap().entry.address();
        assert_eq!(store.latest_version(newest, store.hlog.head.load()), 2);
        let prepare = OpMode {
            phase: Phase::Prepare,
            plain: false,
            version: 1,
            holds_old: false,
        };
        let (gate, guard) = store.cpr_gate(prepare, hash, snapshot_version);
        if gate == Gate::Normal {
            // This is the real disk-completion path after the stale request
            // retains an old-side lock. A changed index restarts the RMW.
            assert_eq!(store.mark_pending(prepare, hash, guard), Some(true));
            let disk_value = AtomicU64::new(1);
            let pending = OpMode {
                holds_old: true,
                ..prepare
            };
            assert!(matches!(
                store.rmw_from_disk::<true, _>(
                    &slot,
                    &mut op,
                    pending,
                    observed,
                    Some(&disk_value)
                ),
                Outcome::Done(Status::Ok)
            ));
            store.locks().unwrap().unlock_old(hash);
        } else {
            drop(guard);
        }
        // SAFETY: Both initialized records remain protected by the helper slot.
        let old_value = unsafe { store.record(observed).value().load(Ordering::Relaxed) };
        // SAFETY: The new initialized record has the same epoch protection.
        let new_value = unsafe { store.record(newest).value().load(Ordering::Relaxed) };
        store
            .checkpoint_locks
            .store(std::ptr::null_mut(), Ordering::Release);
        // SAFETY: No operation retains a lock and this test owns the allocation.
        unsafe { drop(Box::from_raw(locks)) };
        store.epoch.release(slot);
        assert_eq!(
            gate,
            Gate::Shift,
            "stale PREPARE must shift; old value={old_value}, new value={new_value}"
        );
        assert_eq!(old_value, 1);
        assert_eq!(new_value, 2);
    }
}
