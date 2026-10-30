//! Log compaction (FASTER `CompactWithLookup`, `ConditionalInsert`).
//!
//! Compaction scans `[begin, until)` and copies every record that is still
//! the newest version of its key to the tail, then optionally truncates the
//! log at `until`. Liveness is decided against the index: a copy is published
//! with a CAS on the entry observed while checking, so a concurrent update
//! wins and the copy is discarded.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{Key, KeyContext, Value};
use super::pending::{DiskError, DiskRecord, PendingOp, Step};
use super::record::{Layout, RecordInfo};
use super::store::{FasterKv, OpMode, Outcome, Status};

#[cfg(test)]
mod tests;

/// Completed compaction progress, independent of an optional checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub struct CompactionResult {
    pub until: Address,
    /// Begin address after this call's optional truncation.
    pub begin_address: Address,
    /// None when not requested; a failure does not undo compaction or truncation.
    pub checkpoint: Option<Result<super::guid::Guid, Status>>,
}

/// A record being copied: its bytes and the chain range left to check.
pub(crate) struct CopyRequest<'a, K, V> {
    pub words: &'a [u64],
    pub types: std::marker::PhantomData<(K, V)>,
    /// Records above this address may still hide a newer version of the key;
    /// it starts at the address of the record being copied.
    pub search_min: Address,
}

// SAFETY: The borrowed scan record is complete and immutable; suspension
// deep-copies its initialized words before the scanner can release it.
unsafe impl<K: Key, V> KeyContext<K> for CopyRequest<'_, K, V> {
    fn key(&self) -> &K {
        // SAFETY: words contains a complete aligned record copied from a scan.
        unsafe {
            &*self
                .words
                .as_ptr()
                .cast::<u8>()
                .add(Layout::<K, V>::KEY_OFFSET)
                .cast::<K>()
        }
    }
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Copies `request` to the tail if its source is still the newest record
    /// of its key (FASTER `InternalConditionalInsert`). `Done(Ok)` means
    /// copied, `Done(Aborted)` means a newer version exists or the source is
    /// no longer reachable.
    pub(crate) fn conditional_insert(
        &self,
        slot_id: &SlotId,
        request: &CopyRequest<'_, K, V>,
        mode: OpMode,
    ) -> Outcome {
        let hash = request.key_hash();
        loop {
            let slot = self.index.find_or_create(hash);
            let entry = self.skip_and_invalidate(slot.entry.address(), request);
            let head = self.hlog.head.load();
            let (gate, guard) = if mode.phase == super::state::Phase::Rest {
                (super::store::Gate::Normal, super::store::LockGuard::none())
            } else {
                self.cpr_gate(mode, hash, self.latest_version(entry, head))
            };
            match gate {
                super::store::Gate::Shift => return Outcome::Shift,
                super::store::Gate::RetryLater => {
                    return match self.mark_pending(mode, hash, guard) {
                        Some(marked) => Outcome::RetryLater { marked },
                        None => Outcome::Shift,
                    };
                }
                _ => {}
            }
            let mut address = entry;
            while address > request.search_min && address >= head {
                let record = self.record(address);
                // SAFETY: published, in-memory record under epoch protection.
                if request.key_equals(unsafe { record.key() }) {
                    return Outcome::Done(Status::Aborted);
                }
                address = record.info().previous();
            }
            if address < request.search_min {
                // The source is not on the key's chain any more.
                return Outcome::Done(Status::Aborted);
            }
            if address > request.search_min {
                return match self.mark_pending(mode, hash, guard) {
                    Some(marked) => Outcome::Disk {
                        address,
                        observed: entry,
                        marked,
                    },
                    None => Outcome::Shift,
                };
            }
            let new_address = self.block_allocate(slot_id, (request.words.len() * 8) as u64);
            let record = self.record(new_address);
            let info = RecordInfo::new(entry, mode.version, false);
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_from(info, request.words) };
            if slot.try_update(new_address) {
                return Outcome::Done(Status::Ok);
            }
            record.set_invalid();
        }
    }
}

/// Continues a copy whose liveness check reached the device.
#[derive(Default)]
pub(crate) struct CopyBatch {
    pub pending: std::cell::Cell<usize>,
    pub failure: std::cell::Cell<Option<Status>>,
}

pub(crate) struct PendingCopy<K, V> {
    pub words: Vec<u64>,
    pub search_min: Address,
    pub types: std::marker::PhantomData<(K, V)>,
    pub batch: std::rc::Rc<CopyBatch>,
    /// Index entry observed by the attempt that went to the device.
    pub observed: Address,
}

impl<K, V> Drop for PendingCopy<K, V> {
    fn drop(&mut self) {
        self.batch.pending.set(self.batch.pending.get() - 1);
    }
}

impl<K, V> PendingCopy<K, V> {
    fn request(&self) -> CopyRequest<'_, K, V> {
        CopyRequest {
            words: &self.words,
            search_min: self.search_min,
            types: std::marker::PhantomData,
        }
    }
}

impl<K: Key, V: Value> PendingOp<K, V> for PendingCopy<K, V> {
    fn hash(&self) -> u64 {
        self.request().key_hash()
    }

    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
        mode: OpMode,
        disk: Option<DiskRecord>,
    ) -> Step {
        let Some(mut disk) = disk else {
            self.batch.failure.set(Some(Status::IoError));
            return Step::Done;
        };
        let record = match disk.record::<K, V>(store.hlog.page_size()) {
            Ok(record) => record,
            Err(DiskError::NeedMore(len)) => {
                return Step::Read {
                    address: disk.address,
                    len,
                    marked: mode.holds_old,
                };
            }
            Err(DiskError::Corruption) => {
                self.batch.failure.set(Some(Status::Corruption));
                return Step::Done;
            }
        };
        // SAFETY: the record was fully written before it was flushed.
        if record.info().is_null() || self.request().key_equals(unsafe { record.key() }) {
            return Step::Done;
        }
        let previous = record.info().previous();
        if previous > self.search_min {
            return Step::Read {
                address: previous,
                len: Layout::<K, V>::min_io_size(),
                marked: mode.holds_old,
            };
        }
        if previous < self.search_min {
            return Step::Done;
        }
        // Everything older than `observed` is clean; only newer records need
        // checking from here on.
        self.search_min = self.observed;
        self.retry(store, slot, mode)
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        match store.conditional_insert(slot, &self.request(), mode) {
            Outcome::Done(_) => Step::Done,
            Outcome::Disk {
                address,
                observed,
                marked,
            } => {
                self.observed = observed;
                Step::Read {
                    address,
                    len: Layout::<K, V>::min_io_size(),
                    marked,
                }
            }
            Outcome::RetryLater { marked } => Step::RetryLater { marked },
            Outcome::Shift => Step::RetryLater { marked: false },
        }
    }
}

impl<'a, K: Key, V: Value> super::session::Session<'a, K, V> {
    /// Copies live records below until, optionally truncating and checkpointing.
    /// Workers and budget-limited writers claim disjoint pages. Only one
    /// compaction may run at a time; the caller must start in REST.
    /// Err reports a failure before this call truncates the log. Once copying
    /// and optional truncation finish, checkpoint errors are returned inside
    /// CompactionResult and do not imply rollback of that completed work.
    pub fn compact(
        &mut self,
        until: Address,
        shift_begin: bool,
        threads: usize,
        checkpoint: bool,
    ) -> Result<CompactionResult, Status> {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let store = self.store();
        if store.system_state().phase != super::state::Phase::Rest {
            return Err(Status::Aborted);
        }
        let _coordinator = store.maintenance.start().ok_or(Status::Aborted)?;
        let until = until
            .min(store.hlog.safe_read_only.load())
            .max(store.hlog.begin.load());
        self.validate_compaction_boundary(until)?;
        let work = std::sync::Arc::new(super::maintenance::CompactionWork::new(
            store.hlog.begin.load(),
            until,
        ));
        {
            let mut active = store.maintenance.work.lock().unwrap();
            if active.is_some() {
                return Err(Status::Aborted);
            }
            *active = Some(work.clone());
        }
        let helpers = threads.max(1) - 1;
        let finished = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..helpers)
                .map(|_| {
                    let (work, finished) = (&work, &finished);
                    scope.spawn(move || {
                        // The coordinator can consume every unclaimed page.
                        // Never wait for a slot held by a persistent user session.
                        if let Some(mut session) = store.try_start_session()
                            && let Err(status) = copy_pages(&mut session, work, usize::MAX)
                        {
                            work.fail(status);
                        }
                        finished.fetch_add(1, Ordering::AcqRel);
                    })
                })
                .collect();
            if let Err(status) = copy_pages(self, &work, usize::MAX) {
                work.fail(status);
            }
            while finished.load(Ordering::Acquire) < helpers {
                self.refresh();
                std::thread::yield_now();
            }
            for handle in handles {
                handle.join().expect("compaction worker panicked");
            }
        });
        // Closing registration under this lock prevents a late assistant from
        // entering after the final wait and before truncation.
        store.maintenance.work.lock().unwrap().take();
        while work.assistants.load(Ordering::Acquire) != 0 {
            self.refresh();
            std::thread::yield_now();
        }
        if let Some(status) = *work.failure.lock().unwrap() {
            return Err(status);
        }
        if shift_begin {
            // Another caller may already have moved begin past `until`.
            while store.hlog.begin.load() < until && !store.shift_begin_address(until, None, None) {
                self.refresh();
                std::thread::yield_now();
            }
            self.complete_pending(true);
        }
        let checkpoint = if checkpoint {
            let failed = std::sync::Arc::new(AtomicBool::new(false));
            let index_failed = failed.clone();
            let log_failed = failed.clone();
            match store.checkpoint(
                Some(Box::new(move |status| {
                    if status != Status::Ok {
                        index_failed.store(true, Ordering::Release);
                    }
                })),
                Some(std::sync::Arc::new(move |status, _| {
                    if status != Status::Ok {
                        log_failed.store(true, Ordering::Release);
                    }
                })),
            ) {
                Some(token) => {
                    self.complete_pending(true);
                    Some(if failed.load(Ordering::Acquire) {
                        Err(Status::IoError)
                    } else {
                        Ok(token)
                    })
                }
                None => Some(Err(Status::Aborted)),
            }
        } else {
            None
        };
        Ok(CompactionResult {
            until,
            begin_address: store.hlog.begin.load(),
            checkpoint,
        })
    }

    /// Reject a caller-supplied partial record before copying any pages.
    fn validate_compaction_boundary(&self, until: Address) -> Result<(), Status> {
        let store = self.store();
        if until.offset() == 0 || until == store.hlog.begin.load() {
            return Ok(());
        }
        let mut scanner = super::scan::LogScanner::for_session(
            store,
            Address::new(until.page(), 0),
            Address::new(until.page() + 1, 0),
        );
        let Some(mut page) = self.copy_scan_page(&mut scanner)? else {
            return Err(Status::InvalidArgument);
        };
        let mut boundary = false;
        for record in page.records() {
            if record.address == until || record.address.add(record.size() as u64) == until {
                boundary = true;
                break;
            }
            if record.address > until {
                break;
            }
        }
        if let Some(status) = page.error() {
            return Err(status);
        }
        if boundary {
            Ok(())
        } else {
            Err(Status::InvalidArgument)
        }
    }

    /// Called only from configured maintenance paths, never the plain path.
    #[cold]
    pub(crate) fn assist_compaction(&mut self) {
        use std::sync::atomic::Ordering;
        let store = self.store();
        if store.maintenance.budget == 0 || store.log_size() < store.maintenance.budget {
            return;
        }
        // User completions belong to complete_pending, not to an unrelated
        // write. They may also hold CPR locks needed by a new copy batch.
        if self.pending_count() != 0 {
            return;
        }
        let work = {
            let active = store.maintenance.work.lock().unwrap();
            let Some(work) = active.as_ref() else {
                return;
            };
            work.assistants.fetch_add(1, Ordering::AcqRel);
            work.clone()
        };
        struct Assistant<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for Assistant<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _assistant = Assistant(&work.assistants);
        if let Err(status) = copy_pages(self, &work, 4) {
            work.fail(status);
        }
    }
}

/// Copies a bounded batch of pages; completing pending copies before returning
/// is essential because truncation waits for page owners, not queued requests.
fn copy_pages<K: Key, V: Value>(
    session: &mut super::session::Session<'_, K, V>,
    work: &super::maintenance::CompactionWork,
    limit: usize,
) -> Result<(), Status> {
    let Some(mut start) = work.claim() else {
        return Ok(());
    };
    let mut scanner = super::scan::LogScanner::for_session(session.store(), work.begin, work.until);
    let batch = std::rc::Rc::new(CopyBatch::default());
    let mut copies = 0u64;
    let result = (|| {
        for index in 0..limit {
            if index != 0 {
                let Some(next) = work.claim() else { break };
                start = next;
            }
            scanner.seek(start);
            let Some(mut page) = session.copy_scan_page(&mut scanner)? else {
                break;
            };
            for record in page.records() {
                if record.info.invalid() || record.info.tombstone() {
                    continue;
                }
                let request = CopyRequest {
                    // SAFETY: this scan page belongs only to this copy batch;
                    // its typed key and value are never shared or mutated.
                    words: unsafe { record.raw_words() },
                    types: std::marker::PhantomData,
                    search_min: record.address,
                };
                match session.copy_record(request, &batch) {
                    Status::Ok | Status::Aborted | Status::Pending => {}
                    status => return Err(status),
                }
                copies += 1;
                if copies.is_multiple_of(512) {
                    session.complete_pending(false);
                }
            }
            if let Some(status) = page.error() {
                return Err(status);
            }
            session.refresh();
        }
        Ok(())
    })();
    // Even a later scan failure must not leave an earlier copy outstanding
    // when this page owner leaves and the coordinator considers truncation.
    while batch.pending.get() != 0 {
        session.complete_pending(false);
        std::thread::yield_now();
    }
    result?;
    match batch.failure.get() {
        Some(status) => Err(status),
        None => Ok(()),
    }
}
