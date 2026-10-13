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

/// A record being copied: its bytes and the chain range left to check.
pub(crate) struct CopyRequest<K, V> {
    pub words: Vec<u64>,
    pub types: std::marker::PhantomData<(K, V)>,
    /// Records above this address may still hide a newer version of the key;
    /// it starts at the address of the record being copied.
    pub search_min: Address,
}

impl<K: Key, V> KeyContext<K> for CopyRequest<K, V> {
    fn key(&self) -> &K {
        // SAFETY: words owns a complete aligned record copied from a scan.
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
        request: &mut CopyRequest<K, V>,
        mode: OpMode,
    ) -> Outcome {
        let hash = request.key_hash();
        loop {
            let slot = self.index.find_or_create(hash);
            let entry = self.skip_and_invalidate(slot.entry.address(), request);
            let head = self.hlog.head.load();
            let (gate, _guard) = if mode.phase == super::state::Phase::Rest {
                (super::store::Gate::Normal, super::store::LockGuard::none())
            } else {
                self.cpr_gate(mode, hash, self.latest_version(entry, head))
            };
            match gate {
                super::store::Gate::Shift => return Outcome::Shift,
                super::store::Gate::RetryLater => {
                    return Outcome::RetryLater {
                        marked: mode.holds_old,
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
                return Outcome::Disk {
                    address,
                    observed: entry,
                    marked: mode.holds_old,
                };
            }
            let new_address = self.block_allocate(slot_id, (request.words.len() * 8) as u64);
            let record = self.record(new_address);
            let info = RecordInfo::new(entry, mode.version, false);
            // SAFETY: freshly allocated and not yet published.
            unsafe { record.initialize_from(info, &request.words) };
            if slot.try_update(new_address) {
                return Outcome::Done(Status::Ok);
            }
            record.set_invalid();
        }
    }
}

/// Continues a copy whose liveness check reached the device.
pub(crate) struct PendingCopy<K, V> {
    pub request: CopyRequest<K, V>,
    /// Index entry observed by the attempt that went to the device.
    pub observed: Address,
}

impl<K: Key, V: Value> PendingOp<K, V> for PendingCopy<K, V> {
    fn hash(&self) -> u64 {
        self.request.key_hash()
    }

    fn resume(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
        mode: OpMode,
        disk: Option<DiskRecord>,
    ) -> Step {
        let Some(mut disk) = disk else {
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
            Err(DiskError::Corruption) => return Step::Done,
        };
        // SAFETY: the record was fully written before it was flushed.
        if record.info().is_null() || self.request.key_equals(unsafe { record.key() }) {
            return Step::Done;
        }
        let previous = record.info().previous();
        if previous > self.request.search_min {
            return Step::Read {
                address: previous,
                len: Layout::<K, V>::min_io_size(),
                marked: mode.holds_old,
            };
        }
        if previous < self.request.search_min {
            return Step::Done;
        }
        // Everything older than `observed` is clean; only newer records need
        // checking from here on.
        self.request.search_min = self.observed;
        self.retry(store, slot, mode)
    }

    fn retry(&mut self, store: &FasterKv<K, V>, slot: &SlotId, mode: OpMode) -> Step {
        match store.conditional_insert(slot, &mut self.request, mode) {
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
    /// Compacts the log below `until` (capped at the safe read-only address)
    /// with `threads` workers, this session included, and with `shift_begin`
    /// truncates the log at `until` afterwards (FASTER `CompactWithLookup`).
    /// Must start in REST. Returns the address compacted up to.
    pub fn compact(
        &mut self,
        until: Address,
        shift_begin: bool,
        threads: usize,
    ) -> Result<Address, Status> {
        let store = self.store();
        let scanner = store.scan(store.hlog.begin.load(), until);
        let until = scanner.until();
        let scanner = std::sync::Mutex::new(scanner);
        let finished = std::sync::atomic::AtomicUsize::new(0);
        let helpers = threads.max(1) - 1;
        let result = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..helpers)
                .map(|_| {
                    let (scanner, finished) = (&scanner, &finished);
                    scope.spawn(move || {
                        // An action may be starting; this caller keeps
                        // refreshing, so it will finish and REST will return.
                        let mut session = loop {
                            if let Some(session) = store.try_start_session() {
                                break session;
                            }
                            std::thread::yield_now();
                        };
                        let result = copy_pages(&mut session, scanner);
                        finished.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        result
                    })
                })
                .collect();
            let mut result = copy_pages(self, &scanner);
            // Keep this session refreshing so the helpers are never blocked
            // on its epoch or on a phase barrier.
            while finished.load(std::sync::atomic::Ordering::Acquire) < helpers {
                self.refresh();
                std::thread::yield_now();
            }
            for handle in handles {
                result = result.and(handle.join().expect("compaction worker panicked"));
            }
            result
        });
        drop(scanner);
        result?;
        if shift_begin {
            while !store.shift_begin_address(until, None, None) {
                self.refresh();
                std::thread::yield_now();
            }
            self.complete_pending(true);
        }
        Ok(until)
    }
}

/// Copies live records from pages taken from `scanner` until it runs dry.
fn copy_pages<K: Key, V: Value>(
    session: &mut super::session::Session<'_, K, V>,
    scanner: &std::sync::Mutex<super::scan::LogScanner<'_, K, V>>,
) -> Result<(), Status> {
    let mut copies = 0u64;
    loop {
        let page = scanner.lock().unwrap().next_page()?;
        let Some(mut page) = page else {
            break;
        };
        for record in page.records() {
            if record.info.invalid() || record.info.tombstone() {
                continue;
            }
            let request = CopyRequest {
                words: record.words.to_vec(),
                types: std::marker::PhantomData,
                search_min: record.address,
            };
            let _ = session.copy_record(request);
            copies += 1;
            if copies.is_multiple_of(512) {
                session.complete_pending(false);
            }
        }
        session.refresh();
    }
    session.complete_pending(true);
    Ok(())
}
