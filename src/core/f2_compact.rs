//! Explicit F2 log compaction and migration.

use super::{Address, F2Kv, F2Session, Key, KeyContext, Session, Status, UpsertContext, Value};
use crate::core::auto_compact::{AutoCompaction, AutoCompactionConfig};
use crate::core::cold_index::{ColdIndexEntry, ColdIndexOutcome};
use crate::core::maintenance::CompactionWork;
use crate::core::pending::{DiskError, DiskRecord};
use crate::core::record::{Layout, Record};
use crate::core::{CompactionResult, FasterKv, ValueSlot};
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, mpsc};

impl<K: Key, V: Value> F2Kv<K, V> {
    /// The end of the hot prefix eligible for migration.
    pub fn hot_compaction_limit(&self) -> Address {
        self.hot.hlog.safe_head.load()
    }

    /// The end of the cold prefix eligible for compaction.
    pub fn cold_compaction_limit(&self) -> Address {
        self.cold.data_store().hlog.safe_head.load()
    }

    /// Copies the live hot prefix to cold storage, including tombstones.
    pub fn compact_hot(&self, until: Address, threads: usize) -> Result<CompactionResult, Status> {
        self.compact_prefix(until, true, threads)
    }

    /// Rewrites the live records in the cold prefix to its tail.
    pub fn compact_cold(&self, until: Address, threads: usize) -> Result<CompactionResult, Status> {
        self.compact_prefix(until, false, threads)
    }

    /// Compacts each configured layer when it crosses its log budget.
    /// At least one layer must be configured.
    pub fn spawn_auto_compaction(
        self: &Arc<Self>,
        hot: Option<AutoCompactionConfig>,
        cold: Option<AutoCompactionConfig>,
    ) -> AutoCompaction {
        assert!(
            hot.iter()
                .chain(cold.iter())
                .all(|config| config.log_size_budget > 0),
            "auto compaction needs a log size budget"
        );
        let interval = hot
            .iter()
            .chain(cold.iter())
            .map(|config| config.check_interval)
            .min()
            .expect("F2 auto compaction needs at least one log budget");
        let store = self.clone();
        let now = std::time::Instant::now();
        let mut hot_due = now;
        let mut cold_due = now;
        AutoCompaction::spawn(interval, move || {
            let now = std::time::Instant::now();
            if let Some(config) = hot
                && now >= hot_due
            {
                store.auto_compact_layer(&config, true);
                hot_due = now + config.check_interval;
            }
            if let Some(config) = cold
                && now >= cold_due
            {
                store.auto_compact_layer(&config, false);
                cold_due = now + config.check_interval;
            }
        })
    }

    fn auto_compact_layer(&self, config: &AutoCompactionConfig, hot_source: bool) {
        let source = if hot_source {
            &self.hot
        } else {
            self.cold.data_store()
        };
        if let Some(until) = config.cutoff(source) {
            let _ = self.compact_prefix(until, hot_source, config.threads);
        }
    }

    fn compact_prefix(
        &self,
        until: Address,
        hot_source: bool,
        threads: usize,
    ) -> Result<CompactionResult, Status> {
        let _maintenance = self.maintenance.try_lock().map_err(|_| Status::Aborted)?;
        let source = if hot_source {
            &self.hot
        } else {
            self.cold.data_store()
        };
        let begin = source.hlog.begin.load();
        let until = until.min(source.hlog.safe_head.load()).max(begin);
        if until == begin {
            return Ok(CompactionResult {
                until,
                begin_address: begin,
                checkpoint: None,
            });
        }
        let mut session = self.try_start_session().ok_or(Status::OutOfMemory)?;
        let source_session = if hot_source {
            &session.hot
        } else {
            session.cold.data_session()
        };
        {
            let _epoch = source_session.epoch_scope();
            source_session.validate_compaction_boundary(until)?;
        }
        let work = CompactionWork::new(begin, until);
        std::thread::scope(|scope| {
            for _ in 1..threads {
                let work = &work;
                scope.spawn(move || {
                    if let Some(mut session) = self.try_start_session()
                        && let Err(status) = copy_pages(&mut session, source, work, hot_source)
                    {
                        work.fail(status);
                    }
                });
            }
            if let Err(status) = copy_pages(&mut session, source, &work, hot_source) {
                work.fail(status);
            }
        });
        if let Some(status) = *work.failure.lock().unwrap() {
            return Err(status);
        }

        // Files stay available for pending reads and previously published
        // checkpoints. Only the logical source boundary changes here.
        if hot_source {
            let _cut = self.hot_cut.write().unwrap();
            if source.hlog.begin.monotonic_update(until) {
                source
                    .truncations
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                session.with_hot(|_| {
                    for chunk in 0..source.index.chunks() {
                        source.index.garbage_collect_chunk(chunk, until, &|entry| {
                            source.skip_read_cache(entry)
                        });
                    }
                });
            }
        } else {
            source.hlog.begin.monotonic_update(until);
            self.cold.reclaim_index_prefix();
        }
        Ok(CompactionResult {
            until,
            begin_address: source.hlog.begin.load(),
            checkpoint: None,
        })
    }
}

fn copy_pages<K: Key, V: Value>(
    session: &mut F2Session<'_, K, V>,
    source: &FasterKv<K, V>,
    work: &CompactionWork,
    hot_source: bool,
) -> Result<(), Status> {
    let mut scan = source.scan(work.begin, work.until);
    let mut serial = 0;
    while let Some(start) = work.claim() {
        scan.seek(start);
        let Some(mut page) = scan.next_page()? else {
            break;
        };
        for record in page.records() {
            if record.info.invalid() {
                continue;
            }
            serial += 1;
            let hash = record.key.hash();
            let origin = record.address;
            if hot_source {
                let head = session.with_hot(|hot| hot.rmw_head(hash));
                if !is_live(&session.hot, record.key, origin, head)? {
                    continue;
                }
            }
            loop {
                let completed = Rc::new(Cell::new(None));
                let result = session.cold.lookup_head(hash, serial, {
                    let completed = completed.clone();
                    move |result| completed.set(Some(result))
                });
                let result = if result == Err(Status::Pending) {
                    session.cold.complete_pending(true);
                    completed.take().expect("cold index lookup completed")
                } else {
                    result
                };
                let ColdIndexOutcome::Entry(expected) = result? else {
                    unreachable!("lookup cannot conflict");
                };
                if !hot_source {
                    let ColdIndexEntry::Published(head) = expected else {
                        break;
                    };
                    if !is_live(session.cold.data_session(), record.key, origin, head)? {
                        break;
                    }
                }
                // SAFETY: The scan page belongs to this worker, which only
                // reads its typed key and value.
                let words = unsafe { record.raw_words() };
                let mut copy = MigrationRecord::<K, V> {
                    words: Rc::from(words),
                    status: Rc::new(Cell::new(None)),
                    types: PhantomData,
                };
                let status =
                    session
                        .cold
                        .copy_if_head(&mut copy, expected, record.info.tombstone(), serial);
                let status = if status == Status::Pending {
                    session.cold.complete_pending(true);
                    copy.status.take().expect("cold copy completed")
                } else {
                    status
                };
                match status {
                    Status::Ok => break,
                    Status::Aborted => continue,
                    status => return Err(status),
                }
            }
        }
        if let Some(status) = page.error() {
            return Err(status);
        }
    }
    Ok(())
}

/// Tests whether the source is reachable and no later record shadows it.
/// The foreground may publish newer hot records after this check; those
/// records stay above the cutoff and take precedence over the cold copy.
fn is_live<K: Key, V: Value>(
    session: &Session<'_, K, V>,
    key: &K,
    origin: Address,
    mut cursor: Address,
) -> Result<bool, Status> {
    let store = session.store();
    while cursor > origin {
        {
            let _epoch = session.epoch_scope();
            let head = store.hlog.head.load();
            while cursor > origin && cursor >= head {
                let record = store.record(cursor);
                // SAFETY: The epoch protects the published record.
                if unsafe { record.key() } == key {
                    return Ok(false);
                }
                cursor = record.info().previous();
            }
        }
        if cursor > origin {
            let mut len = Layout::<K, V>::min_io_size();
            loop {
                let (tx, rx) = mpsc::channel();
                store
                    .hlog
                    .read_async(cursor, len, Box::new(move |result| drop(tx.send(result))));
                let bytes = rx
                    .recv()
                    .map_err(|_| Status::IoError)?
                    .map_err(|_| Status::IoError)?;
                if bytes.len() != len {
                    return Err(Status::IoError);
                }
                let mut disk = DiskRecord::new(bytes, cursor);
                match disk.record::<K, V>(store.hlog.page_size()) {
                    Ok(record) => {
                        // SAFETY: The owned disk record has a validated key.
                        if unsafe { record.key() } == key {
                            return Ok(false);
                        }
                        cursor = record.info().previous();
                        break;
                    }
                    Err(DiskError::NeedMore(needed)) => len = needed,
                    Err(DiskError::Corruption) => return Err(Status::Corruption),
                }
            }
        }
    }
    Ok(cursor == origin)
}

struct MigrationRecord<K, V> {
    words: Rc<[u64]>,
    status: Rc<Cell<Option<Status>>>,
    types: PhantomData<(K, V)>,
}

impl<K, V> Clone for MigrationRecord<K, V> {
    fn clone(&self) -> Self {
        Self {
            words: self.words.clone(),
            status: self.status.clone(),
            types: PhantomData,
        }
    }
}

impl<K: Key, V: Value> MigrationRecord<K, V> {
    fn record(&self) -> Record<K, V> {
        // SAFETY: This private allocation contains a complete aligned record
        // copied from a scan page. All accesses through this pointer are reads.
        unsafe { Record::from_ptr(self.words.as_ptr().cast_mut().cast()) }
    }
}

// SAFETY: The context owns a complete aligned key through all pending clones.
unsafe impl<K: Key, V: Value> KeyContext<K> for MigrationRecord<K, V> {
    fn key(&self) -> &K {
        // SAFETY: The scan key lives as long as this context.
        unsafe {
            &*self
                .words
                .as_ptr()
                .cast::<u8>()
                .add(Layout::<K, V>::KEY_OFFSET)
                .cast()
        }
    }

    unsafe fn write_key(&self, dst: *mut K) {
        // SAFETY: The raw allocation includes the entire key, and the caller
        // supplied a disjoint destination of that same size.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.words
                    .as_ptr()
                    .cast::<u8>()
                    .add(Layout::<K, V>::KEY_OFFSET),
                dst.cast::<u8>(),
                self.key().size() as usize,
            );
        }
    }
}

// SAFETY: put copies the complete immutable value to its unpublished slot.
unsafe impl<K: Key, V: Value> UpsertContext<K, V> for MigrationRecord<K, V> {
    fn value_size(&self) -> u32 {
        // SAFETY: The value header is part of the owned scan record.
        unsafe { self.record().value().size() }
    }

    fn put(&mut self, mut slot: ValueSlot<'_, V>) {
        // SAFETY: Both disjoint ranges contain the full value allocation.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.record().value_ptr().cast::<u8>(),
                slot.as_mut_ptr().cast::<u8>(),
                slot.len() as usize,
            );
        }
    }

    fn put_atomic(&mut self, _: &V) -> bool {
        false
    }

    fn on_complete(&mut self, status: Status) {
        self.status.set(Some(status));
    }
}
