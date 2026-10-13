//! Read cache (FASTER `read_cache.h`): copies of records read from the device,
//! kept in a second in-memory log in front of the hash chains.
//!
//! An index entry may point at a read-cache record (address bit 47); that
//! record's `previous` is the hybrid-log chain head, so at most one cached
//! record sits in front of each chain. Reads check it first. Writers skip it,
//! invalidating it if it holds their key, and link new records to the log
//! address behind it. Before the cache log's head passes a page, eviction
//! points every entry still referring to a record there back at the log.
use super::address::Address;
use super::epoch::LightEpoch;
use super::hlog::{Allocation, HybridLog, LogConfig};
use super::index::Slot;
use super::kv::{Key, KeyContext, ReadContext, Value};
use super::record::{Layout, Record, RecordInfo};
use super::store::{FasterKv, Status};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
pub struct ReadCacheConfig {
    /// Allocation hint; cache frames are currently always allocated eagerly.
    pub pre_allocate: bool,
    /// Geometry of the cache log. Records in its read-only part are copied
    /// to its tail when read again, so hot records survive eviction.
    pub log: LogConfig,
}

impl Default for ReadCacheConfig {
    /// FASTER's defaults: 256 MiB, 90% mutable.
    fn default() -> Self {
        Self {
            pre_allocate: true,
            log: LogConfig {
                page_bits: 25,
                memory_pages: 8,
                mutable_fraction: 0.9,
            },
        }
    }
}

pub(crate) struct ReadCache {
    pub log: HybridLog,
    /// Cache-log address below which eviction has unlinked every record.
    evicted_to: Mutex<Address>,
}

impl ReadCache {
    pub fn new(config: ReadCacheConfig, epoch: Arc<LightEpoch>) -> Self {
        let log = HybridLog::new(config.log, epoch, Arc::new(super::device::NullDevice));
        let start = log.begin.load();
        Self {
            log,
            evicted_to: Mutex::new(start),
        }
    }
}

impl<K: Key, V: Value> FasterKv<K, V> {
    #[inline(always)]
    fn cache(&self) -> &ReadCache {
        self.read_cache
            .as_ref()
            .expect("read-cache addresses only exist with a read cache")
    }

    /// The cache record behind an index-entry address with the cache bit.
    #[inline(always)]
    fn cache_record(&self, entry: Address) -> Record<K, V> {
        // SAFETY: an entry is unlinked before its cache page closes, and the
        // caller loaded it under epoch protection, so the frame is still live.
        unsafe { Record::from_ptr(self.cache().log.get(entry.read_cache_address())) }
    }

    /// The hybrid-log address an entry address leads to (FASTER `Skip`).
    #[inline(always)]
    pub(crate) fn skip_read_cache(&self, entry: Address) -> Address {
        if entry.in_read_cache() {
            self.cache_record(entry).info().previous()
        } else {
            entry
        }
    }

    /// Like [`FasterKv::skip_read_cache`], also invalidating the cache record
    /// if it holds `key`, before a writer replaces it (FASTER `SkipAndInvalidate`).
    #[inline(always)]
    pub(crate) fn skip_and_invalidate<C: KeyContext<K>>(&self, entry: Address, ctx: &C) -> Address {
        if entry.in_read_cache() {
            self.invalidate_cached(entry, ctx)
        } else {
            entry
        }
    }

    #[cold]
    #[inline(never)]
    fn invalidate_cached<C: KeyContext<K>>(&self, entry: Address, ctx: &C) -> Address {
        let record = self.cache_record(entry);
        // SAFETY: cache records are fully written before being linked.
        if ctx.key_equals(unsafe { record.key() }) {
            record.set_invalid();
        }
        record.info().previous()
    }

    /// Answers a read from the cache record at `entry` (FASTER
    /// `ReadCache::Read`); otherwise returns the log address to continue at.
    #[inline(never)]
    pub(crate) fn read_cached<C: ReadContext<K, V>>(
        &self,
        ctx: &mut C,
        slot: Slot<'_>,
        entry: Address,
    ) -> Result<Status, Address> {
        let record = self.cache_record(entry);
        let info = record.info();
        let previous = info.previous();
        // SAFETY: cache records are fully written before being linked.
        let matches = ctx.key_equals(unsafe { record.key() });
        // A copy of a truncated record must not outlive the truncation.
        if !matches || info.invalid() || previous < self.hlog.begin.load() {
            return Err(previous);
        }
        // SAFETY: as above; cache records are never updated in place.
        ctx.get(unsafe { record.value() });
        let cache = self.cache();
        if entry.read_cache_address() < cache.log.read_only.load()
            && self.insert_cached(slot.entry.address(), &record)
        {
            // Copied to the cache tail; this copy is now unreachable.
            record.set_invalid();
        }
        Ok(Status::Ok)
    }

    /// Caches `source`, the newest record of its key on the log chain behind
    /// `observed`, if the index entry still holds `observed` (FASTER
    /// `TryInsert`). Best effort: gives up rather than wait for space.
    pub(crate) fn insert_cached(&self, observed: Address, source: &Record<K, V>) -> bool {
        let Some(cache) = self.read_cache.as_ref() else {
            return false;
        };
        // SAFETY: `source` is a complete record.
        let key = unsafe { source.key() };
        let Some(slot) = self.index.find(key.hash()) else {
            return false;
        };
        if slot.entry.address() != observed {
            return false;
        }
        // Still linked, so a cache record at `observed` is not evicted yet.
        let previous = self.skip_read_cache(observed);
        // SAFETY: the source is complete and immutable.
        let size = unsafe { source.size() } as u64;
        let address = match cache.log.allocate(size) {
            Allocation::Ok(address) => address,
            Allocation::PageFull(page) | Allocation::Retry(page) => {
                cache
                    .log
                    .new_page_with(page, |from, to| self.evict_cached(from, to));
                return false;
            }
        };
        // SAFETY: the frame of a freshly allocated address is live.
        let record = unsafe { Record::<K, V>::from_ptr(cache.log.get(address)) };
        // SAFETY: freshly allocated and not yet linked.
        unsafe { record.initialize_copy(RecordInfo::new(previous, 0, false), source) };
        if slot.try_update(address.to_read_cache()) {
            true
        } else {
            record.set_invalid();
            false
        }
    }

    /// Points every index entry that refers to a cache record in
    /// `[from, to)` back at the log (FASTER `ReadCache::Evict`). Runs before
    /// the cache head passes `to`; concurrent callers wait for each other.
    #[cold]
    fn evict_cached(&self, from: Address, to: Address) {
        let cache = self.cache();
        let mut evicted = cache.evicted_to.lock().unwrap();
        debug_assert!(*evicted >= from);
        let min_size = Layout::<K, V>::min_io_size() as u64;
        let page_size = cache.log.page_size();
        let mut address = *evicted;
        while address < to {
            if address.offset() + min_size > page_size {
                address = Address::new(address.page() + 1, 0);
                continue;
            }
            // SAFETY: the cache head has not passed `to` yet.
            let record = unsafe { Record::<K, V>::from_ptr(cache.log.get(address)) };
            let info = record.info();
            if info.is_null() {
                // Unused space at the end of the page.
                address = Address::new(address.page() + 1, 0);
                continue;
            }
            // Invalid records may still be linked (a writer invalidates
            // before its own update), so unlink them as well.
            // SAFETY: records below the read-only boundary are complete.
            if let Some(mut slot) = self.index.find(unsafe { record.key() }.hash()) {
                let linked = address.to_read_cache();
                while slot.entry.address() == linked && !slot.try_update(info.previous()) {
                    slot = slot.reload();
                }
            }
            // SAFETY: complete record in the cache region being evicted.
            address = address.add(unsafe { record.size() } as u64);
        }
        *evicted = (*evicted).max(to);
    }
}
