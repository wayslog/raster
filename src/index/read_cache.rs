//! Read-only snapshot copies are validated against bucket publication stamps.
use super::*;

const CACHE_SLOTS: usize = 512;

#[derive(Clone, Copy)]
struct CachedEntry {
    entry: EntrySnapshot,
    stamp: u64,
}

#[derive(Default)]
pub(crate) struct SnapshotCache {
    entries: Vec<Option<CachedEntry>>,
    misses: usize,
    bypass: usize,
}

impl SnapshotCache {
    pub(super) fn bypass_probe(&mut self) -> bool {
        if self.bypass == 0 {
            return false;
        }
        self.bypass -= 1;
        true
    }

    fn insert(&mut self, slot: usize, entry: EntrySnapshot, stamp: u64) {
        self.misses += 1;
        if self.misses == CACHE_SLOTS {
            // A full cache worth of consecutive misses indicates a streaming
            // workload. Use canonical reads for a bounded interval, then probe
            // again so a later hot working set can recover without intervention.
            self.bypass = CACHE_SLOTS * 8;
            self.misses = 0;
        }
        if stamp & 1 != 0 {
            return;
        }
        if self.entries.is_empty() {
            if self.entries.try_reserve_exact(CACHE_SLOTS).is_err() {
                return;
            }
            self.entries.resize_with(CACHE_SLOTS, || None);
        }
        self.entries[slot] = Some(CachedEntry { entry, stamp });
    }
}

/// Created under the bucket lock, before its first mutation. A panic leaves
/// the stamp unstable until the poisoned bucket rejects the fallback reader.
pub(super) struct BucketPublication<'a> {
    stamp: &'a AtomicU64,
    stable: u64,
}

impl<'a> BucketPublication<'a> {
    pub(super) fn begin(stamp: &'a AtomicU64) -> Self {
        let previous = stamp.load(PUBLISH_ORDER);
        let unstable = previous.saturating_add(1) | 1;
        stamp.store(unstable, PUBLISH_ORDER);
        Self {
            stamp,
            stable: unstable.saturating_add(1),
        }
    }
}

impl Drop for BucketPublication<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            // MAX stays odd and permanently disables caching, preventing ABA.
            self.stamp.store(self.stable, PUBLISH_ORDER);
        }
    }
}

impl Table {
    pub(super) fn prepare_cached(
        &self,
        hash: KeyHash,
        cache: &mut SnapshotCache,
    ) -> Result<EntrySnapshot, Error> {
        let bucket = hash.0 as usize & (self.buckets.len() - 1);
        let slot = hash.0 as usize & (CACHE_SLOTS - 1);
        if let Some(Some(cached)) = cache.entries.get(slot)
            && cached.entry.hash == Some(hash)
            && cached.entry.owner == self.owner
            && cached.entry.table_generation == self.generation
            && cached.stamp == self.publications[bucket].load(PUBLISH_ORDER)
            && !self.buckets[bucket].is_poisoned()
        {
            // Only the session-owned Copy is read here, never the mutable bucket.
            cache.misses = 0;
            return Ok(cached.entry);
        }
        let entries = self.buckets[bucket]
            .lock()
            .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
        let entry = self.entry(bucket, hash, &entries);
        // Capture the stamp and snapshot under the same lock. Later publication
        // either invalidates this entry or follows the read's linearization.
        cache.insert(slot, entry, self.publications[bucket].load(PUBLISH_ORDER));
        Ok(entry)
    }
}

#[cfg(test)]
#[path = "read_cache_tests.rs"]
mod tests;
