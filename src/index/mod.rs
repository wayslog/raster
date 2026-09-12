//! Each bucket independently controls its lock for conditional release. A same-tag
//! shared chain head is checked for key equality through the log chain.
use crate::{
    config::IndexConfig,
    sync::{AtomicU64, Mutex, PUBLISH_ORDER},
    types::*,
};
const SLOTS: usize = 7;
static NEXT_INDEX: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexHead {
    Empty,
    Log(LogAddress),
    Cache(CacheAddress),
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntrySnapshot {
    owner: u64,
    pub bucket: usize,
    pub tag: u16,
    pub head: IndexHead,
    /// Slots that have occupied labels but do not yet have log addresses can still be found by blind deletion.
    pub present: bool,
    pub table_generation: Generation,
    revision: u64,
    hash: Option<KeyHash>,
}
// The full hash is only used for post-migration relocations,Does not change the equality semantics of the same physical entry.
impl PartialEq for EntrySnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.owner == other.owner
            && self.bucket == other.bucket
            && self.tag == other.tag
            && self.head == other.head
            && self.present == other.present
            && self.table_generation == other.table_generation
            && self.revision == other.revision
    }
}
impl Eq for EntrySnapshot {}
pub(crate) struct IndexImage {
    pub buckets: usize,
    pub generation: Generation,
    pub entries: Vec<EntrySnapshot>,
}
impl IndexImage {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let owner = self.entries.first().map(|entry| entry.owner);
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.entries.len())
            .map_err(|_| Error::OutOfMemory)?;
        for entry in &self.entries {
            if Some(entry.owner) != owner || entry.table_generation != self.generation {
                return Err(Error::InvalidState(
                    "Index image contains different identities or generations",
                ));
            }
            let IndexHead::Log(address) = entry.head else {
                return Err(Error::InvalidState(
                    "Persistent indexes only allow log addresses",
                ));
            };
            entries.push(crate::format::IndexEntry {
                bucket: entry.bucket as u64,
                tag: entry.tag,
                address,
            });
        }
        entries.sort_unstable_by_key(|entry| (entry.bucket, entry.tag));
        crate::format::IndexSnapshot {
            buckets: self.buckets as u64,
            generation: self.generation,
            entries,
        }
        .encode()
    }
}
#[derive(Debug)]
pub(crate) enum PublishResult {
    Published,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Re-prepare index entry after engine conflict;The protocol test checks the complete snapshot returned by this conflict"
        )
    )]
    Conflict(EntrySnapshot),
}
#[derive(Clone, Copy)]
struct Entry {
    tag: u16,
    head: IndexHead,
    revision: u64,
}
struct Bucket {
    blocks: Vec<[Option<Entry>; SLOTS]>,
    // Even if GC All items released,Also keep the bucket monotonic revisions,Prevent empty slots from appearing again ABA.
    revision: u64,
    empty_revision: u64,
}
struct Table {
    owner: u64,
    buckets: Vec<Mutex<Bucket>>,
    generation: Generation,
}
impl Table {
    pub fn new(config: IndexConfig) -> Result<Self, Error> {
        if !config.buckets.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "index.buckets",
                reason: "The number of buckets must be a non-zero power of two",
            });
        }
        let owner = NEXT_INDEX
            .fetch_update(PUBLISH_ORDER, PUBLISH_ORDER, |n| n.checked_add(1))
            .map_err(|_| Error::CapacityExceeded)?;
        let mut buckets = Vec::new();
        buckets
            .try_reserve_exact(config.buckets)
            .map_err(|_| Error::OutOfMemory)?;
        for _ in 0..config.buckets {
            buckets.push(Mutex::new(Bucket {
                blocks: Vec::new(),
                revision: 0,
                empty_revision: 0,
            }));
        }
        Ok(Self {
            owner,
            buckets,
            generation: Generation(0),
        })
    }
    fn entry(&self, bucket: usize, hash: KeyHash, entries: &Bucket) -> EntrySnapshot {
        let tag = hash.tag();
        let entry = entries
            .blocks
            .iter()
            .flatten()
            .flatten()
            .find(|entry| entry.tag == tag);
        EntrySnapshot {
            owner: self.owner,
            bucket,
            tag,
            head: entry.map_or(IndexHead::Empty, |e| e.head),
            present: entry.is_some(),
            revision: entry.map_or(entries.empty_revision, |e| e.revision),
            table_generation: self.generation,
            hash: Some(hash),
        }
    }
    /// An empty link header also returns a snapshot with identity.,For use by first conditional release.
    pub fn prepare(&self, hash: KeyHash) -> Result<EntrySnapshot, Error> {
        let bucket = hash.0 as usize & (self.buckets.len() - 1);
        let entries = self.buckets[bucket]
            .lock()
            .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
        Ok(self.entry(bucket, hash, &entries))
    }
    /// The caller first completes value initialization and log publishing;When replacing, you also need to hold the source record arbitration.
    pub fn compare_publish(
        &self,
        expected: EntrySnapshot,
        head: IndexHead,
    ) -> Result<PublishResult, Error> {
        self.compare_publish_mode(expected, head, false)
    }
    fn compare_publish_mode(
        &self,
        expected: EntrySnapshot,
        head: IndexHead,
        reserve_empty: bool,
    ) -> Result<PublishResult, Error> {
        if expected.owner != self.owner
            || expected.table_generation != self.generation
            || expected.bucket >= self.buckets.len()
        {
            return Err(Error::InvalidState(
                "Index snapshot identity or generation invalid",
            ));
        }
        match head {
            IndexHead::Log(a) => a.validate()?,
            IndexHead::Cache(a) => a.validate()?,
            IndexHead::Empty => (),
        }
        let mut bucket = self.buckets[expected.bucket]
            .lock()
            .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
        let current = self.entry(
            expected.bucket,
            expected.hash.ok_or(Error::InvalidState(
                "Image entries are not licensed as releases",
            ))?,
            &bucket,
        );
        if current != expected {
            return Ok(PublishResult::Conflict(current));
        }
        let revision = bucket
            .revision
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        if head == IndexHead::Empty && !reserve_empty {
            for slot in bucket.blocks.iter_mut().flatten() {
                if slot.is_some_and(|entry| entry.tag == expected.tag) {
                    *slot = None;
                    break;
                }
            }
            // Empty slot history retained after removing entries,Old empty snapshots cannot be inserted and deleted again.
            bucket.revision = revision;
            bucket.empty_revision = revision;
            return Ok(PublishResult::Published);
        }
        let next = Entry {
            tag: expected.tag,
            head,
            revision,
        };
        if let Some(entry) = bucket
            .blocks
            .iter_mut()
            .flatten()
            .flatten()
            .find(|e| e.tag == expected.tag)
        {
            *entry = next;
        } else if let Some(slot) = bucket.blocks.iter_mut().flatten().find(|e| e.is_none()) {
            *slot = Some(next);
        } else {
            bucket
                .blocks
                .try_reserve(1)
                .map_err(|_| Error::OutOfMemory)?;
            let mut block = [None; SLOTS];
            block[0] = Some(next);
            bucket.blocks.push(block);
        }
        bucket.revision = revision;
        Ok(PublishResult::Published)
    }
    /// Bucket-by-bucket blur image;Do not claim cross-bucket transaction snapshots,The cache header must be normalized by the upper layer before calling.
    pub fn snapshot(&self) -> Result<IndexImage, Error> {
        let mut entries = Vec::new();
        for (number, mutex) in self.buckets.iter().enumerate() {
            let bucket = mutex
                .lock()
                .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
            for entry in bucket.blocks.iter().flatten().flatten() {
                if matches!(entry.head, IndexHead::Cache(_)) {
                    return Err(Error::InvalidState(
                        "Persistent indexes must first normalize cache addresses",
                    ));
                }
                if entry.head != IndexHead::Empty {
                    entries.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                    entries.push(EntrySnapshot {
                        owner: self.owner,
                        bucket: number,
                        tag: entry.tag,
                        head: entry.head,
                        present: true,
                        table_generation: self.generation,
                        revision: entry.revision,
                        hash: None,
                    });
                }
            }
        }
        Ok(IndexImage {
            buckets: self.buckets.len(),
            generation: self.generation,
            entries,
        })
    }
    /// Build new runtime identity from persistent entries;Replace after all success,Failure does not change the old index.
    /// The number of buckets must be consistent with the recovery configuration,Unable to arbitrarily expand allocation by untrusted disk fields.
    pub fn restore(&mut self, image: crate::format::IndexSnapshot) -> Result<(), Error> {
        image.validate()?;
        if usize::try_from(image.buckets).ok() != Some(self.buckets.len()) {
            return Err(Error::InvalidFormat(
                "The number of recovery index buckets does not match the configuration",
            ));
        }
        let mut restored = Self::new(IndexConfig {
            buckets: self.buckets.len(),
        })?;
        restored.generation = image.generation;
        for entry in image.entries {
            let bucket = restored.buckets[entry.bucket as usize]
                .get_mut()
                .map_err(|_| Error::InvalidState("Recovery index bucket lock poisoning"))?;
            if bucket
                .blocks
                .last()
                .is_none_or(|block| block[SLOTS - 1].is_some())
            {
                bucket
                    .blocks
                    .try_reserve(1)
                    .map_err(|_| Error::OutOfMemory)?;
                bucket.blocks.push([None; SLOTS]);
            }
            let slot = bucket
                .blocks
                .last_mut()
                .expect("Overflow block allocated")
                .iter_mut()
                .find(|entry| entry.is_none())
                .expect("There are still empty slots in the last block");
            *slot = Some(Entry {
                tag: entry.tag,
                head: IndexHead::Log(entry.address),
                revision: 0,
            });
        }
        *self = restored;
        Ok(())
    }
}

mod gc;
pub(crate) mod growth;
pub(crate) use growth::MemIndex;

#[cfg(test)]
mod tests {
    use super::*;
    fn index() -> MemIndex {
        MemIndex::new(IndexConfig { buckets: 2 }).unwrap()
    }
    #[test]
    fn persistent_images_do_not_contain_process_identity_revision_numbers_and_overflow_bucket_order_is_normalized()
     {
        let first = index();
        let second = index();
        for tag in 0..30 {
            for (index, tag) in [(&first, tag), (&second, 29 - tag)] {
                let hash = KeyHash((tag << 48) | (tag % 2));
                let expected = index.prepare(hash).unwrap();
                assert!(matches!(
                    index.compare_publish(expected, IndexHead::Log(LogAddress(tag * 64))),
                    Ok(PublishResult::Published)
                ));
            }
        }
        let mut image = first.snapshot().unwrap();
        let other = second.snapshot().unwrap();
        assert_ne!(image.entries[0].owner, other.entries[0].owner);
        let encoded = image.encode().unwrap();
        assert_eq!(encoded, other.encode().unwrap());
        for entry in &mut image.entries {
            entry.revision = u64::MAX;
        }
        assert_eq!(encoded, image.encode().unwrap());
        let decoded = crate::format::IndexSnapshot::decode(&encoded).unwrap();
        assert_eq!(decoded.buckets, 2);
        assert_eq!(decoded.entries.len(), 30);
        for entry in decoded.entries {
            assert_eq!(entry.bucket, u64::from(entry.tag) % 2);
            assert_eq!(entry.address, LogAddress(u64::from(entry.tag) * 64));
        }
    }
    #[test]
    fn recovery_index_rebuilds_the_overflow_bucket_with_a_new_identity_and_the_old_snapshot_cannot_be_published()
     {
        let mut index = index();
        let old = index.prepare(KeyHash(0)).unwrap();
        let image = crate::format::IndexSnapshot {
            buckets: 2,
            generation: Generation(9),
            entries: (0..30)
                .map(|tag| crate::format::IndexEntry {
                    bucket: 0,
                    tag,
                    address: LogAddress(u64::from(tag) * 64),
                })
                .collect(),
        };
        let expected = image.encode().unwrap();
        index.restore(image).unwrap();
        assert_eq!(index.snapshot().unwrap().encode().unwrap(), expected);
        assert_eq!(
            index.state.read().unwrap().active.buckets[0]
                .lock()
                .unwrap()
                .blocks
                .len(),
            5
        );
        assert!(
            index
                .compare_publish(old, IndexHead::Log(LogAddress(17)))
                .is_err()
        );
        for tag in 0..30 {
            let snapshot = index.prepare(KeyHash(tag << 48)).unwrap();
            assert_eq!(snapshot.head, IndexHead::Log(LogAddress(tag * 64)));
            assert_eq!(snapshot.table_generation, Generation(9));
            assert_eq!(snapshot.revision, 0);
        }
        let before = index.prepare(KeyHash(0)).unwrap();
        assert!(matches!(
            index.compare_publish(before, IndexHead::Log(LogAddress(2048))),
            Ok(PublishResult::Published)
        ));
        assert!(matches!(
            index.compare_publish(before, IndexHead::Log(LogAddress(4096))),
            Ok(PublishResult::Conflict(_))
        ));
    }
    #[test]
    fn an_invalid_recovery_image_does_not_change_existing_indexes_and_an_empty_image_can_be_recovered()
     {
        let mut index = index();
        let old = index.prepare(KeyHash(0)).unwrap();
        index
            .compare_publish(old, IndexHead::Log(LogAddress(64)))
            .unwrap();
        let before = index.prepare(KeyHash(0)).unwrap();
        let valid = crate::format::IndexSnapshot {
            buckets: 2,
            generation: Generation(0),
            entries: vec![],
        };
        let mut bad = valid.clone();
        bad.buckets = 4;
        assert!(index.restore(bad).is_err());
        assert_eq!(index.prepare(KeyHash(0)).unwrap(), before);
        let mut bad = valid.clone();
        bad.entries.push(crate::format::IndexEntry {
            bucket: 0,
            tag: 0,
            address: LogAddress::INVALID,
        });
        assert!(index.restore(bad).is_err());
        assert_eq!(index.prepare(KeyHash(0)).unwrap(), before);
        index.restore(valid).unwrap();
        assert!(index.locate(KeyHash(0)).unwrap().is_none());
        assert!(
            index
                .compare_publish(before, IndexHead::Log(LogAddress(128)))
                .is_err()
        );
    }
    #[test]
    fn persistent_image_rejects_mixed_identity_generation_cache_headers_and_duplicate_entries() {
        let first = index();
        for tag in 0..2 {
            let entry = first.prepare(KeyHash(tag << 48)).unwrap();
            first
                .compare_publish(entry, IndexHead::Log(LogAddress(tag)))
                .unwrap();
        }
        let mut image = first.snapshot().unwrap();
        image.entries[0].owner += 1;
        assert!(image.encode().is_err());
        let mut image = first.snapshot().unwrap();
        image.entries[0].table_generation = Generation(1);
        assert!(image.encode().is_err());
        let mut image = first.snapshot().unwrap();
        image.entries[0].head = IndexHead::Cache(CacheAddress(0));
        assert!(image.encode().is_err());
        let mut image = first.snapshot().unwrap();
        image.entries.push(image.entries[0]);
        assert!(image.encode().is_err());
    }
    #[test]
    fn real_key_encoding_collisions_along_the_log_chain_lookup_will_not_string_keys() {
        use crate::{
            config::LogConfig,
            log::HybridLog,
            schema::{
                KeyCodec,
                builtin::{AtomicU64Value, U64Key},
            },
        };
        let index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 512,
                memory_pages: 1,
                mutable_fraction: 0.5,
            },
            std::sync::Arc::new(AtomicU64Value),
        )
        .unwrap();
        let codec = U64Key;
        let a = 8969;
        let b = 9239;
        assert_eq!(codec.hash(&a).tag(), codec.hash(&b).tag());
        let empty = index.prepare(codec.hash(&a)).unwrap();
        let first = log
            .finish_initialization(log.reserve_record(&a.to_le_bytes(), None, 17).unwrap())
            .unwrap();
        index.compare_publish(empty, IndexHead::Log(first)).unwrap();
        let previous = index.prepare(codec.hash(&b)).unwrap();
        let second = log
            .finish_initialization(
                log.reserve_record(&b.to_le_bytes(), Some(first), 29)
                    .unwrap(),
            )
            .unwrap();
        index
            .compare_publish(previous, IndexHead::Log(second))
            .unwrap();
        for (key, expected) in [(a, 17), (b, 29)] {
            let IndexHead::Log(head) = index.locate(codec.hash(&key)).unwrap().unwrap().head else {
                panic!("Should be the main log chain head")
            };
            assert_eq!(
                log.find(&codec, &key, Some(head))
                    .unwrap()
                    .unwrap()
                    .read(|v| v)
                    .unwrap(),
                expected
            );
        }
        assert!(log.find(&codec, &0, Some(second)).unwrap().is_none());
        assert!(
            log.reserve_record(&a.to_le_bytes(), Some(LogAddress(500)), 31)
                .is_err()
        );
    }
    #[test]
    fn there_are_clear_results_for_changing_empty_keys_to_long_keys_and_corrupted_encodings() {
        use crate::{
            config::LogConfig,
            log::HybridLog,
            schema::builtin::{AtomicU64Value, ByteKey, U64Key},
        };
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 512,
                memory_pages: 1,
                mutable_fraction: 0.5,
            },
            std::sync::Arc::new(AtomicU64Value),
        )
        .unwrap();
        let first = log
            .finish_initialization(log.reserve_record(b"", None, 1).unwrap())
            .unwrap();
        let key = vec![255; 100];
        let second = log
            .finish_initialization(log.reserve_record(&key, Some(first), 2).unwrap())
            .unwrap();
        assert_eq!(
            log.find(&ByteKey, b"", Some(second))
                .unwrap()
                .unwrap()
                .read(|v| v)
                .unwrap(),
            1
        );
        assert_eq!(
            log.find(&ByteKey, &key, Some(second))
                .unwrap()
                .unwrap()
                .read(|v| v)
                .unwrap(),
            2
        );
        assert!(log.find(&U64Key, &0, Some(second)).is_err());
        assert!(log.reserve_record(&[0; 600], None, 3).is_err());
    }
    #[test]
    fn initialized_logging_release_and_failed_release_cleanup() {
        use crate::{config::LogConfig, log::HybridLog, schema::builtin::AtomicU64Value};
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 256,
                memory_pages: 1,
                mutable_fraction: 0.5,
            },
            std::sync::Arc::new(AtomicU64Value),
        )
        .unwrap();
        let index = index();
        let expected = index.prepare(KeyHash(0)).unwrap();
        let reserve = log.reserve(7).unwrap();
        let address = reserve.address().unwrap();
        assert!(log.lease(address).is_err());
        let first = log.finish_initialization(reserve).unwrap();
        index
            .compare_publish(expected, IndexHead::Log(first))
            .unwrap();
        let losing = log.finish_initialization(log.reserve(9).unwrap()).unwrap();
        let PublishResult::Conflict(current) = index
            .compare_publish(expected, IndexHead::Log(losing))
            .unwrap()
        else {
            panic!("Old snapshots must not be published successfully")
        };
        log.retire(losing).unwrap();
        assert!(log.lease(losing).is_err());
        assert_eq!(current.head, IndexHead::Log(first));
        assert_eq!(log.lease(first).unwrap().read(|v| v).unwrap(), 7);
    }
    #[test]
    fn different_hashes_in_the_same_bucket_and_the_same_label_share_the_key_chain_to_be_looked_up()
    {
        let index = index();
        let a = KeyHash(3 << 48);
        let b = KeyHash((3 << 48) | 2);
        let entry = index.prepare(a).unwrap();
        index
            .compare_publish(entry, IndexHead::Log(LogAddress(8)))
            .unwrap();
        assert_eq!(index.prepare(b).unwrap(), index.prepare(a).unwrap());
        // Index is not based on tag Declare key equality;P3 Log chain is used to record one by one KeyCodec Compare complete encoding.
    }
    #[test]
    fn bucket_overflow_and_label_independent_addressing() {
        let index = index();
        for tag in 0..30 {
            let hash = KeyHash(tag << 48);
            let entry = index.prepare(hash).unwrap();
            assert!(matches!(
                index.compare_publish(entry, IndexHead::Log(LogAddress(tag))),
                Ok(PublishResult::Published)
            ));
        }
        for tag in 0..30 {
            assert_eq!(
                index.locate(KeyHash(tag << 48)).unwrap().unwrap().head,
                IndexHead::Log(LogAddress(tag))
            );
        }
        assert_eq!(
            index.state.read().unwrap().active.buckets[0]
                .lock()
                .unwrap()
                .blocks
                .len(),
            5
        );
        assert_eq!(index.snapshot().unwrap().entries.len(), 30);
    }
    #[test]
    fn if_the_conflict_returns_a_new_snapshot_and_the_address_returns_to_the_original_value_the_revision_number_cannot_be_bypassed()
     {
        let index = index();
        let empty = index.prepare(KeyHash(1)).unwrap();
        index
            .compare_publish(empty, IndexHead::Log(LogAddress(0)))
            .unwrap();
        assert!(matches!(
            index
                .compare_publish(empty, IndexHead::Log(LogAddress(1)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
        let first = index.prepare(KeyHash(1)).unwrap();
        index.compare_publish(first, IndexHead::Empty).unwrap();
        let cleared = index.prepare(KeyHash(1)).unwrap();
        assert_eq!(
            index
                .diagnostics()
                .unwrap()
                .0
                .bucket_distribution
                .iter()
                .sum::<u64>(),
            0
        );
        assert!(matches!(
            index
                .compare_publish(empty, IndexHead::Log(LogAddress(1)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
        index
            .compare_publish(cleared, IndexHead::Log(LogAddress(0)))
            .unwrap();
        assert!(matches!(
            index.compare_publish(first, IndexHead::Empty).unwrap(),
            PublishResult::Conflict(_)
        ));
    }
    #[test]
    fn identity_generations_and_illegal_addresses_are_rejected_and_the_cache_cannot_enter_the_image()
     {
        let index = index();
        let other = MemIndex::new(IndexConfig { buckets: 2 }).unwrap();
        let entry = index.prepare(KeyHash(0)).unwrap();
        assert!(other.compare_publish(entry, IndexHead::Empty).is_err());
        let mut stale = entry;
        stale.table_generation = Generation(1);
        assert!(index.compare_publish(stale, IndexHead::Empty).is_err());
        assert!(
            index
                .compare_publish(entry, IndexHead::Log(LogAddress::INVALID))
                .is_err()
        );
        index
            .compare_publish(entry, IndexHead::Cache(CacheAddress(0)))
            .unwrap();
        assert!(index.snapshot().is_err());
    }
    #[test]
    fn there_can_only_be_one_winner_for_concurrent_publishing_of_the_same_snapshot() {
        let index = index();
        let entry = index.prepare(KeyHash(1)).unwrap();
        let barrier = std::sync::Barrier::new(2);
        let results = std::thread::scope(|s| {
            let a = s.spawn(|| {
                barrier.wait();
                index
                    .compare_publish(entry, IndexHead::Log(LogAddress(1)))
                    .unwrap()
            });
            let b = s.spawn(|| {
                barrier.wait();
                index
                    .compare_publish(entry, IndexHead::Log(LogAddress(2)))
                    .unwrap()
            });
            [a.join().unwrap(), b.join().unwrap()]
        });
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, PublishResult::Published))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, PublishResult::Conflict(_)))
                .count(),
            1
        );
    }
}
