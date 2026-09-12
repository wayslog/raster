//! Clean up failed chain heads bucket by bucket;Empty slots and surviving entries share monotonic bucket revisions,Recycling overflow blocks without resetting history.
use super::*;
impl Table {
    pub(super) fn clean_bucket(&self, number: usize, begin: LogAddress) -> Result<usize, Error> {
        begin.validate()?;
        let mut bucket = self
            .buckets
            .get(number)
            .ok_or(Error::InvalidState("Cleaning bucket number out of bounds"))?
            .lock()
            .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
        let obsolete = |entry: Entry| {
            matches!(entry.head, IndexHead::Empty)
                || matches!(entry.head, IndexHead::Log(address) if address < begin)
        };
        let mut removed = 0;
        for entry in bucket.blocks.iter().flatten().flatten() {
            if matches!(entry.head, IndexHead::Cache(_)) {
                return Err(Error::InvalidState(
                    "Cache headers must be normalized before cleaning",
                ));
            }
            removed += usize::from(obsolete(*entry));
        }
        if removed == 0 {
            return Ok(0);
        }
        let revision = bucket
            .revision
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        let mut kept = 0;
        // Entry It is pure copy data;Press forward in exclusive barrel lock,Do not allocate the second bucket.
        for read in 0..bucket.blocks.len() * SLOTS {
            if let Some(entry) = bucket.blocks[read / SLOTS][read % SLOTS]
                && !obsolete(entry)
            {
                bucket.blocks[kept / SLOTS][kept % SLOTS] = Some(entry);
                kept += 1;
            }
        }
        for slot in bucket.blocks.iter_mut().flatten().skip(kept) {
            *slot = None;
        }
        bucket.blocks.truncate(kept.div_ceil(SLOTS));
        if kept == 0 {
            bucket.blocks = Vec::new();
        } else {
            bucket.blocks.shrink_to_fit();
        }
        bucket.revision = revision;
        bucket.empty_revision = revision;
        Ok(removed)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn table() -> Table {
        Table::new(IndexConfig { buckets: 1 }).unwrap()
    }
    fn hash(tag: u64) -> KeyHash {
        KeyHash(tag << 48)
    }
    fn put(table: &Table, tag: u64, at: u64) {
        let expected = table.prepare(hash(tag)).unwrap();
        assert!(matches!(
            table
                .compare_publish(expected, IndexHead::Log(LogAddress(at)))
                .unwrap(),
            PublishResult::Published
        ));
    }
    #[test]
    fn clean_up_and_recycle_overflow_slots_and_retain_new_chain_heads_and_old_empty_slot_permissions_cannot_be_reused_across_cleanups()
     {
        let table = table();
        let empty = table.prepare(hash(7)).unwrap();
        for tag in 0..30 {
            put(&table, tag, tag * 64);
        }
        let old = table.prepare(hash(7)).unwrap();
        let untouched = table.prepare(hash(29)).unwrap();
        assert_eq!(table.clean_bucket(0, LogAddress(20 * 64)).unwrap(), 20);
        assert_eq!(table.buckets[0].lock().unwrap().blocks.len(), 2);
        assert_eq!(table.snapshot().unwrap().entries.len(), 10);
        for tag in 0..30 {
            assert_eq!(
                table.prepare(hash(tag)).unwrap().head,
                if tag < 20 {
                    IndexHead::Empty
                } else {
                    IndexHead::Log(LogAddress(tag * 64))
                }
            );
        }
        assert!(matches!(
            table
                .compare_publish(empty, IndexHead::Log(LogAddress(9000)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
        put(&table, 7, 7 * 64);
        assert!(matches!(
            table
                .compare_publish(old, IndexHead::Log(LogAddress(9000)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
        // Other items have not changed,Licenses already held for genuine articles may still be released conditionally.
        assert!(matches!(
            table
                .compare_publish(untouched, IndexHead::Log(LogAddress(9999)))
                .unwrap(),
            PublishResult::Published
        ));
        assert_eq!(table.clean_bucket(0, LogAddress(10000)).unwrap(), 11);
        let bucket = table.buckets[0].lock().unwrap();
        assert!(bucket.blocks.is_empty());
        assert_eq!(bucket.blocks.capacity(), 0);
    }
    #[test]
    fn cache_header_and_revision_exhaustion_reject_before_cleaning_any_entries() {
        let table = table();
        put(&table, 1, 1);
        let expected = table.prepare(hash(2)).unwrap();
        table
            .compare_publish(expected, IndexHead::Cache(CacheAddress(1)))
            .unwrap();
        assert!(matches!(
            table.clean_bucket(0, LogAddress(10)),
            Err(Error::InvalidState(_))
        ));
        assert_eq!(
            table.prepare(hash(1)).unwrap().head,
            IndexHead::Log(LogAddress(1))
        );
        put(&table, 2, 2);
        table.buckets[0].lock().unwrap().revision = u64::MAX;
        assert!(matches!(
            table.clean_bucket(0, LogAddress(10)),
            Err(Error::CapacityExceeded)
        ));
        assert_eq!(table.snapshot().unwrap().entries.len(), 2);
        let expected = table.prepare(hash(1)).unwrap();
        assert!(matches!(
            table.compare_publish(expected, IndexHead::Log(LogAddress(20))),
            Err(Error::CapacityExceeded)
        ));
    }
    #[test]
    fn cleaning_concurrently_with_same_key_publishing_does_not_delete_new_headers_after_boundaries()
    {
        for _ in 0..32 {
            let table = table();
            put(&table, 1, 1);
            let expected = table.prepare(hash(1)).unwrap();
            let barrier = std::sync::Barrier::new(2);
            let (removed, publication) = std::thread::scope(|scope| {
                let clean = scope.spawn(|| {
                    barrier.wait();
                    table.clean_bucket(0, LogAddress(10)).unwrap()
                });
                let write = scope.spawn(|| {
                    barrier.wait();
                    table
                        .compare_publish(expected, IndexHead::Log(LogAddress(20)))
                        .unwrap()
                });
                (clean.join().unwrap(), write.join().unwrap())
            });
            match publication {
                PublishResult::Published => {
                    assert_eq!(removed, 0);
                    assert_eq!(
                        table.prepare(hash(1)).unwrap().head,
                        IndexHead::Log(LogAddress(20))
                    );
                }
                PublishResult::Conflict(_) => {
                    assert_eq!(removed, 1);
                    assert_eq!(table.prepare(hash(1)).unwrap().head, IndexHead::Empty);
                    put(&table, 1, 20);
                }
            }
        }
    }
}
