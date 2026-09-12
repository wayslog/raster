//! Routing to only writable table during bucket-by-bucket migration;The old table was created by the caller in epoch Release after safety.
use super::*;
use std::sync::{Arc, RwLock};

pub(crate) struct MemIndex {
    pub(super) state: RwLock<State>,
    metrics: Arc<crate::engine::metrics::Metrics>,
}
pub(super) struct State {
    pub active: Arc<Table>,
    growing: Option<Growing>,
    retired: Option<Arc<Table>>,
}
struct Growing {
    table: Arc<Table>,
    next: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GrowthProgress {
    pub migrated: usize,
    pub old_buckets: usize,
    pub new_buckets: usize,
    pub generation: Generation,
    pub complete: bool,
}
impl State {
    fn route(&self, hash: KeyHash) -> &Table {
        if let Some(growth) = &self.growing
            && hash.0 as usize & (self.active.buckets.len() - 1) < growth.next
        {
            &growth.table
        } else {
            &self.active
        }
    }
}
impl MemIndex {
    pub fn new(config: IndexConfig) -> Result<Self, Error> {
        Ok(Self {
            metrics: Arc::new(crate::engine::metrics::Metrics::new(false)),
            state: RwLock::new(State {
                active: Arc::new(Table::new(config)?),
                growing: None,
                retired: None,
            }),
        })
    }
    pub fn set_metrics(&mut self, metrics: Arc<crate::engine::metrics::Metrics>) {
        self.metrics = metrics;
    }
    pub fn identity(&self) -> Result<u64, Error> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?
            .active
            .owner)
    }
    pub fn prepare(&self, hash: KeyHash) -> Result<EntrySnapshot, Error> {
        self.metrics
            .index(crate::engine::metrics::IndexEvent::Lookup);
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        state.route(hash).prepare(hash)
    }
    #[cfg(test)]
    pub fn locate(&self, hash: KeyHash) -> Result<Option<EntrySnapshot>, Error> {
        let entry = self.prepare(hash)?;
        Ok((entry.head != IndexHead::Empty).then_some(entry))
    }
    pub fn compare_publish(
        &self,
        expected: EntrySnapshot,
        head: IndexHead,
    ) -> Result<PublishResult, Error> {
        self.compare_publish_mode(expected, head, false)
    }
    /// RMW with upstream FindOrCreateEntry Alignment;Empty address slot contains no value,Still participating in subsequent blind deletions.
    pub fn reserve_empty(&self, expected: EntrySnapshot) -> Result<PublishResult, Error> {
        if expected.present {
            return Err(Error::InvalidState(
                "Cannot change occupied index slot to empty reservation",
            ));
        }
        self.compare_publish_mode(expected, IndexHead::Empty, true)
    }
    fn compare_publish_mode(
        &self,
        expected: EntrySnapshot,
        head: IndexHead,
        reserve_empty: bool,
    ) -> Result<PublishResult, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        let hash = expected.hash.ok_or(Error::InvalidState(
            "Image entries are not licensed as releases",
        ))?;
        let table = state.route(hash);
        if expected.owner != table.owner
            || expected.tag != hash.tag()
            || expected.table_generation > table.generation
        {
            return Err(Error::InvalidState(
                "Index snapshot identity or generation invalid",
            ));
        }
        match head {
            IndexHead::Log(address) => address.validate()?,
            IndexHead::Cache(address) => address.validate()?,
            IndexHead::Empty => (),
        }
        self.metrics
            .index(crate::engine::metrics::IndexEvent::Publish);
        let result = if expected.table_generation != table.generation {
            PublishResult::Conflict(table.prepare(hash)?)
        } else {
            if reserve_empty {
                table.compare_publish_mode(expected, head, true)?
            } else {
                table.compare_publish(expected, head)?
            }
        };
        if matches!(result, PublishResult::Conflict(_)) {
            self.metrics
                .index(crate::engine::metrics::IndexEvent::Conflict);
        }
        Ok(result)
    }
    pub fn diagnostics(
        &self,
    ) -> Result<
        (
            crate::diagnostics::IndexTableDiagnostics,
            Option<crate::diagnostics::IndexTableDiagnostics>,
            usize,
            bool,
        ),
        Error,
    > {
        fn table(table: &Table) -> Result<crate::diagnostics::IndexTableDiagnostics, Error> {
            let mut counts = Vec::new();
            counts
                .try_reserve_exact(table.buckets.len())
                .map_err(|_| Error::OutOfMemory)?;
            for bucket in &table.buckets {
                let bucket = bucket
                    .lock()
                    .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
                counts.push(bucket.blocks.iter().flatten().flatten().count() as u64);
            }
            Ok(crate::diagnostics::IndexTableDiagnostics {
                generation: table.generation,
                bucket_distribution: counts,
            })
        }
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        Ok((
            table(&state.active)?,
            state
                .growing
                .as_ref()
                .map(|growth| table(&growth.table))
                .transpose()?,
            state.growing.as_ref().map_or(0, |growth| growth.next),
            state.retired.is_some(),
        ))
    }
    pub fn bucket_count(&self) -> Result<usize, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        if state.growing.is_some() {
            return Err(Error::Busy);
        }
        Ok(state.active.buckets.len())
    }
    /// logic begin Clear old chain heads bucket by bucket after release;The caller first excludes expansion and normalizes cache headers.
    pub fn clean_bucket(&self, bucket: usize, begin: LogAddress) -> Result<usize, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        if state.growing.is_some() {
            return Err(Error::Busy);
        }
        state.active.clean_bucket(bucket, begin)
    }
    pub fn snapshot(&self) -> Result<IndexImage, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        if state.growing.is_some() {
            return Err(Error::Busy);
        }
        state.active.snapshot()
    }
    pub fn restore(&mut self, image: crate::format::IndexSnapshot) -> Result<(), Error> {
        let state = self
            .state
            .get_mut()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        if state.growing.is_some() || state.retired.is_some() {
            return Err(Error::Busy);
        }
        let mut restored = Table::new(IndexConfig {
            buckets: state.active.buckets.len(),
        })?;
        restored.restore(image)?;
        state.active = Arc::new(restored);
        Ok(())
    }
    pub fn begin_growth(&self) -> Result<GrowthProgress, Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        if state.growing.is_some() || state.retired.is_some() {
            return Err(Error::Busy);
        }
        let old_buckets = state.active.buckets.len();
        let new_buckets = old_buckets.checked_mul(2).ok_or(Error::CapacityExceeded)?;
        let generation = Generation(
            state
                .active
                .generation
                .0
                .checked_add(1)
                .ok_or(Error::CapacityExceeded)?,
        );
        let mut table = Table::new(IndexConfig {
            buckets: new_buckets,
        })?;
        table.owner = state.active.owner;
        table.generation = generation;
        state.growing = Some(Growing {
            table: Arc::new(table),
            next: 0,
        });
        Ok(GrowthProgress {
            migrated: 0,
            old_buckets,
            new_buckets,
            generation,
            complete: false,
        })
    }
    /// Each old bucket is migrated only once;Copy two copies of the link header,Shared history suffix,Subsequent writes are diverted to new buckets.
    pub fn grow_step(&self, budget: PollBudget) -> Result<GrowthProgress, Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        let State {
            active, growing, ..
        } = &mut *state;
        let growth = growing
            .as_mut()
            .ok_or(Error::InvalidState("There are no indexes being expanded"))?;
        let old_buckets = active.buckets.len();
        for _ in 0..budget.0.get() {
            if growth.next == old_buckets {
                break;
            }
            let source = active.buckets[growth.next]
                .lock()
                .map_err(|_| Error::InvalidState("index_bucket_lock_poisoned"))?;
            if source
                .blocks
                .iter()
                .flatten()
                .flatten()
                .any(|entry| matches!(entry.head, IndexHead::Cache(_)))
            {
                return Err(Error::InvalidState(
                    "Cache headers must be normalized before expansion",
                ));
            }
            let mut low = Vec::new();
            let mut high = Vec::new();
            low.try_reserve_exact(source.blocks.len())
                .map_err(|_| Error::OutOfMemory)?;
            high.try_reserve_exact(source.blocks.len())
                .map_err(|_| Error::OutOfMemory)?;
            low.extend_from_slice(&source.blocks);
            high.extend_from_slice(&source.blocks);
            let mut lower = growth.table.buckets[growth.next]
                .lock()
                .map_err(|_| Error::InvalidState("New index bucket lock poisoning"))?;
            let mut upper = growth.table.buckets[growth.next + old_buckets]
                .lock()
                .map_err(|_| Error::InvalidState("New index bucket lock poisoning"))?;
            lower.blocks = low;
            upper.blocks = high;
            lower.revision = source.revision;
            upper.revision = source.revision;
            lower.empty_revision = source.empty_revision;
            upper.empty_revision = source.empty_revision;
            growth.next += 1;
        }
        let result = GrowthProgress {
            migrated: growth.next,
            old_buckets,
            new_buckets: growth.table.buckets.len(),
            generation: growth.table.generation,
            complete: growth.next == old_buckets,
        };
        if result.complete {
            let next = state.growing.take().expect("Expansion exists").table;
            state.retired = Some(std::mem::replace(&mut state.active, next));
        }
        Ok(result)
    }
    /// Only in correspondence ReleaseIndex epoch Called after the action has been safely delivered.
    pub fn release_retired(&self, generation: Generation) -> Result<(), Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("Index route lock poisoning"))?;
        if state
            .retired
            .as_ref()
            .is_none_or(|old| old.generation != generation)
        {
            return Err(Error::InvalidState(
                "The index generation to be recycled does not match",
            ));
        }
        state.retired = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn one() -> PollBudget {
        PollBudget(std::num::NonZeroUsize::new(1).unwrap())
    }
    #[test]
    fn migrate_the_offload_chain_head_bucket_by_bucket_and_relocate_the_old_snapshot_before_it_can_be_released()
     {
        let index = MemIndex::new(IndexConfig { buckets: 2 }).unwrap();
        let low = KeyHash(7 << 48);
        let high = KeyHash((7 << 48) | 2);
        let old = index.prepare(high).unwrap();
        index
            .compare_publish(old, IndexHead::Log(LogAddress(10)))
            .unwrap();
        let stale = index.prepare(high).unwrap();
        let started = index.begin_growth().unwrap();
        assert_eq!((started.old_buckets, started.new_buckets), (2, 4));
        assert!(matches!(index.begin_growth(), Err(Error::Busy)));
        assert!(matches!(index.snapshot(), Err(Error::Busy)));
        let first = index.grow_step(one()).unwrap();
        assert_eq!(first.migrated, 1);
        assert!(!first.complete);
        assert_eq!(
            index.prepare(KeyHash(1)).unwrap().table_generation,
            Generation(0)
        );
        let PublishResult::Conflict(current) = index
            .compare_publish(stale, IndexHead::Log(LogAddress(20)))
            .unwrap()
        else {
            panic!("Old table snapshots cannot be published directly")
        };
        assert_eq!(current.bucket, 2);
        assert_eq!(current.table_generation, Generation(1));
        assert!(matches!(
            index
                .compare_publish(current, IndexHead::Log(LogAddress(20)))
                .unwrap(),
            PublishResult::Published
        ));
        assert_eq!(
            index.prepare(low).unwrap().head,
            IndexHead::Log(LogAddress(10))
        );
        let current = index.prepare(low).unwrap();
        index
            .compare_publish(current, IndexHead::Log(LogAddress(30)))
            .unwrap();
        assert_eq!(
            index.prepare(high).unwrap().head,
            IndexHead::Log(LogAddress(20))
        );
        assert!(index.grow_step(one()).unwrap().complete);
        assert_eq!(index.snapshot().unwrap().buckets, 4);
        assert_eq!(index.snapshot().unwrap().generation, Generation(1));
        assert!(matches!(index.begin_growth(), Err(Error::Busy)));
        assert!(index.release_retired(Generation(1)).is_err());
        index.release_retired(Generation(0)).unwrap();
        assert!(index.begin_growth().is_ok());
    }
    #[test]
    fn the_old_table_is_being_accessed_epoch_the_release_action_cannot_be_recycled_until_it_is_delivered()
     {
        use crate::epoch::{DeferredAction, EpochManager};
        let index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
        let old = Arc::downgrade(&index.state.read().unwrap().active);
        let epoch = EpochManager::new().unwrap();
        let participant = epoch.register().unwrap();
        let guard = epoch.enter(participant).unwrap();
        index.begin_growth().unwrap();
        assert!(index.grow_step(one()).unwrap().complete);
        epoch
            .defer(DeferredAction::ReleaseIndex(Generation(0)))
            .unwrap();
        epoch.advance().unwrap();
        assert!(epoch.collect().unwrap().is_empty());
        assert!(old.upgrade().is_some());
        drop(guard);
        for action in epoch.collect().unwrap() {
            let DeferredAction::ReleaseIndex(generation) = action;
            index.release_retired(generation).unwrap();
        }
        assert!(old.upgrade().is_none());
        epoch.unregister(participant).unwrap();
    }
    #[test]
    fn concurrent_conditional_release_traverses_bucket_by_bucket_migration_without_losing_the_last_link_head()
     {
        let index = MemIndex::new(IndexConfig { buckets: 8 }).unwrap();
        let barrier = std::sync::Barrier::new(5);
        index.begin_growth().unwrap();
        std::thread::scope(|scope| {
            for worker in 0..4u64 {
                let index = &index;
                let barrier = &barrier;
                scope.spawn(move || {
                    let hash = KeyHash(((worker + 1) << 48) | worker | 8);
                    barrier.wait();
                    for round in 0..100 {
                        loop {
                            let expected = index.prepare(hash).unwrap();
                            if matches!(
                                index
                                    .compare_publish(
                                        expected,
                                        IndexHead::Log(LogAddress(worker * 100 + round))
                                    )
                                    .unwrap(),
                                PublishResult::Published
                            ) {
                                break;
                            }
                        }
                    }
                });
            }
            barrier.wait();
            while !index.grow_step(one()).unwrap().complete {
                std::thread::yield_now();
            }
        });
        for worker in 0..4 {
            let hash = KeyHash(((worker + 1) << 48) | worker | 8);
            assert_eq!(
                index.prepare(hash).unwrap().head,
                IndexHead::Log(LogAddress(worker * 100 + 99))
            );
        }
    }
}

#[cfg(test)]
mod gc_tests {
    use super::*;
    #[test]
    fn expand_replication_bucket_revision_history_and_reject_cleanup_during_migration() {
        let index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
        let hash = KeyHash(7 << 48);
        let empty = index.prepare(hash).unwrap();
        index
            .compare_publish(empty, IndexHead::Log(LogAddress(7)))
            .unwrap();
        assert_eq!(index.clean_bucket(0, LogAddress(8)).unwrap(), 1);
        let before = index.prepare(hash).unwrap();
        index.begin_growth().unwrap();
        assert!(matches!(
            index.clean_bucket(0, LogAddress(8)),
            Err(Error::Busy)
        ));
        index.grow_step(PollBudget::default()).unwrap();
        let after = index.prepare(hash).unwrap();
        assert!(after.revision > empty.revision);
        assert!(matches!(
            index
                .compare_publish(before, IndexHead::Log(LogAddress(7)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
        index
            .compare_publish(after, IndexHead::Log(LogAddress(7)))
            .unwrap();
        let created = index.prepare(hash).unwrap();
        assert!(created.revision > after.revision);
        assert_eq!(index.clean_bucket(0, LogAddress(8)).unwrap(), 1);
        assert!(matches!(
            index
                .compare_publish(after, IndexHead::Log(LogAddress(7)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
        assert!(index.clean_bucket(2, LogAddress(8)).is_err());
    }
    #[test]
    fn ordinary_publishing_of_different_tags_will_not_revoke_the_permission_of_another_empty_slot_but_recycling_will()
     {
        let index = MemIndex::new(IndexConfig { buckets: 1 }).unwrap();
        let one = KeyHash(1 << 48);
        let two = KeyHash(2 << 48);
        let first = index.prepare(one).unwrap();
        let second = index.prepare(two).unwrap();
        index
            .compare_publish(first, IndexHead::Log(LogAddress(1)))
            .unwrap();
        assert!(matches!(
            index
                .compare_publish(second, IndexHead::Log(LogAddress(2)))
                .unwrap(),
            PublishResult::Published
        ));
        let empty = index.prepare(KeyHash(3 << 48)).unwrap();
        index.clean_bucket(0, LogAddress(3)).unwrap();
        assert!(matches!(
            index
                .compare_publish(empty, IndexHead::Log(LogAddress(3)))
                .unwrap(),
            PublishResult::Conflict(_)
        ));
    }
}
