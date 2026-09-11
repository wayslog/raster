//! 逐桶迁移期间路由到唯一可写表；旧表由调用者在 epoch 安全后释放。
use super::*;
use std::sync::{Arc, RwLock};

pub(crate) struct MemIndex {
    pub(super) state: RwLock<State>,
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
            state: RwLock::new(State {
                active: Arc::new(Table::new(config)?),
                growing: None,
                retired: None,
            }),
        })
    }
    pub fn identity(&self) -> Result<u64, Error> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?
            .active
            .owner)
    }
    pub fn prepare(&self, hash: KeyHash) -> Result<EntrySnapshot, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
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
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
        let hash = expected
            .hash
            .ok_or(Error::InvalidState("映像条目不作为发布许可"))?;
        let table = state.route(hash);
        if expected.owner != table.owner
            || expected.tag != hash.tag()
            || expected.table_generation > table.generation
        {
            return Err(Error::InvalidState("索引快照身份或代次失效"));
        }
        match head {
            IndexHead::Log(address) => address.validate()?,
            IndexHead::Cache(address) => address.validate()?,
            IndexHead::Empty => (),
        }
        if expected.table_generation != table.generation {
            return Ok(PublishResult::Conflict(table.prepare(hash)?));
        }
        table.compare_publish(expected, head)
    }
    /// 逻辑 begin 发布后逐桶清除旧链头；调用者先排除扩容并规范化缓存头。
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "P7.2 逐桶 GC 驱动接入前由索引竞争和扩容测试验证")
    )]
    pub fn clean_bucket(&self, bucket: usize, begin: LogAddress) -> Result<usize, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
        if state.growing.is_some() {
            return Err(Error::Busy);
        }
        state.active.clean_bucket(bucket, begin)
    }
    pub fn snapshot(&self) -> Result<IndexImage, Error> {
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
        if state.growing.is_some() {
            return Err(Error::Busy);
        }
        state.active.snapshot()
    }
    pub fn restore(&mut self, image: crate::format::IndexSnapshot) -> Result<(), Error> {
        let state = self
            .state
            .get_mut()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
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
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
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
    /// 每个旧桶只迁移一次；复制两份链头，共享历史后缀，后续写入按新桶分流。
    pub fn grow_step(&self, budget: PollBudget) -> Result<GrowthProgress, Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
        let State {
            active, growing, ..
        } = &mut *state;
        let growth = growing
            .as_mut()
            .ok_or(Error::InvalidState("没有正在扩容的索引"))?;
        let old_buckets = active.buckets.len();
        for _ in 0..budget.0.get() {
            if growth.next == old_buckets {
                break;
            }
            let source = active.buckets[growth.next]
                .lock()
                .map_err(|_| Error::InvalidState("索引桶锁中毒"))?;
            if source
                .blocks
                .iter()
                .flatten()
                .flatten()
                .any(|entry| matches!(entry.head, IndexHead::Cache(_)))
            {
                return Err(Error::InvalidState("扩容前须规范化缓存头"));
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
                .map_err(|_| Error::InvalidState("新索引桶锁中毒"))?;
            let mut upper = growth.table.buckets[growth.next + old_buckets]
                .lock()
                .map_err(|_| Error::InvalidState("新索引桶锁中毒"))?;
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
            let next = state.growing.take().expect("扩容存在").table;
            state.retired = Some(std::mem::replace(&mut state.active, next));
        }
        Ok(result)
    }
    /// 仅在对应 ReleaseIndex epoch 动作已安全交付后调用。
    pub fn release_retired(&self, generation: Generation) -> Result<(), Error> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::InvalidState("索引路由锁中毒"))?;
        if state
            .retired
            .as_ref()
            .is_none_or(|old| old.generation != generation)
        {
            return Err(Error::InvalidState("待回收索引代次不匹配"));
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
    fn 逐桶迁移分流链头且旧快照重新定位后才能发布() {
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
            panic!("旧表快照不能直接发布")
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
    fn 旧表在访问_epoch_释放动作交付后才能回收() {
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
            let DeferredAction::ReleaseIndex(generation) = action else {
                panic!("错误延迟动作")
            };
            index.release_retired(generation).unwrap();
        }
        assert!(old.upgrade().is_none());
        epoch.unregister(participant).unwrap();
    }
    #[test]
    fn 并发条件发布穿越逐桶迁移不会丢失最后链头() {
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
    fn 扩容复制桶修订历史并拒绝迁移期间清理() {
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
    fn 不同标签普通发布不撤销另一空槽许可但回收会撤销() {
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
