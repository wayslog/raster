//! 每桶独立控制锁实现条件发布；同 tag 共用链头，键相等由日志链校验。
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntrySnapshot {
    owner: u64,
    pub bucket: usize,
    pub tag: u16,
    pub head: IndexHead,
    pub table_generation: Generation,
    revision: u64,
}
pub(crate) struct IndexImage {
    pub generation: Generation,
    pub entries: Vec<EntrySnapshot>,
}
#[derive(Debug)]
pub(crate) enum PublishResult {
    Published,
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
}
pub(crate) struct MemIndex {
    owner: u64,
    buckets: Vec<Mutex<Bucket>>,
    generation: Generation,
}
impl MemIndex {
    pub fn new(config: IndexConfig) -> Result<Self, Error> {
        if !config.buckets.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "index.buckets",
                reason: "桶数量必须是非零二次幂",
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
            buckets.push(Mutex::new(Bucket { blocks: Vec::new() }));
        }
        Ok(Self {
            owner,
            buckets,
            generation: Generation(0),
        })
    }
    fn entry(&self, bucket: usize, tag: u16, entries: &Bucket) -> EntrySnapshot {
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
            revision: entry.map_or(0, |e| e.revision),
            table_generation: self.generation,
        }
    }
    /// 空链头同样返回带身份快照，供首次条件发布使用。
    pub fn prepare(&self, hash: KeyHash) -> Result<EntrySnapshot, Error> {
        let bucket = hash.0 as usize & (self.buckets.len() - 1);
        let entries = self.buckets[bucket]
            .lock()
            .map_err(|_| Error::InvalidState("索引桶锁中毒"))?;
        Ok(self.entry(bucket, hash.tag(), &entries))
    }
    pub fn locate(&self, hash: KeyHash) -> Result<Option<EntrySnapshot>, Error> {
        let entry = self.prepare(hash)?;
        Ok((entry.head != IndexHead::Empty).then_some(entry))
    }
    /// 调用者先完成值初始化和日志发布；替换时还需持有源记录仲裁。
    pub fn compare_publish(
        &self,
        expected: EntrySnapshot,
        head: IndexHead,
    ) -> Result<PublishResult, Error> {
        if expected.owner != self.owner
            || expected.table_generation != self.generation
            || expected.bucket >= self.buckets.len()
        {
            return Err(Error::InvalidState("索引快照身份或代次失效"));
        }
        match head {
            IndexHead::Log(a) => a.validate()?,
            IndexHead::Cache(a) => a.validate()?,
            IndexHead::Empty => (),
        }
        let mut bucket = self.buckets[expected.bucket]
            .lock()
            .map_err(|_| Error::InvalidState("索引桶锁中毒"))?;
        let current = self.entry(expected.bucket, expected.tag, &bucket);
        if current != expected {
            return Ok(PublishResult::Conflict(current));
        }
        let revision = current
            .revision
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
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
        Ok(PublishResult::Published)
    }
    /// 逐桶模糊映像；不声称跨桶事务快照，缓存头须由上层规范化后再调用。
    pub fn snapshot(&self) -> Result<IndexImage, Error> {
        let mut entries = Vec::new();
        for (number, mutex) in self.buckets.iter().enumerate() {
            let bucket = mutex
                .lock()
                .map_err(|_| Error::InvalidState("索引桶锁中毒"))?;
            for entry in bucket.blocks.iter().flatten().flatten() {
                if matches!(entry.head, IndexHead::Cache(_)) {
                    return Err(Error::InvalidState("持久化索引必须先规范化缓存地址"));
                }
                if entry.head != IndexHead::Empty {
                    entries.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                    entries.push(EntrySnapshot {
                        owner: self.owner,
                        bucket: number,
                        tag: entry.tag,
                        head: entry.head,
                        table_generation: self.generation,
                        revision: entry.revision,
                    });
                }
            }
        }
        Ok(IndexImage {
            generation: self.generation,
            entries,
        })
    }
    pub fn restore(&mut self, _image: IndexImage) -> Result<(), Error> {
        Err(Error::unimplemented("index::restore"))
    }
    pub fn grow_step(&self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("index::grow"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn index() -> MemIndex {
        MemIndex::new(IndexConfig { buckets: 2 }).unwrap()
    }
    #[test]
    fn 已初始化日志记录发布与失败发布清理() {
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
            panic!("旧快照不得发布成功")
        };
        log.retire(losing).unwrap();
        assert!(log.lease(losing).is_err());
        assert_eq!(current.head, IndexHead::Log(first));
        assert_eq!(log.lease(first).unwrap().read(|v| v).unwrap(), 7);
    }
    #[test]
    fn 同桶同标签的不同哈希共用待查键链() {
        let index = index();
        let a = KeyHash(3 << 48);
        let b = KeyHash((3 << 48) | 2);
        let entry = index.prepare(a).unwrap();
        index
            .compare_publish(entry, IndexHead::Log(LogAddress(8)))
            .unwrap();
        assert_eq!(index.prepare(b).unwrap(), index.prepare(a).unwrap());
        // 索引不据 tag 宣称键相等；P3 日志链逐记录用 KeyCodec 比较完整编码。
    }
    #[test]
    fn 桶溢出与标签独立寻址() {
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
        assert_eq!(index.buckets[0].lock().unwrap().blocks.len(), 5);
        assert_eq!(index.snapshot().unwrap().entries.len(), 30);
    }
    #[test]
    fn 冲突返回新快照且地址回到原值不能绕过修订号() {
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
        index
            .compare_publish(cleared, IndexHead::Log(LogAddress(0)))
            .unwrap();
        assert!(matches!(
            index.compare_publish(first, IndexHead::Empty).unwrap(),
            PublishResult::Conflict(_)
        ));
    }
    #[test]
    fn 身份代次和非法地址拒绝且缓存不能进入映像() {
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
    fn 同一快照并发发布只能有一个成功者() {
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
