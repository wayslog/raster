//! 不可变磁盘记录缓存；索引条件发布、地址解析和淘汰共用控制锁。
use crate::{
    config::CacheConfig,
    format::Record,
    index::{EntrySnapshot, IndexHead, MemIndex, PublishResult},
    types::*,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(crate) struct CachedRecord {
    pub source: LogAddress,
    pub version: CheckpointVersion,
    head: LogAddress,
    hash: KeyHash,
    bytes: Vec<u8>,
    charge: usize,
    allocated: Arc<AtomicUsize>,
}
impl CachedRecord {
    pub fn encoded(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for CachedRecord {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.bytes));
        self.allocated.fetch_sub(self.charge, Ordering::SeqCst);
    }
}
pub(crate) struct Resolved {
    pub entry: EntrySnapshot,
    pub head: Option<LogAddress>,
    pub cached: Option<Arc<CachedRecord>>,
}
#[derive(Default)]
struct State {
    owner: Option<u64>,
    next: u64,
    entries: BTreeMap<u64, Arc<CachedRecord>>,
}
pub(crate) struct ReadCache {
    config: CacheConfig,
    allocated: Arc<AtomicUsize>,
    state: Mutex<State>,
}
impl ReadCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            allocated: Arc::new(AtomicUsize::new(0)),
            state: Mutex::new(State::default()),
        }
    }
    pub fn max_record_bytes(&self) -> usize {
        self.config
            .capacity_bytes
            .saturating_sub(std::mem::size_of::<CachedRecord>())
    }
    fn bind(state: &mut State, index: &MemIndex) -> Result<(), Error> {
        let owner = index.identity()?;
        if state.owner.is_some_and(|old| old != owner) {
            return Err(Error::InvalidState("缓存属于另一索引身份"));
        }
        state.owner = Some(owner);
        Ok(())
    }
    fn head(state: &State, entry: EntrySnapshot) -> Result<Option<LogAddress>, Error> {
        match entry.head {
            IndexHead::Empty => Ok(None),
            IndexHead::Log(address) => Ok(Some(address)),
            IndexHead::Cache(address) => state
                .entries
                .get(&address.0)
                .map(|record| Some(record.head))
                .ok_or(Error::InvalidState("索引引用了不存在的缓存记录")),
        }
    }
    /// 选择索引头和取得缓存租约是同一临界区，避免淘汰后解析过期缓存地址。
    pub fn resolve(&self, index: &MemIndex, hash: KeyHash, key: &[u8]) -> Result<Resolved, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("缓存控制锁中毒"))?;
        Self::bind(&mut state, index)?;
        let entry = index.prepare(hash)?;
        let head = Self::head(&state, entry)?;
        let cached = if let IndexHead::Cache(address) = entry.head {
            let record = state.entries.get(&address.0).expect("缓存头已验证");
            (Record::decode(record.encoded())?.key == key).then(|| record.clone())
        } else {
            None
        };
        Ok(Resolved {
            entry,
            head,
            cached,
        })
    }
    /// 源必须是已验证的冷记录，hash 对应记录的规范键；未命中或预算不足不是业务错误。
    pub fn insert_if_current(
        &self,
        index: &MemIndex,
        expected: EntrySnapshot,
        hash: KeyHash,
        source: LogAddress,
        bytes: Vec<u8>,
    ) -> Result<Option<CacheAddress>, Error> {
        if !self.config.enabled {
            return Ok(None);
        }
        source.validate()?;
        let record = Record::decode(&bytes)?;
        if record.header.invalid || record.header.tombstone {
            return Ok(None);
        }
        if record
            .header
            .previous
            .is_some_and(|previous| previous >= source)
        {
            return Err(Error::InvalidFormat("缓存源记录前驱没有递减"));
        }
        let version = record.header.version;
        let charge = bytes
            .capacity()
            .checked_add(std::mem::size_of::<CachedRecord>())
            .ok_or(Error::CapacityExceeded)?;
        if charge > self.config.capacity_bytes {
            return Ok(None);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("缓存控制锁中毒"))?;
        Self::bind(&mut state, index)?;
        if index.prepare(hash)? != expected {
            return Ok(None);
        }
        let head =
            Self::head(&state, expected)?.ok_or(Error::InvalidState("空索引头不能安装缓存"))?;
        if source > head {
            return Err(Error::InvalidFormat("缓存源记录越过主日志链头"));
        }
        while self
            .allocated
            .load(Ordering::SeqCst)
            .checked_add(charge)
            .is_none_or(|bytes| bytes > self.config.capacity_bytes)
        {
            let Some(address) = state.entries.keys().next().copied() else {
                return Ok(None);
            };
            Self::remove(&mut state, index, address)?;
        }
        // 淘汰可能还原 expected 本身，重新取得快照但不接受其他写入造成的链变化。
        let current = index.prepare(hash)?;
        if current != expected {
            return Ok(None);
        }
        let address = CacheAddress(state.next);
        if address.validate().is_err() {
            return Ok(None);
        }
        state.next = state.next.checked_add(1).ok_or(Error::CapacityExceeded)?;
        self.allocated.fetch_add(charge, Ordering::SeqCst);
        let record = Arc::new(CachedRecord {
            source,
            version,
            head,
            hash,
            bytes,
            charge,
            allocated: self.allocated.clone(),
        });
        match index.compare_publish(expected, IndexHead::Cache(address))? {
            PublishResult::Conflict(_) => Ok(None),
            PublishResult::Published => {
                state.entries.insert(address.0, record);
                if let IndexHead::Cache(old) = expected.head {
                    state.entries.remove(&old.0);
                }
                Ok(Some(address))
            }
        }
    }
    fn remove(state: &mut State, index: &MemIndex, address: u64) -> Result<(), Error> {
        let record = state
            .entries
            .get(&address)
            .ok_or(Error::InvalidState("缓存项不存在"))?;
        let current = index.prepare(record.hash)?;
        if current.head == IndexHead::Cache(CacheAddress(address)) {
            // 非缓存写入可抢先替换；冲突时无需覆盖其新链头。
            index.compare_publish(current, IndexHead::Log(record.head))?;
        }
        state.entries.remove(&address);
        Ok(())
    }
    #[cfg(test)]
    pub fn invalidate(&self, index: &MemIndex, address: CacheAddress) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("缓存控制锁中毒"))?;
        Self::bind(&mut state, index)?;
        if state.entries.contains_key(&address.0) {
            Self::remove(&mut state, index, address.0)?;
        }
        Ok(())
    }
    /// 调用者在本临界区执行索引快照或迁移，不能插入用户回调或等待 I/O。
    pub fn with_normalized_index<T>(
        &self,
        index: &MemIndex,
        operation: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("缓存控制锁中毒"))?;
        Self::bind(&mut state, index)?;
        while let Some(address) = state.entries.keys().next().copied() {
            Self::remove(&mut state, index, address)?;
        }
        operation()
    }
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "P8 诊断将读取缓存计费，当前由原生容量测试验证")
    )]
    pub fn allocated_bytes(&self) -> usize {
        self.allocated.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::IndexConfig, format::RecordHeader};
    fn bytes(key: &[u8], value: &[u8]) -> Vec<u8> {
        let header = RecordHeader {
            previous: None,
            version: CheckpointVersion(3),
            key_bytes: key.len() as u32,
            value_bytes: value.len() as u32,
            capacity_bytes: value.len() as u32,
            tombstone: false,
            invalid: false,
            final_record: false,
        };
        let mut bytes = vec![0; header.encoded_len().unwrap()];
        Record { header, key, value }.encode(&mut bytes).unwrap();
        bytes
    }
    fn index() -> MemIndex {
        MemIndex::new(IndexConfig { buckets: 2 }).unwrap()
    }
    fn head(index: &MemIndex, hash: KeyHash, address: u64) -> EntrySnapshot {
        index
            .compare_publish(
                index.prepare(hash).unwrap(),
                IndexHead::Log(LogAddress(address)),
            )
            .unwrap();
        index.prepare(hash).unwrap()
    }
    fn cache(capacity: usize) -> ReadCache {
        ReadCache::new(CacheConfig {
            enabled: true,
            capacity_bytes: capacity,
        })
    }
    #[test]
    fn 缓存条件安装完整键命中且快照先还原日志地址() {
        let index = index();
        let cache = cache(4096);
        let expected = head(&index, KeyHash(0), 10);
        let address = cache
            .insert_if_current(
                &index,
                expected,
                KeyHash(0),
                LogAddress(8),
                bytes(b"a", b"value"),
            )
            .unwrap()
            .unwrap();
        let resolved = cache.resolve(&index, KeyHash(0), b"a").unwrap();
        assert_eq!(resolved.entry.head, IndexHead::Cache(address));
        assert_eq!(resolved.head, Some(LogAddress(10)));
        let pinned = resolved.cached.unwrap();
        assert_eq!(pinned.source, LogAddress(8));
        assert_eq!(pinned.version, CheckpointVersion(3));
        assert_eq!(Record::decode(pinned.encoded()).unwrap().value, b"value");
        assert!(
            cache
                .resolve(&index, KeyHash(0), b"b")
                .unwrap()
                .cached
                .is_none()
        );
        assert!(index.snapshot().is_err());
        let image = cache
            .with_normalized_index(&index, || index.snapshot())
            .unwrap();
        assert_eq!(image.entries[0].head, IndexHead::Log(LogAddress(10)));
        image.encode().unwrap();
        assert!(cache.allocated_bytes() > 0, "已借出的记录仍计入预算");
        drop(pinned);
        assert_eq!(cache.allocated_bytes(), 0);
    }
    #[test]
    fn 淘汰中的借用继续计费且旧缓存地址不能误伤新项() {
        let index = index();
        let record = bytes(b"a", b"v");
        let charge = record.capacity() + std::mem::size_of::<CachedRecord>();
        let cache = cache(charge);
        let first = head(&index, KeyHash(0), 10);
        let second = head(&index, KeyHash(1), 20);
        let old = cache
            .insert_if_current(&index, first, KeyHash(0), LogAddress(10), record)
            .unwrap()
            .unwrap();
        let pinned = cache
            .resolve(&index, KeyHash(0), b"a")
            .unwrap()
            .cached
            .unwrap();
        assert!(
            cache
                .insert_if_current(
                    &index,
                    second,
                    KeyHash(1),
                    LogAddress(20),
                    bytes(b"b", b"v")
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(cache.allocated_bytes(), charge);
        assert_eq!(
            index.prepare(KeyHash(0)).unwrap().head,
            IndexHead::Log(LogAddress(10))
        );
        assert_eq!(Record::decode(pinned.encoded()).unwrap().key, b"a");
        drop(pinned);
        assert_eq!(cache.allocated_bytes(), 0);
        let new = cache
            .insert_if_current(
                &index,
                second,
                KeyHash(1),
                LogAddress(20),
                bytes(b"b", b"v"),
            )
            .unwrap()
            .unwrap();
        assert_ne!(old, new);
        cache.invalidate(&index, old).unwrap();
        assert!(
            cache
                .resolve(&index, KeyHash(1), b"b")
                .unwrap()
                .cached
                .is_some()
        );
        assert_eq!(cache.allocated_bytes(), charge);
    }
    #[test]
    fn 迟到安装与不同索引身份拒绝且同标签键不串值() {
        let index = index();
        let cache = cache(4096);
        let stale = head(&index, KeyHash(0), 10);
        let current = head(&index, KeyHash(0), 20);
        assert!(
            cache
                .insert_if_current(
                    &index,
                    stale,
                    KeyHash(0),
                    LogAddress(10),
                    bytes(b"a", b"old")
                )
                .unwrap()
                .is_none()
        );
        let old = cache
            .insert_if_current(
                &index,
                current,
                KeyHash(0),
                LogAddress(20),
                bytes(b"a", b"new"),
            )
            .unwrap()
            .unwrap();
        let expected = cache.resolve(&index, KeyHash(2), b"b").unwrap().entry;
        cache
            .insert_if_current(
                &index,
                expected,
                KeyHash(2),
                LogAddress(8),
                bytes(b"b", b"other"),
            )
            .unwrap()
            .unwrap();
        assert!(
            cache
                .resolve(&index, KeyHash(0), b"a")
                .unwrap()
                .cached
                .is_none()
        );
        let record = cache
            .resolve(&index, KeyHash(2), b"b")
            .unwrap()
            .cached
            .unwrap();
        assert_eq!(Record::decode(record.encoded()).unwrap().value, b"other");
        cache.invalidate(&index, old).unwrap();
        let foreign = MemIndex::new(IndexConfig { buckets: 2 }).unwrap();
        assert!(cache.resolve(&foreign, KeyHash(0), b"a").is_err());
        assert!(
            cache
                .resolve(&index, KeyHash(2), b"b")
                .unwrap()
                .cached
                .is_some()
        );
    }
}
