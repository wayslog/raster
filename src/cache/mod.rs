//! 缓存独立地址空间；缓存命中、失效和主日志头规范化尚未实现。
use crate::{config::CacheConfig, index::EntrySnapshot, types::*};

pub(crate) struct CacheEntry {
    pub address: CacheAddress,
    pub source: LogAddress,
    pub version: CheckpointVersion,
}
pub(crate) struct ReadCache {
    config: CacheConfig,
    allocated_bytes: crate::sync::AtomicU64,
}
impl ReadCache {
    pub fn lookup(&self, _entry: EntrySnapshot) -> Result<Option<CacheEntry>, Error> {
        Err(Error::unimplemented("cache::lookup"))
    }
    pub fn insert_if_current(
        &self,
        _expected: EntrySnapshot,
        _record: Vec<u8>,
    ) -> Result<(), Error> {
        Err(Error::unimplemented("cache::insert"))
    }
    pub fn invalidate(&self, _address: CacheAddress) -> Result<(), Error> {
        Err(Error::unimplemented("cache::invalidate"))
    }
    pub fn normalize_head(&self, _address: CacheAddress) -> Result<LogAddress, Error> {
        Err(Error::unimplemented("cache::normalize"))
    }
    pub fn evict_step(&self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("cache::evict"))
    }
}
