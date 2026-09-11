//! 缓存修改取得全部业务写入仲裁；读取只取得拥有型记录租约。
use super::Engine;
use crate::{
    cache::Resolved, index::EntrySnapshot, log::lookup::LogLookup, schema::Schema, types::*,
};
impl<S: Schema> Engine<S> {
    pub(crate) fn resolve_index(&self, hash: KeyHash, key: &[u8]) -> Result<Resolved, Error> {
        if self.config.cache.enabled {
            return self.cache.resolve(&self.index, hash, key);
        }
        let entry = self.index.prepare(hash)?;
        Ok(Resolved {
            entry,
            head: Self::head(entry)?,
            cached: None,
        })
    }
    pub(crate) fn populate_cache(
        &self,
        hash: KeyHash,
        expected: EntrySnapshot,
        lookup: &LogLookup,
    ) -> Result<(), Error> {
        if !self.config.cache.enabled {
            return Ok(());
        }
        let mut guards = Vec::new();
        if guards.try_reserve_exact(self.operations.len()).is_err() {
            return Ok(());
        }
        for gate in &self.operations {
            match gate.try_lock() {
                Ok(guard) => guards.push(guard),
                Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
                Err(_) => return Err(Error::InvalidState("缓存安装遇到业务仲裁锁中毒")),
            }
        }
        // 检查须在仲裁内：否则 GC 可在检查后清空缓存，迟到安装又把缓存头放回待清桶。
        if self.coordinator.snapshot()?.id.is_some() {
            return Ok(());
        }
        let result = (|| {
            if let Some((source, bytes)) = lookup.cache_record(self.cache.max_record_bytes())? {
                // 同标签的较新链头可能仍有效，但本次读出的旧键已被逻辑截断。
                if source < self.log.frontiers()?.begin {
                    return Ok(());
                }
                self.cache
                    .insert_if_current(&self.index, expected, hash, source, bytes)?;
            }
            Ok(())
        })();
        match result {
            Err(Error::OutOfMemory | Error::CapacityExceeded) => Ok(()),
            result => result,
        }
    }
    pub(crate) fn snapshot_index(&self) -> Result<Vec<u8>, Error> {
        let mut guards = Vec::new();
        guards
            .try_reserve_exact(self.operations.len())
            .map_err(|_| Error::OutOfMemory)?;
        for gate in &self.operations {
            guards.push(gate.try_lock().map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => Error::Busy,
                _ => Error::InvalidState("索引快照遇到业务仲裁锁中毒"),
            })?);
        }
        self.cache.with_normalized_index(&self.index, || {
            let begin = self.log.frontiers()?.begin;
            let mut image = self.index.snapshot()?;
            // GC 失败后可能尚有未清完的旧桶；这些链头已经不属于当前逻辑键空间。
            image.entries.retain(|entry| matches!(entry.head, crate::index::IndexHead::Log(address) if address >= begin));
            image.encode()
        })
    }
}
