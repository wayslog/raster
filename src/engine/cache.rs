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
        if !self.config.cache.enabled || self.coordinator.snapshot()?.id.is_some() {
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
        let result = (|| {
            if let Some((source, bytes)) = lookup.cache_record(self.cache.max_record_bytes())? {
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
        self.cache
            .with_normalized_index(&self.index, || self.index.snapshot()?.encode())
    }
}
