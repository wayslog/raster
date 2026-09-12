//! Cache modification obtains all business write arbitration;Read only obtains the lease of owned records.
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
                Err(_) => {
                    return Err(Error::InvalidState(
                        "Cache installation encounters business arbitration lock poisoning",
                    ));
                }
            }
        }
        // Inspection must be within arbitration:Otherwise GC Cache can be cleared after checking,The installation was late and the cache header was returned to the bucket to be cleared..
        if self.coordinator.snapshot()?.id.is_some() {
            return Ok(());
        }
        let result = (|| {
            if let Some((source, bytes)) = lookup.cache_record(self.cache.max_record_bytes())? {
                // Newer link headers with the same tag may still be valid,But the old key read this time has been logically truncated.
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
                _ => Error::InvalidState(
                    "Index snapshot encounters business arbitration lock poisoning",
                ),
            })?);
        }
        self.cache.with_normalized_index(&self.index, || {
            let begin = self.log.frontiers()?.begin;
            let mut image = self.index.snapshot()?;
            // GC There may still be old buckets that have not been cleared after failure.;These chain heads no longer belong to the current logical key space.
            image.entries.retain(|entry| matches!(entry.head, crate::index::IndexHead::Log(address) if address >= begin));
            image.encode()
        })
    }
}
