//! Preserve logical version registration during suspend,Does not retain record leases or user context references.
use crate::types::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[derive(Default)]
pub(crate) struct VersionPermits {
    active: Arc<Mutex<BTreeMap<(u64, u64), usize>>>,
}
pub(crate) struct VersionPermit {
    active: Arc<Mutex<BTreeMap<(u64, u64), usize>>>,
    key: (u64, u64),
}
impl VersionPermits {
    pub fn reserve(
        &self,
        hash: KeyHash,
        version: CheckpointVersion,
    ) -> Result<VersionPermit, Error> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::InvalidState("Request version registration lock poisoning"))?;
        // Phase observation lag calls cannot be inserted before a registered new version;Rejection occurs before sequence number is accepted.
        if active
            .range((hash.0, version.0)..=(hash.0, u64::MAX))
            .any(|(&(h, v), _)| h == hash.0 && v > version.0)
        {
            return Err(Error::Busy);
        }
        let key = (hash.0, version.0);
        let count = active
            .get(&key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        active.insert(key, count);
        Ok(VersionPermit {
            active: self.active.clone(),
            key,
        })
    }
}
impl VersionPermit {
    pub fn ready(&self) -> Result<bool, Error> {
        let active = self
            .active
            .lock()
            .map_err(|_| Error::InvalidState("Request version registration lock poisoning"))?;
        Ok(active.range((self.key.0, 0)..self.key).next().is_none())
    }
}
impl Drop for VersionPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock()
            && let Some(count) = active.get_mut(&self.key)
        {
            *count -= 1;
            if *count == 0 {
                active.remove(&self.key);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_new_version_can_be_executed_only_after_all_old_versions_have_been_exited_and_different_keys_do_not_block_each_other()
     {
        let permits = VersionPermits::default();
        let old = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
        let same = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
        let new = permits.reserve(KeyHash(1), CheckpointVersion(1)).unwrap();
        let other = permits.reserve(KeyHash(2), CheckpointVersion(1)).unwrap();
        assert!(old.ready().unwrap());
        assert!(same.ready().unwrap());
        assert!(other.ready().unwrap());
        assert!(!new.ready().unwrap());
        assert!(matches!(
            permits.reserve(KeyHash(1), CheckpointVersion(0)),
            Err(Error::Busy)
        ));
        drop(old);
        assert!(!new.ready().unwrap());
        drop(same);
        assert!(new.ready().unwrap());
        drop(new);
        drop(other);
        assert!(permits.active.lock().unwrap().is_empty());
    }
}
