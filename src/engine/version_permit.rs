//! Preserve logical version registration during suspend,Does not retain record leases or user context references.
use crate::types::*;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
const SHARD_COUNT: usize = 64;
type Registrations = BTreeMap<(u64, u64), usize>;
#[repr(align(64))]
#[derive(Default)]
struct FailureFlag(AtomicBool);
#[repr(align(64))]
struct Shard {
    active: Mutex<Registrations>,
    failed: Arc<FailureFlag>,
}
struct PoisonNotification<'a> {
    active: &'a Mutex<Registrations>,
    failed: &'a FailureFlag,
}
impl Drop for PoisonNotification<'_> {
    fn drop(&mut self) {
        // The actual mutex guard is dropped first, so std::sync::Mutex decides
        // whether this unwind poisoned the state. Do not duplicate its panic checks.
        if self.active.is_poisoned() {
            self.failed.0.store(true, Ordering::Release);
        }
    }
}
impl Shard {
    fn is_poisoned(&self) -> bool {
        self.failed.0.load(Ordering::Acquire)
    }
    fn with_state<R>(
        &self,
        use_state: impl FnOnce(&mut Registrations) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if self.is_poisoned() {
            return Err(Error::InvalidState(
                "Request version registration lock poisoning",
            ));
        }
        // Locals drop in reverse declaration order, including during unwinding.
        let _notification = PoisonNotification {
            active: &self.active,
            failed: &self.failed,
        };
        let mut guard = self.active.lock().map_err(|_| {
            self.failed.0.store(true, Ordering::Release);
            Error::InvalidState("Request version registration lock poisoning")
        })?;
        if self.is_poisoned() {
            return Err(Error::InvalidState(
                "Request version registration lock poisoning",
            ));
        }
        use_state(&mut guard)
    }
}
pub(crate) struct VersionPermits {
    shards: [Arc<Shard>; SHARD_COUNT],
}
impl Default for VersionPermits {
    fn default() -> Self {
        let failed = Arc::new(FailureFlag::default());
        Self {
            shards: std::array::from_fn(|_| {
                Arc::new(Shard {
                    active: Mutex::new(BTreeMap::new()),
                    failed: failed.clone(),
                })
            }),
        }
    }
}
pub(crate) struct VersionPermit {
    active: Arc<Shard>,
    key: (u64, u64),
    ready: AtomicBool,
}
impl VersionPermits {
    fn shard(&self, hash: KeyHash) -> &Arc<Shard> {
        &self.shards[hash.0 as usize % SHARD_COUNT]
    }
    pub fn reserve(
        &self,
        hash: KeyHash,
        version: CheckpointVersion,
    ) -> Result<VersionPermit, Error> {
        // Version ordering is defined per full hash. Different shards never need
        // to inspect each other's registrations; collisions retain separate keys.
        let shard = self.shard(hash);
        let key = (hash.0, version.0);
        let ready = shard.with_state(|active| {
            // Phase observation lag calls cannot be inserted before a registered new version;Rejection occurs before sequence number is accepted.
            if active
                .range((hash.0, version.0)..=(hash.0, u64::MAX))
                .any(|(&(h, v), _)| h == hash.0 && v > version.0)
            {
                return Err(Error::Busy);
            }
            let ready = active.range((hash.0, 0)..key).next().is_none();
            let count = active
                .get(&key)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(Error::CapacityExceeded)?;
            active.insert(key, count);
            Ok(ready)
        })?;
        Ok(VersionPermit {
            active: shard.clone(),
            key,
            ready: AtomicBool::new(ready),
        })
    }
}
impl VersionPermit {
    pub fn ready(&self) -> Result<bool, Error> {
        if self.active.is_poisoned() {
            return Err(Error::InvalidState(
                "Request version registration lock poisoning",
            ));
        }
        // A live registration rejects every subsequent older version. Once all
        // preceding registrations have exited, readiness therefore cannot regress.
        // Release/acquire carries their completion ordering to other observers.
        if self.ready.load(Ordering::Acquire) {
            return Ok(true);
        }
        let ready = self
            .active
            .with_state(|active| Ok(active.range((self.key.0, 0)..self.key).next().is_none()))?;
        if ready {
            self.ready.store(true, Ordering::Release);
        }
        Ok(ready)
    }
}
impl Drop for VersionPermit {
    fn drop(&mut self) {
        let _ = self.active.with_state(|active| {
            if let Some(count) = active.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    active.remove(&self.key);
                }
            }
            Ok(())
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hashes_in_the_same_shard_keep_independent_version_histories() {
        let permits = VersionPermits::default();
        let a = KeyHash(1);
        let b = KeyHash(1 + SHARD_COUNT as u64);
        let old = permits.reserve(a, CheckpointVersion(0)).unwrap();
        let new = permits.reserve(a, CheckpointVersion(1)).unwrap();
        let unrelated = permits.reserve(b, CheckpointVersion(u64::MAX)).unwrap();
        assert!(!new.ready().unwrap());
        assert!(unrelated.ready().unwrap());
        drop(old);
        assert!(new.ready().unwrap());
        assert!(unrelated.ready().unwrap());
    }
    #[test]
    fn poisoning_one_shard_immediately_closes_the_entire_registry() {
        let permits = VersionPermits::default();
        let ready = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
        assert!(
            std::panic::catch_unwind(|| {
                let _ = permits
                    .shard(KeyHash(2))
                    .with_state::<()>(|_| panic!("injected registry poison"));
            })
            .is_err()
        );
        // Neither operation touches the poisoned shard before checking failure.
        assert!(permits.reserve(KeyHash(3), CheckpointVersion(0)).is_err());
        assert!(ready.ready().is_err());
    }
    #[test]
    fn dropping_a_permit_during_an_existing_unwind_does_not_poison_the_registry() {
        let permits = VersionPermits::default();
        assert!(
            std::panic::catch_unwind(|| {
                let _permit = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
                panic!("unrelated client unwind");
            })
            .is_err()
        );
        let permit = permits.reserve(KeyHash(1), CheckpointVersion(0)).unwrap();
        assert!(permit.ready().unwrap());
    }
    #[test]
    fn readiness_cannot_be_revoked_by_registering_an_older_version() {
        let permits = VersionPermits::default();
        let old = permits.reserve(KeyHash(7), CheckpointVersion(0)).unwrap();
        let current = permits.reserve(KeyHash(7), CheckpointVersion(1)).unwrap();
        assert!(!current.ready().unwrap());
        drop(old);
        assert!(current.ready().unwrap());
        let future = permits
            .reserve(KeyHash(7), CheckpointVersion(u64::MAX))
            .unwrap();
        for _ in 0..16 {
            assert!(current.ready().unwrap());
            assert!(!future.ready().unwrap());
            assert!(matches!(
                permits.reserve(KeyHash(7), CheckpointVersion(0)),
                Err(Error::Busy)
            ));
        }
        drop(current);
        assert!(future.ready().unwrap());
        assert!(matches!(
            permits.reserve(KeyHash(7), CheckpointVersion(1)),
            Err(Error::Busy)
        ));
    }
    #[test]
    fn an_already_ready_permit_does_not_ignore_registry_poisoning() {
        let permits = VersionPermits::default();
        let permit = permits.reserve(KeyHash(7), CheckpointVersion(0)).unwrap();
        assert!(permit.ready().unwrap());
        let result = std::panic::catch_unwind(|| {
            let _ = permits
                .shard(KeyHash(7))
                .with_state::<()>(|_| panic!("injected registry poison"));
        });
        assert!(result.is_err());
        assert!(permit.ready().is_err());
        assert!(permits.reserve(KeyHash(8), CheckpointVersion(0)).is_err());
    }
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
        assert!(
            permits
                .shards
                .iter()
                .all(|shard| shard.with_state(|active| Ok(active.is_empty())).unwrap())
        );
    }
}
