//! Process-local identities that remain distinct after their owners are dropped.
use crate::types::Error;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

/// An identity check is not a lifetime or access permission. Actual I/O and
/// resident values retain their own owners independently of this copyable ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InstanceId(u64);
impl InstanceId {
    pub fn new() -> Result<Self, Error> {
        Self::allocate(&NEXT_INSTANCE)
    }
    fn allocate(next: &AtomicU64) -> Result<Self, Error> {
        next.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
            .map(Self)
            .map_err(|_| Error::CapacityExceeded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exhausted_identity_source_never_wraps_or_reuses_an_id() {
        let next = AtomicU64::new(u64::MAX - 1);
        assert_eq!(
            InstanceId::allocate(&next).unwrap(),
            InstanceId(u64::MAX - 1)
        );
        for _ in 0..3 {
            assert!(matches!(
                InstanceId::allocate(&next),
                Err(Error::CapacityExceeded)
            ));
            assert_eq!(next.load(Ordering::SeqCst), u64::MAX);
        }
    }
    #[test]
    fn concurrent_instance_creation_produces_distinct_ids() {
        let next = AtomicU64::new(1);
        let mut ids = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..256)
                            .map(|_| InstanceId::allocate(&next).unwrap().0)
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        ids.sort_unstable();
        assert_eq!(ids, (1..=2048).collect::<Vec<_>>());
    }
}
