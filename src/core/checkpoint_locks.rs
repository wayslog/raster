//! Per-bucket CPR locks (FASTER `checkpoint_locks.h`).
//!
//! Each word holds `{old_count: u32, new_count: u32}`. Version-v operations
//! share the old side, version-v+1 operations share the new side, and the two
//! sides exclude each other, so v and v+1 never interleave on one key.
use std::sync::atomic::{AtomicU64, Ordering};

const NEW_ONE: u64 = 1 << 32;
const OLD_MASK: u64 = NEW_ONE - 1;

pub struct CheckpointLocks {
    words: Box<[AtomicU64]>,
    mask: u64,
}

impl CheckpointLocks {
    /// One lock per index bucket; `buckets` must be a power of two.
    pub fn new(buckets: u64) -> Self {
        debug_assert!(buckets.is_power_of_two());
        Self {
            words: (0..buckets).map(|_| AtomicU64::new(0)).collect(),
            mask: buckets - 1,
        }
    }

    #[inline(always)]
    fn word(&self, hash: u64) -> &AtomicU64 {
        &self.words[(hash & self.mask) as usize]
    }

    fn try_lock(&self, hash: u64, one: u64, blocked_by: impl Fn(u64) -> bool) -> bool {
        let word = self.word(hash);
        let mut current = word.load(Ordering::Acquire);
        loop {
            if blocked_by(current) {
                return false;
            }
            match word.compare_exchange_weak(
                current,
                current + one,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn try_lock_old(&self, hash: u64) -> bool {
        self.try_lock(hash, 1, |word| word >= NEW_ONE)
    }

    pub fn try_lock_new(&self, hash: u64) -> bool {
        self.try_lock(hash, NEW_ONE, |word| word & OLD_MASK != 0)
    }

    pub fn unlock_old(&self, hash: u64) {
        let previous = self.word(hash).fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous & OLD_MASK != 0);
    }

    pub fn unlock_new(&self, hash: u64) {
        let previous = self.word(hash).fetch_sub(NEW_ONE, Ordering::AcqRel);
        debug_assert!(previous >= NEW_ONE);
    }

    pub fn old_locked(&self, hash: u64) -> bool {
        self.word(hash).load(Ordering::Acquire) & OLD_MASK != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sides_share_internally_and_exclude_each_other() {
        let locks = CheckpointLocks::new(4);
        assert!(locks.try_lock_old(1) && locks.try_lock_old(5));
        assert!(locks.old_locked(1));
        assert!(!locks.try_lock_new(1), "new side blocked by old holders");
        assert!(locks.try_lock_new(2), "other buckets are independent");
        locks.unlock_old(1);
        locks.unlock_old(5);
        assert!(locks.try_lock_new(1) && locks.try_lock_new(1));
        assert!(!locks.try_lock_old(1));
        locks.unlock_new(1);
        locks.unlock_new(1);
        assert!(locks.try_lock_old(1));
    }
}
