//! In-memory hash index (FASTER `mem_index.h`, `hash_bucket.h`).
//!
//! Each 64-byte bucket holds seven 8-byte entries and an overflow pointer.
//! Entry: `address:48 | tag:14 | reserved:1 | tentative:1`. Lookups only load;
//! publication is one CAS. Entries are not removed in this phase, so slot
//! references stay valid for the table's lifetime.
use super::address::Address;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

const ENTRIES: usize = 7;
const TAG_SHIFT: u32 = 48;
const TAG_MASK: u64 = (1 << 14) - 1;
const TENTATIVE: u64 = 1 << 63;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry(u64);

impl Entry {
    #[inline(always)]
    fn new(address: Address, tag: u64, tentative: bool) -> Self {
        Self(address.control() | (tag << TAG_SHIFT) | if tentative { TENTATIVE } else { 0 })
    }
    #[inline(always)]
    pub fn address(self) -> Address {
        Address::from_control(self.0)
    }
    #[inline(always)]
    fn tag(self) -> u64 {
        (self.0 >> TAG_SHIFT) & TAG_MASK
    }
    #[inline(always)]
    fn tentative(self) -> bool {
        self.0 & TENTATIVE != 0
    }
    #[inline(always)]
    fn unused(self) -> bool {
        self.0 == 0
    }
}

#[repr(C, align(64))]
#[derive(Default)]
struct Bucket {
    entries: [AtomicU64; ENTRIES],
    overflow: AtomicPtr<Bucket>,
}

/// A located index slot and the entry value observed in it.
#[derive(Clone, Copy)]
pub struct Slot<'a> {
    atomic: &'a AtomicU64,
    pub entry: Entry,
}

impl Slot<'_> {
    /// Publishes `address` if the slot still holds the observed entry.
    #[inline(always)]
    pub fn try_update(&self, address: Address) -> bool {
        let desired = Entry::new(address, self.entry.tag(), false);
        self.atomic
            .compare_exchange(self.entry.0, desired.0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

pub struct HashIndex {
    buckets: Box<[Bucket]>,
    mask: u64,
}

impl HashIndex {
    pub fn new(buckets: u64) -> Self {
        let len = buckets.max(1).next_power_of_two();
        Self {
            buckets: (0..len).map(|_| Bucket::default()).collect(),
            mask: len - 1,
        }
    }

    #[inline(always)]
    fn locate(&self, hash: u64) -> (&Bucket, u64) {
        let bucket = &self.buckets[(hash & Address::MAX.control() & self.mask) as usize];
        (bucket, (hash >> TAG_SHIFT) & TAG_MASK)
    }

    #[inline(always)]
    fn next(bucket: &Bucket) -> Option<&Bucket> {
        let next = bucket.overflow.load(Ordering::Acquire);
        // SAFETY: overflow buckets are never freed while the table is alive.
        unsafe { next.as_ref() }
    }

    /// Finds the non-tentative entry for `hash`'s tag.
    #[inline]
    pub fn find(&self, hash: u64) -> Option<Slot<'_>> {
        let (mut bucket, tag) = self.locate(hash);
        loop {
            for atomic in &bucket.entries {
                let entry = Entry(atomic.load(Ordering::Acquire));
                if !entry.unused() && entry.tag() == tag && !entry.tentative() {
                    return Some(Slot { atomic, entry });
                }
            }
            bucket = Self::next(bucket)?;
        }
    }

    /// Finds the entry for `hash`'s tag, creating one with an invalid address
    /// if absent. Uses tentative insertion so a tag appears at most once.
    #[inline]
    pub fn find_or_create(&self, hash: u64) -> Slot<'_> {
        let (head, tag) = self.locate(hash);
        loop {
            let mut free: Option<&AtomicU64> = None;
            let mut bucket = head;
            loop {
                for atomic in &bucket.entries {
                    let entry = Entry(atomic.load(Ordering::Acquire));
                    if entry.unused() {
                        free.get_or_insert(atomic);
                    } else if entry.tag() == tag && !entry.tentative() {
                        return Slot { atomic, entry };
                    }
                }
                match Self::next(bucket) {
                    Some(next) => bucket = next,
                    None if free.is_none() => {
                        bucket = Self::append_bucket(bucket);
                    }
                    None => break,
                }
            }
            let free = free.expect("a free slot was found or appended");
            let tentative = Entry::new(Address::INVALID, tag, true);
            if free
                .compare_exchange(0, tentative.0, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            if self.has_conflict(head, tag, free) {
                free.store(0, Ordering::Release);
                continue;
            }
            let entry = Entry::new(Address::INVALID, tag, false);
            free.store(entry.0, Ordering::Release);
            return Slot {
                atomic: free,
                entry,
            };
        }
    }

    fn has_conflict(&self, mut bucket: &Bucket, tag: u64, own: &AtomicU64) -> bool {
        loop {
            for atomic in &bucket.entries {
                let entry = Entry(atomic.load(Ordering::Acquire));
                if !ptr::eq(atomic, own) && !entry.unused() && entry.tag() == tag {
                    return true;
                }
            }
            match Self::next(bucket) {
                Some(next) => bucket = next,
                None => return false,
            }
        }
    }

    fn append_bucket(bucket: &Bucket) -> &Bucket {
        let fresh = Box::into_raw(Box::default());
        match bucket.overflow.compare_exchange(
            ptr::null_mut(),
            fresh,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // SAFETY: just published; owned by the table from now on.
            Ok(_) => unsafe { &*fresh },
            Err(winner) => {
                // SAFETY: `fresh` was never published.
                drop(unsafe { Box::from_raw(fresh) });
                // SAFETY: published overflow buckets live as long as the table.
                unsafe { &*winner }
            }
        }
    }
}

impl Drop for HashIndex {
    fn drop(&mut self) {
        for bucket in self.buckets.iter() {
            let mut next = bucket.overflow.load(Ordering::Acquire);
            while !next.is_null() {
                // SAFETY: each overflow bucket was leaked from a Box exactly once.
                let owned = unsafe { Box::from_raw(next) };
                next = owned.overflow.load(Ordering::Acquire);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn bucket_is_one_cache_line() {
        assert_eq!(size_of::<Bucket>(), 64);
    }

    #[test]
    fn create_update_find_with_overflow() {
        let index = HashIndex::new(1);
        // Same bucket, twenty distinct tags: forces two overflow buckets.
        for tag in 0..20u64 {
            let hash = tag << TAG_SHIFT;
            let slot = index.find_or_create(hash);
            assert_eq!(slot.entry.address(), Address::INVALID);
            assert!(slot.try_update(Address::new(1, 64 + tag * 8)));
        }
        for tag in 0..20u64 {
            let slot = index.find(tag << TAG_SHIFT).unwrap();
            assert_eq!(slot.entry.address(), Address::new(1, 64 + tag * 8));
        }
        assert!(index.find(99 << TAG_SHIFT).is_none());
    }

    #[test]
    fn concurrent_creation_yields_one_entry_per_tag() {
        let index = Arc::new(HashIndex::new(1));
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let index = index.clone();
                std::thread::spawn(move || {
                    for tag in 0..64u64 {
                        let slot = index.find_or_create(tag << TAG_SHIFT);
                        slot.try_update(Address::new(1, 64 + t * 8));
                    }
                })
            })
            .collect();
        threads.into_iter().for_each(|t| t.join().unwrap());
        let mut seen = std::collections::HashMap::new();
        let mut bucket = Some(&index.buckets[0]);
        while let Some(b) = bucket {
            for atomic in &b.entries {
                let entry = Entry(atomic.load(Ordering::Acquire));
                if !entry.unused() {
                    *seen.entry(entry.tag()).or_insert(0) += 1;
                }
            }
            bucket = HashIndex::next(b);
        }
        assert_eq!(seen.len(), 64);
        assert!(seen.values().all(|&n| n == 1));
    }
}
