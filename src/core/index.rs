//! In-memory hash index (FASTER `mem_index.h`, `hash_bucket.h`).
//!
//! Each 64-byte bucket holds seven 8-byte entries and an overflow pointer.
//! Entry: `address:48 | tag:14 | reserved:1 | tentative:1`. Lookups only load;
//! publication is one CAS. Entries are not removed in this phase, so slot
//! references stay valid for the table's lifetime.
use super::address::Address;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
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
    /// Unconditionally points the entry at `address` (recovery replay only).
    pub fn store(&self, address: Address) {
        let entry = Entry::new(address, self.entry.tag(), false);
        self.atomic.store(entry.0, Ordering::Release);
    }

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

const CHECKPOINT_MAGIC: u64 = 0x5241_5354_4552_4854; // "RASTERHT"

impl HashIndex {
    pub fn buckets(&self) -> u64 {
        self.mask + 1
    }

    /// Writes a fuzzy copy of the table: `ht` holds a header and the main
    /// buckets, and each of the table's parts writes its overflow buckets to
    /// `<ofb stem>.<part>.dat`, with links stored as 1-based positions in that
    /// file. Concurrent updates may or may not
    /// be captured; recovery replays the log from the checkpoint start to
    /// repair the difference. Returns the number of overflow buckets written.
    pub fn write_checkpoint(&self, ht: &Path, ofb: &Path) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;
        let parts = self.checkpoint_parts();
        let ht = File::create(ht)?;
        let mut header = Vec::with_capacity(24);
        for word in [CHECKPOINT_MAGIC, self.buckets(), parts as u64] {
            header.extend_from_slice(&word.to_le_bytes());
        }
        ht.write_all_at(&header, 0)?;
        let total = (0..parts)
            .map(|part| self.write_part(&ht, &Self::overflow_path(ofb, part), part, parts))
            .sum::<io::Result<u64>>()?;
        ht.sync_all()?;
        Ok(total)
    }

    fn checkpoint_parts(&self) -> usize {
        (self.buckets.len() >> 16).clamp(1, 8)
    }

    fn part_range(&self, part: usize, parts: usize) -> std::ops::Range<usize> {
        let len = self.buckets.len();
        len * part / parts..len * (part + 1) / parts
    }

    fn overflow_path(ofb: &Path, part: usize) -> std::path::PathBuf {
        let stem = ofb.file_stem().and_then(|s| s.to_str()).unwrap_or("ofb");
        ofb.with_file_name(format!("{stem}.{part}.dat"))
    }

    fn write_part(&self, ht: &File, ofb: &Path, part: usize, parts: usize) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;
        const CHUNK_BUCKETS: usize = 1 << 14;
        #[inline(always)]
        fn push(out: &mut Vec<u64>, bucket: &Bucket, has_next: bool, count: &mut u64) {
            let mut words = [0u64; ENTRIES + 1];
            for (word, atomic) in words.iter_mut().zip(&bucket.entries) {
                *word = atomic.load(Ordering::Relaxed);
            }
            if has_next {
                *count += 1;
                words[ENTRIES] = *count;
            }
            out.extend_from_slice(&words);
        }
        let mut ofb_out = File::create(ofb)?;
        let range = self.part_range(part, parts);
        let mut main = Vec::with_capacity(CHUNK_BUCKETS * 8);
        let mut overflow = Vec::new();
        let mut count = 0u64;
        let mut index = range.start;
        for chunk in self.buckets[range].chunks(CHUNK_BUCKETS) {
            main.clear();
            for bucket in chunk {
                let mut next = Self::next(bucket);
                push(&mut main, bucket, next.is_some(), &mut count);
                while let Some(bucket) = next {
                    next = Self::next(bucket);
                    push(&mut overflow, bucket, next.is_some(), &mut count);
                }
            }
            ht.write_all_at(&words_as_bytes(&main), 24 + index as u64 * 64)?;
            index += chunk.len();
            if overflow.len() >= CHUNK_BUCKETS * 8 {
                ofb_out.write_all(&words_as_bytes(&overflow))?;
                overflow.clear();
            }
        }
        ofb_out.write_all(&words_as_bytes(&overflow))?;
        ofb_out.sync_all()?;
        Ok(count)
    }

    /// Loads a checkpoint written by [`HashIndex::write_checkpoint`] into this
    /// empty table and clears tentative entries.
    pub fn read_checkpoint(&self, ht: &Path, ofb: &Path) -> io::Result<()> {
        use std::io::Read;
        let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());
        let mut ht = File::open(ht)?;
        let mut header = [0u8; 24];
        ht.read_exact(&mut header)?;
        let word = |bytes: &[u8], i: usize| {
            u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().expect("8 bytes"))
        };
        if word(&header, 0) != CHECKPOINT_MAGIC {
            return Err(invalid("not an index checkpoint"));
        }
        if word(&header, 1) != self.buckets() {
            return Err(invalid("index checkpoint has a different bucket count"));
        }
        let parts = word(&header, 2) as usize;
        if parts == 0 || parts > self.buckets.len() {
            return Err(invalid("invalid index checkpoint part count"));
        }
        let load = |bucket: &Bucket, bytes: &[u8]| -> bool {
            for (i, atomic) in bucket.entries.iter().enumerate() {
                let entry = Entry(word(bytes, i));
                atomic.store(
                    if entry.tentative() { 0 } else { entry.0 },
                    Ordering::Relaxed,
                );
            }
            word(bytes, ENTRIES) != 0
        };
        const CHUNK_BUCKETS: usize = 1 << 14;
        let mut chunk = vec![0u8; CHUNK_BUCKETS * 64];
        let mut overflow = [0u8; 64];
        for part in 0..parts {
            let mut ofb =
                io::BufReader::with_capacity(1 << 20, File::open(Self::overflow_path(ofb, part))?);
            for buckets in self.buckets[self.part_range(part, parts)].chunks(CHUNK_BUCKETS) {
                let bytes = &mut chunk[..buckets.len() * 64];
                ht.read_exact(bytes)?;
                for (bucket, bytes) in buckets.iter().zip(bytes.as_chunks::<64>().0) {
                    let mut current = bucket;
                    let mut has_next = load(current, bytes);
                    while has_next {
                        ofb.read_exact(&mut overflow)
                            .map_err(|_| invalid("missing overflow bucket"))?;
                        current = Self::append_bucket(current);
                        has_next = load(current, &overflow);
                    }
                }
            }
        }
        Ok(())
    }
}

/// The little-endian byte image of `words`, without copying on little-endian targets.
#[cfg(target_endian = "little")]
fn words_as_bytes(words: &[u64]) -> std::borrow::Cow<'_, [u8]> {
    // SAFETY: u64 has no padding, any byte pattern is a valid u8, and the
    // slice covers exactly the words' memory.
    std::borrow::Cow::Borrowed(unsafe {
        std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), std::mem::size_of_val(words))
    })
}

#[cfg(not(target_endian = "little"))]
fn words_as_bytes(words: &[u64]) -> std::borrow::Cow<'_, [u8]> {
    std::borrow::Cow::Owned(words.iter().flat_map(|w| w.to_le_bytes()).collect())
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

#[cfg(test)]
mod checkpoint_tests {
    use super::*;

    #[test]
    fn checkpoint_round_trip_with_overflow_and_tentative_entries() {
        let dir = std::env::temp_dir().join(format!("raster-index-ckpt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let index = HashIndex::new(2);
        for tag in 0..40u64 {
            let slot = index.find_or_create(tag << TAG_SHIFT | (tag & 1));
            slot.try_update(Address::new(3, 64 + tag * 24));
        }
        // A leftover tentative entry must not survive recovery.
        index.buckets[0].entries[0]
            .store(Entry::new(Address::INVALID, 99, true).0, Ordering::Release);
        let (ht, ofb) = (dir.join("ht.dat"), dir.join("ofb.dat"));
        assert!(index.write_checkpoint(&ht, &ofb).unwrap() > 0);
        let restored = HashIndex::new(2);
        restored.read_checkpoint(&ht, &ofb).unwrap();
        for tag in 1..40u64 {
            let slot = restored.find(tag << TAG_SHIFT | (tag & 1)).unwrap();
            assert_eq!(slot.entry.address(), Address::new(3, 64 + tag * 24));
        }
        assert!(restored.find(99 << TAG_SHIFT).is_none());
        assert!(HashIndex::new(4).read_checkpoint(&ht, &ofb).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod parallel_checkpoint_tests {
    use super::*;

    #[test]
    fn multi_part_checkpoint_round_trips_overflow_in_every_part() {
        let dir = std::env::temp_dir().join(format!("raster-index-parts-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let index = HashIndex::new(1 << 17);
        assert_eq!(index.checkpoint_parts(), 2);
        // Twenty tags in a bucket of each part force overflow chains in both.
        let buckets = [5u64, (1 << 16) + 9];
        for &bucket in &buckets {
            for tag in 0..20u64 {
                let slot = index.find_or_create(tag << TAG_SHIFT | bucket);
                slot.try_update(Address::new(2, 64 + tag * 24 + bucket));
            }
        }
        let (ht, ofb) = (dir.join("ht.dat"), dir.join("ofb.dat"));
        assert_eq!(index.write_checkpoint(&ht, &ofb).unwrap(), 4);
        let restored = HashIndex::new(1 << 17);
        restored.read_checkpoint(&ht, &ofb).unwrap();
        for &bucket in &buckets {
            for tag in 0..20u64 {
                let slot = restored.find(tag << TAG_SHIFT | bucket).unwrap();
                assert_eq!(
                    slot.entry.address(),
                    Address::new(2, 64 + tag * 24 + bucket)
                );
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod checkpoint_timing {
    use super::*;

    /// `cargo test --release --lib checkpoint_timing -- --ignored --nocapture`
    #[test]
    #[ignore = "timing only"]
    fn time_large_index_checkpoint() {
        let dir = std::env::temp_dir().join(format!("raster-index-time-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let index = HashIndex::new(1 << 22);
        for key in 0..4_000_000u64 {
            let hash = key.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            index.find_or_create(hash).try_update(Address::new(1, 64));
        }
        let (ht, ofb) = (dir.join("ht.dat"), dir.join("ofb.dat"));
        for _ in 0..3 {
            let started = std::time::Instant::now();
            index.write_checkpoint(&ht, &ofb).unwrap();
            eprintln!("write_checkpoint: {:?}", started.elapsed());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
