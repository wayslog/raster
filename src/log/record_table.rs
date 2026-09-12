//! Address-indexed resident records with sparse blocks and ordered occupancy bits.
//! Point reads share the page-directory lock; mutations require its write guard.
use crate::types::{Error, LogAddress, PageId};
use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
    sync::{Arc, RwLock, RwLockWriteGuard},
};

// Every log record has at least a 48-byte header and a 4-byte checksum. Two
// record starts therefore cannot occupy the same 32-byte directory cell.
const GRANULE: usize = 32;
const BLOCK_CELLS: usize = 8;
struct Cells<T> {
    values: [Option<Arc<T>>; BLOCK_CELLS],
    offsets: [u8; BLOCK_CELLS],
}
struct Page<T> {
    live: usize,
    blocks: Box<[Option<Box<Cells<T>>>]>,
    occupied: Box<[u64]>,
}
impl<T> Page<T> {
    fn new(cells: usize) -> Result<Self, Error> {
        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(cells.div_ceil(BLOCK_CELLS))
            .map_err(|_| Error::OutOfMemory)?;
        blocks.resize_with(cells.div_ceil(BLOCK_CELLS), || None);
        let mut occupied = Vec::new();
        occupied
            .try_reserve_exact(cells.div_ceil(64))
            .map_err(|_| Error::OutOfMemory)?;
        occupied.resize(cells.div_ceil(64), 0);
        Ok(Self {
            live: 0,
            blocks: blocks.into_boxed_slice(),
            occupied: occupied.into_boxed_slice(),
        })
    }
    fn bit(&mut self, cell: usize, present: bool) {
        // All occupancy writers hold the directory lock. Point reads do not
        // inspect these bits; ordered readers hold that same directory lock.
        let word = &mut self.occupied[cell / 64];
        let mask = 1u64 << (cell % 64);
        *word = if present { *word | mask } else { *word & !mask };
    }
    fn find(&self, low: usize, high: usize, reverse: bool) -> Option<usize> {
        if low > high {
            return None;
        }
        let bits = |word: usize| {
            let mut bits = self.occupied[word];
            if word == low / 64 {
                bits &= u64::MAX << (low % 64);
            }
            if word == high / 64 {
                bits &= u64::MAX >> (63 - high % 64);
            }
            bits
        };
        if reverse {
            for word in (low / 64..=high / 64).rev() {
                let bits = bits(word);
                if bits != 0 {
                    return Some(word * 64 + (63 - bits.leading_zeros()) as usize);
                }
            }
        } else {
            for word in low / 64..=high / 64 {
                let bits = bits(word);
                if bits != 0 {
                    return Some(word * 64 + bits.trailing_zeros() as usize);
                }
            }
        }
        None
    }
}
struct Directory<T> {
    pages: BTreeMap<PageId, Page<T>>,
}
pub(super) struct RecordTable<T> {
    page_bytes: usize,
    directory: RwLock<Directory<T>>,
}
pub(super) struct RecordGuard<'a, T> {
    table: &'a RecordTable<T>,
    directory: RwLockWriteGuard<'a, Directory<T>>,
}
pub(super) struct RecordRange<'a, T> {
    table: &'a RecordTable<T>,
    pages: &'a BTreeMap<PageId, Page<T>>,
    low: u64,
    high: u64,
    ended: bool,
}
fn poisoned() -> Error {
    Error::InvalidState("Record table lock poisoning")
}
impl<T> RecordTable<T> {
    pub fn new(page_bytes: usize) -> Result<Self, Error> {
        if page_bytes < GRANULE || !page_bytes.is_power_of_two() {
            return Err(Error::InvalidState(
                "Invalid resident record directory geometry",
            ));
        }
        Ok(Self {
            page_bytes,
            directory: RwLock::new(Directory {
                pages: BTreeMap::new(),
            }),
        })
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.directory.is_poisoned() {
            return Err(poisoned());
        }
        Ok(())
    }
    fn location(&self, address: LogAddress) -> (PageId, usize, u8) {
        let page = PageId(address.0 / self.page_bytes as u64);
        let offset = address.0 as usize & (self.page_bytes - 1);
        (page, offset / GRANULE, (offset % GRANULE) as u8)
    }
    pub fn get(&self, address: LogAddress) -> Result<Option<Arc<T>>, Error> {
        self.healthy()?;
        let directory = self.directory.read().map_err(|_| poisoned())?;
        self.lookup(&directory, address)
    }
    fn lookup(
        &self,
        directory: &Directory<T>,
        address: LogAddress,
    ) -> Result<Option<Arc<T>>, Error> {
        let (page, cell, offset) = self.location(address);
        let Some(root) = directory.pages.get(&page) else {
            return Ok(None);
        };
        let Some(cells) = root.blocks[cell / BLOCK_CELLS].as_ref() else {
            return Ok(None);
        };
        if cells.offsets[cell % BLOCK_CELLS] != offset {
            return Ok(None);
        }
        Ok(cells.values[cell % BLOCK_CELLS].clone())
    }
    pub fn lock(&self) -> Result<RecordGuard<'_, T>, Error> {
        self.healthy()?;
        let directory = self.directory.write().map_err(|_| poisoned())?;
        self.healthy()?;
        Ok(RecordGuard {
            table: self,
            directory,
        })
    }
    pub fn insert(&self, address: LogAddress, value: Arc<T>) -> Result<(), Error> {
        address.validate()?;
        // Failed insertion must drop the candidate outside the directory lock.
        let mut value = Some(value);
        {
            let mut guard = self.lock()?;
            guard.insert(address, &mut value)
        }
    }
    pub fn remove_checked(
        &self,
        address: LogAddress,
        before: impl FnOnce(&T) -> Result<(), Error>,
    ) -> Result<Arc<T>, Error> {
        let result = {
            let mut guard = self.lock()?;
            let value = self
                .lookup(&guard.directory, address)?
                .ok_or(Error::RangeTruncated)?;
            before(&value)?;
            guard.remove(&address)?.ok_or(Error::RangeTruncated)?
        };
        Ok(result)
    }
}
impl<T> RecordGuard<'_, T> {
    fn insert(&mut self, address: LogAddress, value: &mut Option<Arc<T>>) -> Result<(), Error> {
        let (page, cell, offset) = self.table.location(address);
        if let std::collections::btree_map::Entry::Vacant(entry) = self.directory.pages.entry(page)
        {
            entry.insert(Page::new(self.table.page_bytes / GRANULE)?);
        }
        let root = self
            .directory
            .pages
            .get_mut(&page)
            .expect("Page directory initialized");
        let count = root.live.checked_add(1).ok_or(Error::CapacityExceeded)?;
        let cells = root.blocks[cell / BLOCK_CELLS].get_or_insert_with(|| {
            Box::new(Cells {
                values: std::array::from_fn(|_| None),
                offsets: [0; BLOCK_CELLS],
            })
        });
        if cells.values[cell % BLOCK_CELLS].is_some() {
            return Err(Error::InvalidState(
                "Record address is published repeatedly",
            ));
        }
        cells.offsets[cell % BLOCK_CELLS] = offset;
        cells.values[cell % BLOCK_CELLS] = value.take();
        root.bit(cell, true);
        root.live = count;
        Ok(())
    }
    pub fn remove(&mut self, address: &LogAddress) -> Result<Option<Arc<T>>, Error> {
        let (page, cell, offset) = self.table.location(*address);
        let Some(root) = self.directory.pages.get_mut(&page) else {
            return Ok(None);
        };
        let Some(cells) = root.blocks[cell / BLOCK_CELLS].as_mut() else {
            return Ok(None);
        };
        let result = if cells.offsets[cell % BLOCK_CELLS] == offset {
            cells.values[cell % BLOCK_CELLS].take()
        } else {
            None
        };
        if result.is_some() {
            root.bit(cell, false);
            root.live -= 1;
            if root.live == 0 {
                self.directory.pages.remove(&page);
            }
        }
        Ok(result)
    }
    pub fn range<R: RangeBounds<LogAddress>>(&self, bounds: R) -> RecordRange<'_, T> {
        let low = match bounds.start_bound() {
            Bound::Included(a) => Some(a.0),
            Bound::Excluded(a) => a.0.checked_add(1),
            Bound::Unbounded => Some(0),
        };
        let high = match bounds.end_bound() {
            Bound::Included(a) => Some(a.0),
            Bound::Excluded(a) => a.0.checked_sub(1),
            Bound::Unbounded => Some(u64::MAX),
        };
        RecordRange {
            table: self.table,
            pages: &self.directory.pages,
            low: low.unwrap_or(0),
            high: high.unwrap_or(0),
            ended: low.zip(high).is_none_or(|(a, b)| a > b),
        }
    }
}
impl<T> RecordRange<'_, T> {
    fn take(&mut self, reverse: bool) -> Result<Option<(LogAddress, Arc<T>)>, Error> {
        while !self.ended {
            self.table.healthy()?;
            let page_bytes = self.table.page_bytes as u64;
            let mut pages = self
                .pages
                .range(PageId(self.low / page_bytes)..=PageId(self.high / page_bytes));
            let selected = if reverse {
                pages.next_back()
            } else {
                pages.next()
            };
            let Some((&page, root)) = selected else {
                self.ended = true;
                return Ok(None);
            };
            let start = page.0 * page_bytes;
            let low = self.low.saturating_sub(start) as usize / GRANULE;
            let high = (self.high - start).min(page_bytes - 1) as usize / GRANULE;
            if let Some(cell) = root.find(low, high, reverse) {
                let cells = root.blocks[cell / BLOCK_CELLS]
                    .as_ref()
                    .ok_or(Error::InvalidState("Occupied record block is missing"))?;
                let address = LogAddress(
                    start + (cell * GRANULE) as u64 + u64::from(cells.offsets[cell % BLOCK_CELLS]),
                );
                let value = cells.values[cell % BLOCK_CELLS]
                    .clone()
                    .ok_or(Error::InvalidState("Occupied record cell is empty"))?;
                if address.0 < self.low || address.0 > self.high {
                    if reverse {
                        match (start + (cell * GRANULE) as u64).checked_sub(1) {
                            Some(high) if high >= self.low => self.high = high,
                            _ => self.ended = true,
                        }
                    } else {
                        match (start + (cell * GRANULE) as u64).checked_add(GRANULE as u64) {
                            Some(low) if low <= self.high => self.low = low,
                            _ => self.ended = true,
                        }
                    }
                    continue;
                }
                if reverse {
                    match (start + (cell * GRANULE) as u64).checked_sub(1) {
                        Some(high) if high >= self.low => self.high = high,
                        _ => self.ended = true,
                    }
                } else {
                    match (start + (cell * GRANULE) as u64).checked_add(GRANULE as u64) {
                        Some(low) if low <= self.high => self.low = low,
                        _ => self.ended = true,
                    }
                }
                return Ok(Some((address, value)));
            }
            if reverse {
                match start.checked_sub(1) {
                    Some(high) if high >= self.low => self.high = high,
                    _ => self.ended = true,
                }
            } else {
                match start.checked_add(page_bytes) {
                    Some(low) if low <= self.high => self.low = low,
                    _ => self.ended = true,
                }
            }
        }
        Ok(None)
    }
    fn step(&mut self, reverse: bool) -> Option<Result<(LogAddress, Arc<T>), Error>> {
        match self.take(reverse) {
            Ok(value) => value.map(Ok),
            Err(error) => {
                self.ended = true;
                Some(Err(error))
            }
        }
    }
}
impl<T> Iterator for RecordRange<'_, T> {
    type Item = Result<(LogAddress, Arc<T>), Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.step(false)
    }
}
impl<T> DoubleEndedIterator for RecordRange<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.step(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[test]
    fn ordered_ranges_match_an_independent_tree_in_both_directions() {
        let table = RecordTable::new(4096).unwrap();
        let mut expected = BTreeMap::new();
        for i in (0..200u64).rev() {
            let address = LogAddress(i * 96 + i % 31);
            table.insert(address, Arc::new(i)).unwrap();
            expected.insert(address, i);
        }
        for low in [0, 1, 31, 32, 4095, 4096, 8193, 19_000] {
            for high in [low, low + 1, low + 319, 20_000] {
                let guard = table.lock().unwrap();
                let wanted: Vec<_> = expected
                    .range(LogAddress(low)..LogAddress(high))
                    .map(|(&a, &v)| (a, v))
                    .collect();
                let actual: Vec<_> = guard
                    .range(LogAddress(low)..LogAddress(high))
                    .map(|r| {
                        let (a, v) = r.unwrap();
                        (a, *v)
                    })
                    .collect();
                assert_eq!(actual, wanted);
                let reverse: Vec<_> = guard
                    .range(LogAddress(low)..LogAddress(high))
                    .rev()
                    .map(|r| r.unwrap().0)
                    .collect();
                assert_eq!(
                    reverse,
                    wanted.iter().rev().map(|r| r.0).collect::<Vec<_>>()
                );
                let mut mixed = guard.range(LogAddress(low)..LogAddress(high));
                let mut wanted: std::collections::VecDeque<_> = wanted.into();
                while !wanted.is_empty() {
                    assert_eq!(
                        mixed.next().unwrap().unwrap().0,
                        wanted.pop_front().unwrap().0
                    );
                    if let Some(back) = wanted.pop_back() {
                        assert_eq!(mixed.next_back().unwrap().unwrap().0, back.0);
                    }
                }
                assert!(mixed.next().is_none());
                assert!(mixed.next_back().is_none());
            }
        }
    }
    #[test]
    fn range_bound_variants_match_standard_range_membership() {
        let table = RecordTable::new(4096).unwrap();
        let addresses = [0, 39, 101, 4095, 4110, 8199];
        for address in addresses {
            table
                .insert(LogAddress(address), Arc::new(address))
                .unwrap();
        }
        let mut bounds = vec![Bound::Unbounded];
        for address in [0, 1, 39, 40, 4095, 4096, 8199, 8200, u64::MAX] {
            bounds.push(Bound::Included(LogAddress(address)));
            bounds.push(Bound::Excluded(LogAddress(address)));
        }
        let guard = table.lock().unwrap();
        for start in &bounds {
            for end in &bounds {
                let range = (*start, *end);
                let expected: Vec<_> = addresses
                    .iter()
                    .copied()
                    .map(LogAddress)
                    .filter(|address| range.contains(address))
                    .collect();
                let forward: Vec<_> = guard.range(range).map(|r| r.unwrap().0).collect();
                let backward: Vec<_> = guard.range(range).rev().map(|r| r.unwrap().0).collect();
                assert_eq!(forward, expected, "{range:?}");
                assert_eq!(
                    backward,
                    expected.into_iter().rev().collect::<Vec<_>>(),
                    "{range:?}"
                );
            }
        }
    }
    #[test]
    fn full_addresses_and_old_leases_survive_sparse_page_removal_and_recreation() {
        let table = RecordTable::new(4096).unwrap();
        for (address, value) in [(7, 11), (4096 + 7, 22), (3 * 4096 + 7, 33)] {
            table.insert(LogAddress(address), Arc::new(value)).unwrap();
        }
        assert!(table.get(LogAddress(8)).unwrap().is_none());
        assert!(table.insert(LogAddress(8), Arc::new(99)).is_err());
        let old = table.get(LogAddress(7)).unwrap().unwrap();
        drop(table.remove_checked(LogAddress(7), |_| Ok(())).unwrap());
        assert_eq!(*old, 11);
        assert!(table.get(LogAddress(7)).unwrap().is_none());
        table
            .insert(LogAddress(5 * 4096 + 7), Arc::new(55))
            .unwrap();
        assert_eq!(*table.get(LogAddress(4096 + 7)).unwrap().unwrap(), 22);
        assert_eq!(*table.get(LogAddress(5 * 4096 + 7)).unwrap().unwrap(), 55);
        assert!(
            !table
                .lock()
                .unwrap()
                .directory
                .pages
                .contains_key(&PageId(0))
        );
    }
    #[test]
    fn point_reads_do_not_wait_for_another_directory_reader() {
        let table = RecordTable::new(4096).unwrap();
        table.insert(LogAddress(0), Arc::new(1)).unwrap();
        table.insert(LogAddress(256), Arc::new(2)).unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let finished = std::thread::scope(|scope| {
            let directory = table.directory.read().unwrap();
            let reader = scope.spawn(|| {
                sent.send(*table.get(LogAddress(256)).unwrap().unwrap())
                    .unwrap();
            });
            let result = received.recv_timeout(std::time::Duration::from_secs(2));
            drop(directory);
            reader.join().unwrap();
            result
        });
        assert_eq!(finished.unwrap(), 2);
    }
    struct Probe {
        table: std::sync::Weak<RecordTable<Probe>>,
        outside: Arc<AtomicBool>,
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            if let Some(table) = self.table.upgrade() {
                self.outside
                    .store(table.directory.try_write().is_ok(), Ordering::SeqCst);
            }
        }
    }
    #[test]
    fn rejected_and_removed_values_are_destroyed_outside_the_directory_lock() {
        let table = Arc::new(RecordTable::new(4096).unwrap());
        let original = Arc::new(AtomicBool::new(false));
        let rejected = Arc::new(AtomicBool::new(false));
        table
            .insert(
                LogAddress(7),
                Arc::new(Probe {
                    table: Arc::downgrade(&table),
                    outside: original.clone(),
                }),
            )
            .unwrap();
        assert!(
            table
                .insert(
                    LogAddress(7),
                    Arc::new(Probe {
                        table: Arc::downgrade(&table),
                        outside: rejected.clone()
                    })
                )
                .is_err()
        );
        assert!(rejected.load(Ordering::SeqCst));
        let lease = table.get(LogAddress(7)).unwrap().unwrap();
        drop(table.remove_checked(LogAddress(7), |_| Ok(())).unwrap());
        assert!(!original.load(Ordering::SeqCst));
        drop(lease);
        assert!(original.load(Ordering::SeqCst));
    }
    #[test]
    fn directory_poisoning_closes_point_reads_and_ordered_access() {
        let table = RecordTable::new(4096).unwrap();
        table.insert(LogAddress(0), Arc::new(1)).unwrap();
        table.insert(LogAddress(256), Arc::new(2)).unwrap();
        assert!(
            std::panic::catch_unwind(|| {
                let _directory = table.lock().unwrap();
                panic!("injected record directory poison");
            })
            .is_err()
        );
        assert!(table.get(LogAddress(256)).is_err());
        assert!(table.lock().is_err());
    }
}
