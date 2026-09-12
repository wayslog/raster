//! Page allocator only allocates exclusive ranges;initialization,Publishing and persistence are the responsibility of the record owner.
#[cfg(test)]
mod tail_tests;
use crate::{
    sync::{AtomicU64, Mutex, PUBLISH_ORDER},
    types::*,
};
use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
    sync::Arc,
};

struct Allocation {
    pointer: NonNull<u8>,
    layout: Layout,
}
// SAFETY: assigned to the last Arc Survive before exiting;Only exclusive and non-overlapping PageRange Expose variable bytes.
unsafe impl Send for Allocation {}
// SAFETY: The page itself does not expose shared byte references,Control locks ensure that each range is allocated only once.
unsafe impl Sync for Allocation {}
impl Allocation {
    fn new(bytes: usize) -> Result<Self, Error> {
        let layout = Layout::from_size_align(bytes, bytes).map_err(|_| Error::CapacityExceeded)?;
        // SAFETY: Layout Verified and page length is non-zero;Conversion of null pointer to allocation failed.
        let pointer = NonNull::new(unsafe { alloc_zeroed(layout) }).ok_or(Error::OutOfMemory)?;
        Ok(Self { pointer, layout })
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        // SAFETY: pointer from the same Layout of alloc_zeroed,Arc The last owner released exactly once.
        unsafe { dealloc(self.pointer.as_ptr(), self.layout) };
    }
}
struct Entry {
    id: PageId,
    generation: Generation,
    page: Option<Arc<Allocation>>,
    next: usize,
}
struct State {
    entries: Vec<Entry>,
    next_id: u64,
    preallocated: bool,
    spare: Vec<Arc<Allocation>>,
}
pub(crate) struct PagePool {
    bytes: usize,
    max_pages: usize,
    state: Mutex<State>,
    // Writers publish a validated allocation frontier while holding state.
    // This is an allocation bound, not a record-publication or durability fence.
    published_tail: AtomicU64,
}
/// Unclonable exclusive scope;Move without changing address,Forgetting only prevents full page release.
pub(crate) struct PageRange {
    page: Arc<Allocation>,
    id: PageId,
    generation: Generation,
    offset: usize,
    len: usize,
}
impl PageRange {
    #[cfg(test)]
    pub fn page_id(&self) -> PageId {
        self.id
    }
    pub fn generation(&self) -> Generation {
        self.generation
    }
    pub fn address(&self) -> Result<LogAddress, Error> {
        LogAddress::from_page_offset(self.id, self.offset as u64, self.page.layout.size() as u64)
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn pointer(&self) -> NonNull<u8> {
        // SAFETY: Validate on assignment offset + len Do not exceed the page,len non-zero.
        unsafe { NonNull::new_unchecked(self.page.pointer.as_ptr().add(self.offset)) }
    }
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: PageRange Not cloneable;The ranges do not overlap with each other,&mut self Exclude other secure access to the same scope.
        unsafe { std::slice::from_raw_parts_mut(self.pointer().as_ptr(), self.len) }
    }
}
impl PagePool {
    pub fn new(bytes: usize, max_pages: usize) -> Result<Self, Error> {
        if !bytes.is_power_of_two()
            || max_pages == 0
            || Layout::from_size_align(bytes, bytes).is_err()
        {
            return Err(Error::InvalidConfig {
                field: "page_pool",
                reason: "The page size must be the allocable power of two and the number of pages must be non-zero",
            });
        }
        bytes
            .checked_mul(max_pages)
            .ok_or(Error::CapacityExceeded)?;
        Ok(Self {
            bytes,
            max_pages,
            state: Mutex::new(State {
                entries: Vec::new(),
                next_id: 0,
                preallocated: false,
                spare: Vec::new(),
            }),
            published_tail: AtomicU64::new(0),
        })
    }
    /// During recovery, all old pages are provided by disk,The first memory page starts with the given logical page number.
    pub fn new_at(bytes: usize, max_pages: usize, first: PageId) -> Result<Self, Error> {
        let mut pool = Self::new(bytes, max_pages)?;
        let tail = LogAddress::from_page_offset(first, 0, bytes as u64)?;
        pool.state
            .get_mut()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?
            .next_id = first.0;
        *pool.published_tail.get_mut() = tail.0;
        Ok(pool)
    }
    /// Preallocate all memory pages before creating or restoring a release;Do not create logical pages,log or disk file.
    pub fn preallocate(&mut self) -> Result<(), Error> {
        let state = self
            .state
            .get_mut()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?;
        if state.preallocated {
            return Ok(());
        }
        if !state.entries.is_empty() {
            return Err(Error::InvalidState(
                "Page pool allocation has begun,Cannot switch preallocation policy",
            ));
        }
        let mut spare = Vec::new();
        spare
            .try_reserve_exact(self.max_pages)
            .map_err(|_| Error::OutOfMemory)?;
        for _ in 0..self.max_pages {
            spare.push(Arc::new(Allocation::new(self.bytes)?));
        }
        state
            .entries
            .try_reserve_exact(self.max_pages)
            .map_err(|_| Error::OutOfMemory)?;
        state.spare = spare;
        state.preallocated = true;
        Ok(())
    }
    /// Page load allocation includes unused and pre-allocated pages retained after recycling,Does not contain pool metadata or OS RSS.
    pub fn memory_usage(&self) -> Result<(usize, usize), Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?;
        let active = state
            .entries
            .iter()
            .filter(|entry| entry.page.is_some())
            .count();
        let bytes = active
            .checked_add(state.spare.len())
            .and_then(|n| n.checked_mul(self.bytes))
            .ok_or(Error::CapacityExceeded)?;
        Ok((active, bytes))
    }
    pub fn generation(&self, page: PageId) -> Result<Generation, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?;
        state
            .entries
            .iter()
            .find(|entry| entry.id == page && entry.page.is_some())
            .map(|entry| entry.generation)
            .ok_or(Error::RangeTruncated)
    }
    /// Do not access allocator state when reading only published records,Still reject poisoned page pool.
    pub fn ensure_healthy(&self) -> Result<(), Error> {
        if self.state.is_poisoned() {
            Err(Error::InvalidState("Page pool lock poisoning"))
        } else {
            Ok(())
        }
    }
    pub fn tail(&self) -> Result<LogAddress, Error> {
        self.ensure_healthy()?;
        Ok(LogAddress(self.published_tail.load(PUBLISH_ORDER)))
    }
    /// Close the remaining allocated space of the latest page;No record created,Nor does it change ownership of existing ranges.
    pub fn pad_tail(&self) -> Result<LogAddress, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?;
        if state.entries.is_empty() {
            return LogAddress::from_page_offset(PageId(state.next_id), 0, self.bytes as u64);
        }
        let id = state.next_id.checked_sub(1).ok_or(Error::InvalidState(
            "The allocated page is missing a logical number",
        ))?;
        let entry = state
            .entries
            .iter_mut()
            .find(|entry| entry.id.0 == id)
            .ok_or(Error::InvalidState(
                "The latest logical page does not exist",
            ))?;
        let end = LogAddress::from_page_offset(PageId(id), 0, self.bytes as u64)?
            .checked_add(self.bytes as u64)?;
        entry.next = self.bytes;
        self.published_tail.store(end.0, PUBLISH_ORDER);
        Ok(end)
    }
    pub fn reserve(&self, len: usize, alignment: usize) -> Result<PageRange, Error> {
        if len == 0 || len > self.bytes || !alignment.is_power_of_two() || alignment > self.bytes {
            return Err(Error::CapacityExceeded);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?;
        let current_id = state.next_id.checked_sub(1);
        for entry in &mut state.entries {
            // The log is only appended to the latest logical page,Unable to backfill old page gaps causing address regression.
            if Some(entry.id.0) != current_id {
                continue;
            }
            if let Some(page) = &entry.page {
                let start = entry
                    .next
                    .checked_add(alignment - 1)
                    .ok_or(Error::CapacityExceeded)?
                    & !(alignment - 1);
                if start.checked_add(len).is_some_and(|end| end <= self.bytes) {
                    let tail = LogAddress::from_page_offset(entry.id, 0, self.bytes as u64)?
                        .checked_add((start + len) as u64)?;
                    entry.next = start + len;
                    self.published_tail.store(tail.0, PUBLISH_ORDER);
                    return Ok(PageRange {
                        page: page.clone(),
                        id: entry.id,
                        generation: entry.generation,
                        offset: start,
                        len,
                    });
                }
            }
        }
        let free = state
            .entries
            .iter()
            .position(|e| e.page.is_none() && e.generation.0 < u64::MAX);
        if free.is_none() && state.entries.len() == self.max_pages {
            return Err(Error::CapacityExceeded);
        }
        let id = PageId(state.next_id);
        LogAddress::from_page_offset(id, (self.bytes - 1) as u64, self.bytes as u64)?;
        let tail =
            LogAddress::from_page_offset(id, 0, self.bytes as u64)?.checked_add(len as u64)?;
        let next_id = state
            .next_id
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        let page = if state.preallocated {
            state.spare.pop().ok_or(Error::CapacityExceeded)?
        } else {
            Arc::new(Allocation::new(self.bytes)?)
        };
        let generation = if let Some(i) = free {
            Generation(state.entries[i].generation.0 + 1)
        } else {
            Generation(0)
        };
        let entry = Entry {
            id,
            generation,
            page: Some(page.clone()),
            next: len,
        };
        if let Some(i) = free {
            state.entries[i] = entry;
        } else {
            state
                .entries
                .try_reserve(1)
                .map_err(|_| Error::OutOfMemory)?;
            state.entries.push(entry);
        }
        state.next_id = next_id;
        self.published_tail.store(tail.0, PUBLISH_ORDER);
        Ok(PageRange {
            page,
            id,
            generation,
            offset: 0,
            len,
        })
    }
    /// Can only be called after logical retirement and external retention constraints are satisfied;Reject if scope owner still exists.
    pub fn release(&self, id: PageId, generation: Generation) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("Page pool lock poisoning"))?;
        let entry = state
            .entries
            .iter_mut()
            .find(|e| e.id == id && e.generation == generation && e.page.is_some())
            .ok_or(Error::RangeTruncated)?;
        if Arc::strong_count(entry.page.as_ref().expect("Checked page existence")) != 1 {
            return Err(Error::Busy);
        }
        let mut page = entry.page.take().expect("Checked page existence");
        if state.preallocated {
            let allocation = Arc::get_mut(&mut page)
                .expect("allocation not exposed Weak and all scopes have exited");
            // SAFETY: Page pool exclusive last one Arc,old PageRange All logged out;The original page release condition also ensures that the value destruction is completed..
            unsafe {
                allocation
                    .pointer
                    .as_ptr()
                    .write_bytes(0, allocation.layout.size())
            };
            state.spare.push(page);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_cold_start_page_number_does_not_occupy_the_memory_budget_and_the_tail_does_not_return_to_zero()
     {
        let pool = PagePool::new_at(64, 1, PageId(100)).unwrap();
        assert_eq!(pool.tail().unwrap(), LogAddress(6400));
        assert_eq!(pool.pad_tail().unwrap(), LogAddress(6400));
        assert!(pool.generation(PageId(99)).is_err());
        let range = pool.reserve(64, 8).unwrap();
        assert_eq!(range.address().unwrap(), LogAddress(6400));
        assert!(pool.reserve(8, 8).is_err());
        let generation = range.generation();
        drop(range);
        pool.release(PageId(100), generation).unwrap();
        let next = pool.reserve(8, 8).unwrap();
        assert_eq!(next.address().unwrap(), LogAddress(6464));
        assert_eq!(next.generation(), Generation(1));
        assert!(PagePool::new_at(64, 1, PageId(u64::MAX)).is_err());
    }
    #[test]
    fn small_records_cannot_backfill_the_old_page_after_a_page_spread_and_the_last_page_will_not_go_backwards_when_released()
     {
        let pool = PagePool::new(64, 2).unwrap();
        let first = pool.reserve(24, 8).unwrap();
        let second = pool.reserve(48, 8).unwrap();
        let third = pool.reserve(8, 8).unwrap();
        assert_eq!(first.address().unwrap(), LogAddress(0));
        assert_eq!(second.address().unwrap(), LogAddress(64));
        assert_eq!(third.address().unwrap(), LogAddress(112));
        let page = second.page_id();
        let generation = second.generation();
        drop(second);
        drop(third);
        pool.release(page, generation).unwrap();
        let next = pool.reserve(8, 8).unwrap();
        assert_eq!(next.address().unwrap(), LogAddress(128));
        assert_eq!(next.generation(), Generation(1));
        assert_eq!(first.address().unwrap(), LogAddress(0));
    }
    #[test]
    fn range_alignment_does_not_overlap_and_page_budget_does_not_expand_silently() {
        let pool = PagePool::new(64, 1).unwrap();
        let mut first = pool.reserve(7, 1).unwrap();
        let mut second = pool.reserve(8, 16).unwrap();
        assert_eq!(first.address().unwrap(), LogAddress(0));
        assert_eq!(second.address().unwrap(), LogAddress(16));
        assert_eq!(second.pointer().as_ptr() as usize % 16, 0);
        first.bytes_mut().fill(7);
        second.bytes_mut().fill(8);
        assert_eq!(first.bytes_mut(), [7; 7]);
        assert_eq!(second.bytes_mut(), [8; 8]);
        let _tail = pool.reserve(40, 1).unwrap();
        assert!(pool.reserve(1, 1).is_err());
    }
    #[test]
    fn it_is_released_after_all_scopes_exit_and_the_reuse_generation_is_incremented() {
        let pool = PagePool::new(64, 1).unwrap();
        let range = pool.reserve(64, 8).unwrap();
        let id = range.page_id();
        let generation = range.generation();
        assert!(pool.release(id, generation).is_err());
        drop(range);
        pool.release(id, generation).unwrap();
        let mut next = pool.reserve(64, 8).unwrap();
        assert_ne!(next.page_id(), id);
        assert_eq!(next.generation(), Generation(1));
        assert_eq!(next.bytes_mut(), [0; 64]);
        assert!(pool.release(id, generation).is_err());
    }
    #[test]
    fn the_exclusive_scope_can_be_moved_across_threads_and_pool_destruction_does_not_affect_the_surviving_scope()
     {
        let pool = PagePool::new(64, 1).unwrap();
        let mut a = pool.reserve(16, 8).unwrap();
        let mut b = pool.reserve(16, 8).unwrap();
        drop(pool);
        std::thread::scope(|s| {
            s.spawn(|| a.bytes_mut().fill(1));
            s.spawn(|| b.bytes_mut().fill(2));
        });
        assert_eq!(a.bytes_mut(), [1; 16]);
        assert_eq!(b.bytes_mut(), [2; 16]);
    }
    #[test]
    fn invalid_length_alignment_overflows_and_forgotten_ranges_are_safely_rejected() {
        assert!(PagePool::new(0, 1).is_err());
        assert!(PagePool::new(3, 1).is_err());
        assert!(PagePool::new(64, 0).is_err());
        assert!(PagePool::new(64, usize::MAX).is_err());
        let pool = PagePool::new(64, 1).unwrap();
        for (len, align) in [(0, 1), (65, 1), (1, 0), (1, 3), (1, 128)] {
            assert!(pool.reserve(len, align).is_err());
        }
        let range = pool.reserve(64, 1).unwrap();
        let id = range.page_id();
        let generation = range.generation();
        std::mem::forget(range);
        assert!(pool.release(id, generation).is_err());
    }
}

#[cfg(test)]
mod mutable_poison_tests {
    use super::*;
    use crate::{config::LogConfig, log::HybridLog, schema::builtin::AtomicU64Value};

    #[test]
    fn variable_lookup_rejects_existing_page_pool_boundaries_and_record_table_poisoning() {
        for kind in 0..3 {
            let log = HybridLog::new(
                LogConfig {
                    page_bytes: 256,
                    memory_pages: 2,
                    mutable_fraction: 0.5,
                },
                Arc::new(AtomicU64Value),
            )
            .unwrap();
            let address = log
                .finish_initialization(log.reserve_record(b"key", None, 7).unwrap())
                .unwrap();
            let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match kind {
                0 => {
                    let _guard = log.pool.state.lock().unwrap();
                    panic!("Page pool test poisoning");
                }
                1 => {
                    let _guard = log.state.write().unwrap();
                    panic!("Boundary test poisoning");
                }
                _ => {
                    let _guard = log.records.lock().unwrap();
                    panic!("Record table test poisoning");
                }
            }));
            assert!(poison.is_err());
            let expected = [
                "Page pool lock poisoning",
                "Log boundary lock poisoning",
                "Record table lock poisoning",
            ][kind];
            assert!(
                matches!(log.find_mutable(b"key", Some(address)), Err(Error::InvalidState(reason)) if reason == expected)
            );
        }
    }
}

#[cfg(test)]
mod preallocation_tests {
    use super::*;
    #[test]
    fn pre_allocation_does_not_occupy_logical_addresses_and_all_leases_are_cleared_before_reuse_of_the_same_physical_page()
     {
        let mut pool = PagePool::new_at(4096, 2, PageId(7)).unwrap();
        assert_eq!(pool.memory_usage().unwrap(), (0, 0));
        pool.preallocate().unwrap();
        pool.preallocate().unwrap();
        assert_eq!(pool.memory_usage().unwrap(), (0, 8192));
        assert_eq!(pool.tail().unwrap(), LogAddress(7 * 4096));
        assert!(pool.generation(PageId(7)).is_err());
        let mut first = pool.reserve(4096, 8).unwrap();
        first.bytes_mut().fill(0xa5);
        let pointer = first.pointer();
        let second = pool.reserve(4096, 8).unwrap();
        assert!(matches!(
            pool.release(first.page_id(), first.generation()),
            Err(Error::Busy)
        ));
        assert!(pool.reserve(1, 1).is_err());
        drop(first);
        pool.release(PageId(7), Generation(0)).unwrap();
        let mut third = pool.reserve(4096, 8).unwrap();
        assert_eq!(third.pointer(), pointer);
        assert_eq!(third.page_id(), PageId(9));
        assert_eq!(third.generation(), Generation(1));
        assert!(third.bytes_mut().iter().all(|&b| b == 0));
        assert_eq!(pool.memory_usage().unwrap(), (2, 8192));
        drop(pool);
        drop(second);
        third.bytes_mut().fill(7);
        assert_eq!(third.bytes_mut()[0], 7);
    }
}
