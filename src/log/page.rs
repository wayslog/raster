//! 页分配器只分配独占范围；初始化、发布及持久化由记录所有者负责。
use crate::{sync::Mutex, types::*};
use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
    sync::Arc,
};

struct Allocation {
    pointer: NonNull<u8>,
    layout: Layout,
}
// SAFETY: 分配在最后一个 Arc 退出前存活；仅由独占且不重叠的 PageRange 暴露可变字节。
unsafe impl Send for Allocation {}
// SAFETY: 页本身不暴露共享字节引用，控制锁保证每个范围只被分配一次。
unsafe impl Sync for Allocation {}
impl Allocation {
    fn new(bytes: usize) -> Result<Self, Error> {
        let layout = Layout::from_size_align(bytes, bytes).map_err(|_| Error::CapacityExceeded)?;
        // SAFETY: Layout 已验证且页长度非零；空指针转换为分配失败。
        let pointer = NonNull::new(unsafe { alloc_zeroed(layout) }).ok_or(Error::OutOfMemory)?;
        Ok(Self { pointer, layout })
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        // SAFETY: pointer 来自相同 Layout 的 alloc_zeroed，Arc 最后一个所有者恰好释放一次。
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
}
pub(crate) struct PagePool {
    bytes: usize,
    max_pages: usize,
    state: Mutex<State>,
}
/// 不可克隆的独占范围；移动不改变地址，遗忘只会阻碍整页释放。
pub(crate) struct PageRange {
    page: Arc<Allocation>,
    id: PageId,
    generation: Generation,
    offset: usize,
    len: usize,
}
impl PageRange {
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
        // SAFETY: 分配时验证 offset + len 不越页，len 非零。
        unsafe { NonNull::new_unchecked(self.page.pointer.as_ptr().add(self.offset)) }
    }
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: PageRange 不可克隆；范围互不重叠，&mut self 排除同范围的其他安全访问。
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
                reason: "页大小须为可分配的二次幂且页数非零",
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
            }),
        })
    }
    pub fn generation(&self, page: PageId) -> Result<Generation, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("页池锁中毒"))?;
        state
            .entries
            .iter()
            .find(|entry| entry.id == page && entry.page.is_some())
            .map(|entry| entry.generation)
            .ok_or(Error::RangeTruncated)
    }
    pub fn tail(&self) -> Result<LogAddress, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("页池锁中毒"))?;
        let Some(id) = state.next_id.checked_sub(1) else {
            return Ok(LogAddress(0));
        };
        let entry = state
            .entries
            .iter()
            .find(|entry| entry.id.0 == id)
            .ok_or(Error::InvalidState("最新逻辑页不存在"))?;
        // 满页的尾部是下一页起点，不能使用要求页内偏移小于页长的转换。
        LogAddress::from_page_offset(PageId(id), 0, self.bytes as u64)?
            .checked_add(entry.next as u64)
    }
    pub fn reserve(&self, len: usize, alignment: usize) -> Result<PageRange, Error> {
        if len == 0 || len > self.bytes || !alignment.is_power_of_two() || alignment > self.bytes {
            return Err(Error::CapacityExceeded);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("页池锁中毒"))?;
        let current_id = state.next_id.checked_sub(1);
        for entry in &mut state.entries {
            // 日志只在最新逻辑页追加，不能回填旧页空隙导致地址倒退。
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
                    entry.next = start + len;
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
        let next_id = state
            .next_id
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        let page = Arc::new(Allocation::new(self.bytes)?);
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
        Ok(PageRange {
            page,
            id,
            generation,
            offset: 0,
            len,
        })
    }
    /// 只能在逻辑退役与外部保留约束满足后调用；尚有范围所有者时拒绝。
    pub fn release(&self, id: PageId, generation: Generation) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("页池锁中毒"))?;
        let entry = state
            .entries
            .iter_mut()
            .find(|e| e.id == id && e.generation == generation && e.page.is_some())
            .ok_or(Error::RangeTruncated)?;
        if Arc::strong_count(entry.page.as_ref().expect("已检查页存在")) != 1 {
            return Err(Error::Busy);
        }
        entry.page = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 跨页后小记录也不能回填旧页且释放尾页不倒退() {
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
    fn 范围对齐不重叠且页预算不会静默扩展() {
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
    fn 所有范围退出后才释放且复用代次递增() {
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
    fn 独占范围可跨线程移动且池销毁不影响存活范围() {
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
    fn 无效长度对齐溢出与遗忘范围均安全拒绝() {
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
