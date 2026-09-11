//! 仅用于验收二进制的分配计数；不替换 RasterKV 库使用者的全局分配器。
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

pub(crate) struct Counters {
    bytes: AtomicUsize,
    allocations: AtomicUsize,
    peak: AtomicUsize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub bytes: usize,
    pub allocations: usize,
    pub peak: usize,
}
impl Counters {
    pub const fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
    /// 多字段不是原子快照；验收只在业务线程已退出、对象已销毁后采样。
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            bytes: self.bytes.load(Relaxed),
            allocations: self.allocations.load(Relaxed),
            peak: self.peak.load(Relaxed),
        }
    }
    fn add_bytes(&self, size: usize) {
        let bytes = self.bytes.fetch_add(size, Relaxed).wrapping_add(size);
        self.peak.fetch_max(bytes, Relaxed);
    }
    fn allocated(&self, size: usize) {
        self.allocations.fetch_add(1, Relaxed);
        self.add_bytes(size);
    }
}
pub(crate) struct TrackingAllocator<'a> {
    counters: &'a Counters,
}
impl<'a> TrackingAllocator<'a> {
    pub const fn new(counters: &'a Counters) -> Self {
        Self { counters }
    }
}
// SAFETY: 完整委托 System 的指针和布局契约；计数仅使用不分配、不恐慌的原子操作。
unsafe impl GlobalAlloc for TrackingAllocator<'_> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 调用者提供 GlobalAlloc 要求的有效非零布局，原样传递。
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            self.counters.allocated(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 布局与 alloc 相同；System 保证成功返回的字节已清零。
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            self.counters.allocated(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: 调用者保证指针仍存活且布局与分配一致，未修改该配对。
        unsafe { System.dealloc(pointer, layout) };
        self.counters.bytes.fetch_sub(layout.size(), Relaxed);
        self.counters.allocations.fetch_sub(1, Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: 调用者保证原指针/布局有效且新大小合法，原样委托 System。
        let next = unsafe { System.realloc(pointer, layout, size) };
        if !next.is_null() {
            if size >= layout.size() {
                self.counters.add_bytes(size - layout.size());
            } else {
                self.counters.bytes.fetch_sub(layout.size() - size, Relaxed);
            }
        }
        next
    }
}
