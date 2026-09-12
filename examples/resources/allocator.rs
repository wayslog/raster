//! Allocation count for acceptance binary only;Do not replace RasterKV Global allocator for library consumers.
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
    /// Multifield is not an atomic snapshot;Acceptance only occurs when the business thread has exited,Sample after object has been destroyed.
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
// SAFETY: Complete commission System pointers and layout contracts;count only uses no allocation,Atomic operations without panic.
unsafe impl GlobalAlloc for TrackingAllocator<'_> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller provides GlobalAlloc a valid non-zero layout required,Pass as is.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            self.counters.allocated(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Layout and alloc Same;System Guaranteed that the bytes returned successfully have been cleared.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            self.counters.allocated(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller guarantees that the pointer is still alive and that the layout is consistent with the allocation.,The pairing has not been modified.
        unsafe { System.dealloc(pointer, layout) };
        self.counters.bytes.fetch_sub(layout.size(), Relaxed);
        self.counters.allocations.fetch_sub(1, Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The caller guarantees the original pointer/The layout is valid and the new size is legal,Delegate as is System.
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
