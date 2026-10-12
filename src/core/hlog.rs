//! In-memory hybrid log (FASTER `persistent_memory_malloc.h`).
//!
//! Pages live in a power-of-two ring of frames allocated up front, so
//! `get(address)` is one shift, mask and add. The tail is a packed
//! `page:23 | offset:41` word advanced with `fetch_add`. Boundary shifts run as
//! epoch actions so that no protected session can still use a closed frame.
use super::address::{Address, AtomicAddress};
use super::device::Device;
use super::epoch::LightEpoch;
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const TAIL_OFFSET_BITS: u32 = 41;
/// Offsets below this are never valid record addresses.
pub const FIRST_VALID_OFFSET: u64 = 64;

#[derive(Clone, Copy, Debug)]
pub struct LogConfig {
    /// log2 of the page size; at most `Address::OFFSET_BITS`.
    pub page_bits: u32,
    /// In-memory page frames; rounded up to a power of two, at least 4.
    pub memory_pages: u64,
    /// Fraction of in-memory pages that stay mutable.
    pub mutable_fraction: f64,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            page_bits: Address::OFFSET_BITS,
            memory_pages: 8,
            mutable_fraction: 0.9,
        }
    }
}

pub enum Allocation {
    Ok(Address),
    /// The tail page is full; this caller must open `page + 1`.
    PageFull(u64),
    /// Another caller is opening the next page.
    Retry,
}

#[repr(align(128))]
struct Padded<T>(T);

pub struct HybridLog {
    frames: Box<[*mut u8]>,
    frame_mask: u64,
    page_size: u64,
    mutable_pages: u64,
    tail: Padded<AtomicU64>,
    pub begin: AtomicAddress,
    pub head: AtomicAddress,
    pub safe_head: AtomicAddress,
    pub read_only: AtomicAddress,
    pub safe_read_only: AtomicAddress,
    pub flushed_until: AtomicAddress,
    epoch: Arc<LightEpoch>,
    device: Arc<dyn Device>,
    page_bits: u32,
    /// Per frame: one past the last page number whose flush completed.
    frame_flushed: Box<[AtomicU64]>,
    pending_flushes: AtomicU64,
}

// SAFETY: frames are raw page memory shared under epoch protection; all
// mutation of page bytes follows the record publication protocol.
unsafe impl Send for HybridLog {}
// SAFETY: see above.
unsafe impl Sync for HybridLog {}

struct LogPtr(*const HybridLog);
// SAFETY: epoch actions only run while the store, which owns the log, is alive.
unsafe impl Send for LogPtr {}
impl LogPtr {
    /// # Safety
    /// The log must still be alive.
    unsafe fn log(&self) -> &HybridLog {
        // SAFETY: guaranteed by the caller.
        unsafe { &*self.0 }
    }
}

impl HybridLog {
    pub fn new(config: LogConfig, epoch: Arc<LightEpoch>, device: Arc<dyn Device>) -> Self {
        assert!((12..=Address::OFFSET_BITS).contains(&config.page_bits));
        let frames_len = config.memory_pages.max(4).next_power_of_two();
        let page_size = 1u64 << config.page_bits;
        let layout = Self::frame_layout(page_size);
        let frames = (0..frames_len)
            .map(|_| {
                // SAFETY: non-zero size, valid alignment.
                let frame = unsafe { alloc_zeroed(layout) };
                assert!(!frame.is_null(), "log page allocation failed");
                frame
            })
            .collect();
        let mutable_pages =
            ((frames_len as f64 * config.mutable_fraction) as u64).clamp(1, frames_len - 1);
        let start = Address::new(0, FIRST_VALID_OFFSET);
        Self {
            frames,
            frame_mask: frames_len - 1,
            page_size,
            mutable_pages,
            tail: Padded(AtomicU64::new(FIRST_VALID_OFFSET)),
            begin: AtomicAddress::new(start),
            head: AtomicAddress::new(start),
            safe_head: AtomicAddress::new(start),
            read_only: AtomicAddress::new(start),
            safe_read_only: AtomicAddress::new(start),
            flushed_until: AtomicAddress::new(start),
            epoch,
            device,
            page_bits: config.page_bits,
            frame_flushed: (0..frames_len).map(|_| AtomicU64::new(0)).collect(),
            pending_flushes: AtomicU64::new(0),
        }
    }

    fn frame_layout(page_size: u64) -> Layout {
        Layout::from_size_align(page_size as usize, 4096).expect("page layout")
    }

    #[inline(always)]
    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    /// Pointer to the bytes at `address`.
    ///
    /// The caller must be epoch-protected and have checked `address >= head`
    /// after protecting; the frame then stays valid until its next refresh.
    #[inline(always)]
    pub fn get(&self, address: Address) -> *mut u8 {
        let frame = self.frames[(address.page() & self.frame_mask) as usize];
        // SAFETY: offset < page_size for every allocated address.
        unsafe { frame.add(address.offset() as usize) }
    }

    /// Hints that the record at `address` is about to be updated, so its cache
    /// line is fetched once in exclusive state instead of shared then upgraded.
    /// Prefetches never fault, so any address is acceptable.
    #[inline(always)]
    pub fn prefetch_for_write(&self, address: Address) {
        let ptr = self.get(address);
        #[cfg(target_arch = "x86_64")]
        // SAFETY: PREFETCHW only hints the cache and never faults; it decodes
        // as a NOP on CPUs without PRFCHW.
        unsafe {
            std::arch::asm!("prefetchw [{0}]", in(reg) ptr, options(nostack, preserves_flags, readonly));
        }
        #[cfg(target_arch = "aarch64")]
        // SAFETY: PRFM only hints the cache and never faults.
        unsafe {
            std::arch::asm!("prfm pstl1keep, [{0}]", in(reg) ptr, options(nostack, preserves_flags, readonly));
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = ptr;
    }

    #[inline]
    pub fn tail(&self) -> Address {
        let word = self.tail.0.load(Ordering::Acquire);
        let page = word >> TAIL_OFFSET_BITS;
        let offset = (word & ((1 << TAIL_OFFSET_BITS) - 1)).min(self.page_size);
        Address::new(page, offset.min(Address::MAX_OFFSET))
    }

    /// Reserves `size` bytes (a multiple of 8) at the tail.
    #[inline]
    pub fn allocate(&self, size: u64) -> Allocation {
        debug_assert!(size.is_multiple_of(8) && size <= self.page_size);
        let word = self.tail.0.fetch_add(size, Ordering::AcqRel);
        let page = word >> TAIL_OFFSET_BITS;
        let offset = word & ((1 << TAIL_OFFSET_BITS) - 1);
        let end = offset + size;
        if end <= self.page_size {
            Allocation::Ok(Address::new(page, offset))
        } else if offset <= self.page_size {
            // This caller crossed the boundary and owns opening the next page.
            Allocation::PageFull(page)
        } else {
            Allocation::Retry
        }
    }

    /// Opens `old_page + 1` once its frame is free. Returns false if the
    /// caller must refresh its epoch and try again.
    pub fn new_page(&self, old_page: u64) -> bool {
        let new_page = old_page + 1;
        assert!(
            new_page <= Address::MAX_PAGE,
            "hybrid log address space exhausted"
        );
        if new_page >= self.safe_head.load().page() + self.frames.len() as u64 {
            self.shift_boundaries(old_page);
            return false;
        }
        // Other callers keep bumping the offset of the full page, so retry the
        // CAS until the tail leaves `old_page`.
        let next = new_page << TAIL_OFFSET_BITS;
        let mut current = self.tail.0.load(Ordering::Acquire);
        while current >> TAIL_OFFSET_BITS == old_page {
            match self.tail.0.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.shift_boundaries(new_page);
                    break;
                }
                Err(observed) => current = observed,
            }
        }
        true
    }

    fn shift_boundaries(&self, tail_page: u64) {
        let read_only = Address::new(tail_page.saturating_sub(self.mutable_pages), 0);
        if self.read_only.monotonic_update(read_only) {
            let log = LogPtr(self);
            self.epoch.bump_with_action(move || {
                // SAFETY: the store keeps the log alive while actions can run.
                let log = unsafe { log.log() };
                let previous = log.safe_read_only.fetch_max(read_only);
                for page in previous.page()..read_only.page() {
                    log.flush_page(page);
                }
            });
        }
        let frames = self.frames.len() as u64;
        let desired = Address::new((tail_page + 2).saturating_sub(frames), 0);
        let head = desired.min(self.flushed_until.load());
        if self.head.monotonic_update(head) {
            let log = LogPtr(self);
            self.epoch.bump_with_action(move || {
                // SAFETY: as above.
                let log = unsafe { log.log() };
                let previous = log.safe_head.fetch_max(head);
                for page in previous.page()..head.page() {
                    let frame = log.frames[(page & log.frame_mask) as usize];
                    // SAFETY: every session has moved past the epoch in which
                    // `head` passed this page, so no reader holds the frame.
                    unsafe { frame.write_bytes(0, log.page_size as usize) };
                }
            });
        }
    }
}

impl HybridLog {
    /// Byte offset of `address` on the device.
    #[inline(always)]
    pub fn device_offset(&self, address: Address) -> u64 {
        (address.page() << self.page_bits) | address.offset()
    }

    /// Reads `len` bytes at `address` from the device.
    pub fn read_async(&self, address: Address, len: usize, done: super::device::ReadCallback) {
        self.device
            .read_async(self.device_offset(address), len, done);
    }

    /// Writes a whole read-only page; its frame cannot be reused before the
    /// write completes because `head` never passes `flushed_until`.
    fn flush_page(&self, page: u64) {
        self.pending_flushes.fetch_add(1, Ordering::AcqRel);
        let frame = self.frames[(page & self.frame_mask) as usize];
        let log = LogPtr(self);
        let done = Box::new(move |result: std::io::Result<()>| {
            // SAFETY: `Drop` waits for all pending flushes.
            let log = unsafe { log.log() };
            result.expect("hybrid log page flush failed");
            log.frame_flushed[(page & log.frame_mask) as usize].store(page + 1, Ordering::Release);
            log.advance_flushed();
            log.pending_flushes.fetch_sub(1, Ordering::AcqRel);
        });
        // SAFETY: the frame stays allocated and unmodified until the flush
        // completes: it is read-only and below `head` only after `flushed_until`.
        unsafe {
            self.device.write_async(
                self.device_offset(Address::new(page, 0)),
                frame,
                self.page_size as usize,
                done,
            )
        };
    }

    /// Moves `flushed_until` over every contiguous completed page.
    fn advance_flushed(&self) {
        loop {
            let current = self.flushed_until.load();
            let page = current.page();
            let done =
                self.frame_flushed[(page & self.frame_mask) as usize].load(Ordering::Acquire);
            if done != page + 1 || page + 1 > self.safe_read_only.load().page() {
                return;
            }
            self.flushed_until
                .monotonic_update(Address::new(page + 1, 0));
        }
    }
}

impl Drop for HybridLog {
    fn drop(&mut self) {
        while self.pending_flushes.load(Ordering::Acquire) != 0 {
            std::thread::yield_now();
        }
        let layout = Self::frame_layout(self.page_size);
        for &frame in self.frames.iter() {
            // SAFETY: allocated in `new` with the same layout.
            unsafe { dealloc(frame, layout) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_log() -> (Arc<LightEpoch>, HybridLog) {
        let epoch = Arc::new(LightEpoch::new());
        let config = LogConfig {
            page_bits: 12,
            memory_pages: 4,
            mutable_fraction: 0.5,
        };
        let log = HybridLog::new(
            config,
            epoch.clone(),
            Arc::new(crate::core::device::NullDevice),
        );
        (epoch, log)
    }

    #[test]
    fn allocations_fill_pages_and_advance() {
        let (epoch, log) = small_log();
        let slot = epoch.reserve().unwrap();
        epoch.protect(&slot);
        let mut last = Address::default();
        for _ in 0..2000 {
            loop {
                match log.allocate(64) {
                    Allocation::Ok(address) => {
                        assert!(address > last && address.offset() + 64 <= 4096);
                        last = address;
                        break;
                    }
                    Allocation::PageFull(page) => {
                        while !log.new_page(page) {
                            epoch.protect_and_drain(&slot);
                        }
                    }
                    Allocation::Retry => {
                        epoch.protect_and_drain(&slot);
                    }
                }
            }
        }
        epoch.protect_and_drain(&slot);
        assert!(last.page() > 20);
        assert!(log.head.load().page() + 4 > last.page());
        assert!(log.read_only.load() <= log.tail());
        epoch.release(slot);
    }
}
