//! In-memory hybrid log (FASTER `persistent_memory_malloc.h`).
//!
//! Pages live in a power-of-two ring of frames allocated up front, so
//! `get(address)` is one shift, mask and add. The tail is a packed
//! `page:23 | offset:41` word advanced with `fetch_add`. Boundary shifts run as
//! epoch actions so that no protected session can still use a closed frame.
use super::address::{Address, AtomicAddress};
use super::device::{AlignedBuffer, Device, SECTOR};
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
    /// Completed flush ranges (start -> end control words) not yet merged
    /// into `flushed_until`. Flushes are rare, so a lock is fine here.
    completed_flushes: std::sync::Mutex<std::collections::BTreeMap<u64, u64>>,
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
            completed_flushes: Default::default(),
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
        self.shift_read_only_to(Address::new(
            tail_page.saturating_sub(self.mutable_pages),
            0,
        ));
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
    /// Number of in-memory page frames.
    pub fn frames(&self) -> u64 {
        self.frames.len() as u64
    }

    /// Copies recovered bytes into the frame of `page`, starting at offset 0.
    /// Only for recovery, before any session exists.
    pub fn load_page(&self, page: u64, bytes: &[u8]) {
        assert!(bytes.len() as u64 <= self.page_size);
        let frame = self.frames[(page & self.frame_mask) as usize];
        // SAFETY: no session exists during recovery, so the frame is unshared.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), frame, bytes.len()) };
    }

    /// Restores the in-memory window after recovery (FASTER `RecoveryReset`):
    /// everything below `tail` is durable and read-only.
    pub fn recovery_reset(&self, begin: Address, head: Address, tail: Address) {
        for boundary in [&self.begin, &self.head, &self.safe_head] {
            boundary.store(if std::ptr::eq(boundary, &self.begin) {
                begin
            } else {
                head
            });
        }
        for boundary in [&self.read_only, &self.safe_read_only, &self.flushed_until] {
            boundary.store(tail);
        }
        self.tail.0.store(
            (tail.page() << TAIL_OFFSET_BITS) | tail.offset(),
            Ordering::Release,
        );
    }

    /// Writes raw bytes to the log device (recovery only).
    ///
    /// # Safety
    /// See [`Device::write_async`].
    pub unsafe fn write_async(
        &self,
        offset: u64,
        data: *const u8,
        len: usize,
        done: super::device::WriteCallback,
    ) {
        // SAFETY: forwarded from the caller.
        unsafe { self.device.write_async(offset, data, len, done) };
    }

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

    /// Makes everything below `address` read-only. Once every session has
    /// observed the shift, the newly read-only range is flushed.
    pub fn shift_read_only_to(&self, address: Address) {
        if self.read_only.monotonic_update(address) {
            let log = LogPtr(self);
            self.epoch.bump_with_action(move || {
                // SAFETY: the store keeps the log alive while actions can run.
                let log = unsafe { log.log() };
                let previous = log.safe_read_only.fetch_max(address);
                if previous < address {
                    log.flush_range(previous, address);
                }
            });
        }
    }

    /// Flushes `[from, to)`, which is read-only and therefore stable, split at
    /// page boundaries. A trailing partial sector is copied so that bytes past
    /// `to`, which may still be written, are never read.
    fn flush_range(&self, from: Address, to: Address) {
        let mut start = from;
        while start < to {
            let page_end = Address::new(start.page() + 1, 0);
            let end = to.min(page_end);
            let end_offset = if end == page_end {
                self.page_size
            } else {
                end.offset()
            };
            let aligned_start = start.offset() & !(SECTOR - 1);
            let aligned_end = end_offset & !(SECTOR - 1);
            let frame = self.frames[(start.page() & self.frame_mask) as usize];
            let page_base = self.device_offset(Address::new(start.page(), 0));
            if aligned_end > aligned_start {
                let range_end = if aligned_end == end_offset {
                    end
                } else {
                    Address::new(start.page(), aligned_end)
                };
                // SAFETY: the bytes are read-only and the frame cannot be
                // reused before `flushed_until` passes this range.
                let data = unsafe { frame.add(aligned_start as usize) };
                self.write_range(
                    start,
                    range_end,
                    page_base + aligned_start,
                    data,
                    (aligned_end - aligned_start) as usize,
                    None,
                );
            }
            if aligned_end < end_offset {
                let tail_start = aligned_end.max(aligned_start);
                let mut copy = AlignedBuffer::new(SECTOR as usize);
                let stable = (end_offset - tail_start) as usize;
                // SAFETY: `[tail_start, end_offset)` is read-only and stable.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        frame.add(tail_start as usize),
                        copy.as_mut().as_mut_ptr(),
                        stable,
                    )
                };
                let range_start = start.max(Address::new(start.page(), tail_start));
                let data = copy.as_ptr();
                self.write_range(
                    range_start,
                    end,
                    page_base + tail_start,
                    data,
                    SECTOR as usize,
                    Some(copy),
                );
            }
            start = end;
        }
    }

    fn write_range(
        &self,
        start: Address,
        end: Address,
        offset: u64,
        data: *const u8,
        len: usize,
        buffer: Option<AlignedBuffer>,
    ) {
        self.pending_flushes.fetch_add(1, Ordering::AcqRel);
        let log = LogPtr(self);
        let done = Box::new(move |result: std::io::Result<()>| {
            drop(buffer);
            // SAFETY: `Drop` waits for all pending flushes.
            let log = unsafe { log.log() };
            result.expect("hybrid log flush failed");
            log.complete_flush(start, end);
            log.pending_flushes.fetch_sub(1, Ordering::AcqRel);
        });
        // SAFETY: `data` is either stable frame memory (see `flush_range`) or
        // `buffer`, which the callback keeps alive until completion.
        unsafe { self.device.write_async(offset, data, len, done) };
    }

    /// Records `[start, end)` as durable and advances `flushed_until` over
    /// every contiguous completed range.
    fn complete_flush(&self, start: Address, end: Address) {
        let mut completed = self.completed_flushes.lock().unwrap();
        completed.insert(start.control(), end.control());
        let mut flushed = self.flushed_until.load();
        while let Some(end) = completed.remove(&flushed.control()) {
            flushed = Address::from_control(end);
        }
        self.flushed_until.monotonic_update(flushed);
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

#[cfg(test)]
mod flush_tests {
    use super::*;
    use crate::core::device::FileDevice;

    #[test]
    fn read_only_shift_to_unaligned_address_flushes_exact_prefix() {
        let dir = std::env::temp_dir().join(format!("raster-hlog-flush-{}", std::process::id()));
        let device = Arc::new(FileDevice::open_with(&dir, 20, 2, true).unwrap());
        let epoch = Arc::new(LightEpoch::new());
        let config = LogConfig {
            page_bits: 12,
            memory_pages: 8,
            mutable_fraction: 0.5,
        };
        let log = HybridLog::new(config, epoch.clone(), device.clone());
        let slot = epoch.reserve().unwrap();
        epoch.protect(&slot);
        // Fill ~2.5 pages with recognizable bytes.
        let mut last = Address::default();
        for i in 0..400u64 {
            let address = loop {
                match log.allocate(24) {
                    Allocation::Ok(address) => break address,
                    Allocation::PageFull(page) => {
                        while !log.new_page(page) {
                            epoch.protect_and_drain(&slot);
                        }
                    }
                    Allocation::Retry => {}
                }
            };
            // SAFETY: freshly allocated, exclusive, in memory.
            unsafe { std::ptr::write_bytes(log.get(address), (i % 251) as u8 + 1, 24) };
            last = address;
        }
        let target = Address::new(last.page(), last.offset() - 24 * 7 + 12);
        assert_ne!(target.offset() % SECTOR, 0);
        log.shift_read_only_to(target);
        let started = std::time::Instant::now();
        while log.flushed_until.load() < target {
            epoch.protect_and_drain(&slot);
            assert!(started.elapsed().as_secs() < 10, "flush did not complete");
        }
        assert_eq!(log.flushed_until.load(), target);
        let begin = Address::new(0, FIRST_VALID_OFFSET);
        let len = (log.device_offset(target) - log.device_offset(begin)) as usize;
        let (tx, rx) = std::sync::mpsc::channel();
        log.read_async(begin, len, Box::new(move |r| tx.send(r).unwrap()));
        let disk = rx.recv().unwrap().unwrap();
        let mut memory = Vec::with_capacity(len);
        let mut at = begin;
        while at < target {
            let page_end = Address::new(at.page() + 1, 0).min(target);
            let n = (log.device_offset(page_end) - log.device_offset(at)) as usize;
            // SAFETY: resident, read-only bytes.
            memory.extend_from_slice(unsafe { std::slice::from_raw_parts(log.get(at), n) });
            at = page_end;
        }
        assert_eq!(disk, memory);
        epoch.release(slot);
        drop(log);
        drop(device);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
