//! In-memory hybrid log (FASTER `persistent_memory_malloc.h`).
//!
//! Pages live in a power-of-two ring of frames allocated up front, so
//! `get(address)` is one shift, mask and add. The tail is a packed
//! `page:23 | offset:41` word advanced by bounded reservations. Boundary shifts run as
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
    /// Another caller is opening `page + 1`.
    Retry(u64),
}

#[repr(align(128))]
struct Padded<T>(T);

/// An address-ordered log with epoch-protected page storage.
/// Raw recovery writes and boundary resets are internal operations.
///
/// ```compile_fail
/// use raster::core::hlog::HybridLog;
/// fn overwrite(log: &mut HybridLog) {
///     log.load_page(0, &[0; 64]);
/// }
/// ```
///
/// ```compile_fail
/// use raster::core::{address::Address, hlog::HybridLog};
/// fn reset(log: &mut HybridLog) {
///     let start = Address::new(0, 64);
///     log.recovery_reset(start, start, start);
/// }
/// ```
pub struct HybridLog {
    storage: Arc<HybridLogStorage>,
    epoch: Arc<LightEpoch>,
    device: Arc<dyn Device>,
}

/// Stable storage shared by the log handle and its deferred work.
///
/// This type is exposed only to preserve access to the log boundaries through
/// `HybridLog`. It owns neither the epoch nor the device, so their queued work
/// can keep frames alive without creating an ownership cycle.
#[doc(hidden)]
pub struct HybridLogStorage {
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
    page_bits: u32,
    /// Completed flush ranges (start -> end control words) not yet merged
    /// into `flushed_until`. Flushes are rare, so a lock is fine here.
    completed_flushes: std::sync::Mutex<std::collections::BTreeMap<u64, u64>>,
    /// Orders flush issue with range assignment, so a later range sharing a
    /// sector is always written after the earlier range's padded tail.
    flush_order: std::sync::Mutex<()>,
    /// Serializes clearing frames and publishing safe_head.
    head_order: std::sync::Mutex<()>,
    pending_flushes: AtomicU64,
    /// Set when a flush write fails; the failed range never becomes flushed.
    flush_failed: std::sync::atomic::AtomicBool,
    /// Recovery may overwrite a fresh log only once, including partial loads.
    recovery_written: bool,
}

// SAFETY: frames are raw page memory shared under epoch protection; all
// mutation of page bytes follows the record publication protocol.
unsafe impl Send for HybridLogStorage {}
// SAFETY: see above.
unsafe impl Sync for HybridLogStorage {}

impl std::ops::Deref for HybridLog {
    type Target = HybridLogStorage;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.storage
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
        // Opening page T+1 needs the head at T+2-frames, and the head never
        // passes the read-only boundary at T-mutable_pages, so at least two
        // frames must stay immutable.
        let mutable_pages =
            ((frames_len as f64 * config.mutable_fraction) as u64).clamp(1, frames_len - 2);
        let start = Address::new(0, FIRST_VALID_OFFSET);
        Self {
            storage: Arc::new(HybridLogStorage {
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
                page_bits: config.page_bits,
                completed_flushes: Default::default(),
                flush_order: Default::default(),
                head_order: Default::default(),
                pending_flushes: AtomicU64::new(0),
                flush_failed: Default::default(),
                recovery_written: false,
            }),
            epoch,
            device,
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
    /// Computing the pointer is safe for any address. Dereferencing it requires
    /// an in-page offset and epoch protection after checking `address >= head`;
    /// the frame then stays valid until the next refresh.
    #[inline(always)]
    pub fn get(&self, address: Address) -> *mut u8 {
        let frame = self.frames[(address.page() & self.frame_mask) as usize];
        frame.wrapping_add(address.offset() as usize)
    }

    /// Hints that the record at `address` is about to be updated, so its cache
    /// line is fetched once in exclusive state instead of shared then upgraded.
    /// Prefetches never fault, so any address is acceptable. A compiler fence
    /// keeps record loads after the hint without declaring that the hint
    /// modifies unrelated context memory.
    #[inline(always)]
    pub fn prefetch_for_write(&self, address: Address) {
        let ptr = self.get(address);
        #[cfg(target_arch = "x86_64")]
        // SAFETY: PREFETCHW only hints the cache and never faults; it decodes
        // as a NOP on CPUs without PRFCHW.
        unsafe {
            std::arch::asm!("prefetchw [{0}]", in(reg) ptr, options(readonly, nostack, preserves_flags));
        }
        #[cfg(target_arch = "aarch64")]
        // SAFETY: PRFM only hints the cache and never faults.
        unsafe {
            std::arch::asm!("prfm pstl1keep, [{0}]", in(reg) ptr, options(readonly, nostack, preserves_flags));
        }
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = ptr;
    }

    #[inline]
    /// Current tail. A full tail page reads as the start of the next page, so
    /// every position has a single representation.
    pub fn tail(&self) -> Address {
        let word = self.tail.0.load(Ordering::Acquire);
        let page = word >> TAIL_OFFSET_BITS;
        let offset = word & ((1 << TAIL_OFFSET_BITS) - 1);
        if offset >= self.page_size {
            Address::new(page + 1, 0)
        } else {
            Address::new(page, offset)
        }
    }

    /// Reserves `size` bytes (a multiple of 8) at the tail.
    #[inline]
    pub fn allocate(&self, size: u64) -> Allocation {
        assert!(
            size.is_multiple_of(8) && size <= self.page_size,
            "record exceeds log page size or is unaligned"
        );
        let mut word = self.tail.0.load(Ordering::Acquire);
        loop {
            let page = word >> TAIL_OFFSET_BITS;
            let offset = word & ((1 << TAIL_OFFSET_BITS) - 1);
            if offset > self.page_size {
                return Allocation::Retry(page);
            }
            // Only the boundary-crossing reservation may exceed page_size.
            // A failed frame wait never increments the packed offset again.
            let end = offset + size;
            match self.tail.0.compare_exchange_weak(
                word,
                word + size,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) if end <= self.page_size => {
                    return Allocation::Ok(Address::new(page, offset));
                }
                Ok(_) => return Allocation::PageFull(page),
                Err(observed) => word = observed,
            }
        }
    }

    /// Reserves for the engine's bounded set of epoch participants.
    ///
    /// # Safety
    /// The caller must own a slot from this log's epoch and make at most one
    /// reservation at a time with that slot, keeping it reserved until this
    /// method returns. Rejected reservations are cancelled before returning,
    /// so unwinding or slot reuse cannot accumulate offsets on a closed page.
    /// Public callers without bounded slots must use the CAS in `allocate`.
    #[inline]
    pub(crate) unsafe fn reserve_for_session(&self, size: u64) -> Allocation {
        const {
            // Each of the 128 epoch slots owns at most one outstanding reservation.
            // Include the initial page and one boundary-crossing public CAS:
            // (128 + 2) * 32 MiB = 4,362,076,160 bytes, below the 41-bit offset.
            assert!(
                ((super::epoch::MAX_SLOTS as u64 + 2) << Address::OFFSET_BITS)
                    < (1 << TAIL_OFFSET_BITS)
            );
        }
        assert!(
            size > 0 && size.is_multiple_of(8) && size <= self.page_size,
            "record exceeds log page size or is unaligned"
        );
        let word = self.tail.0.fetch_add(size, Ordering::AcqRel);
        let page = word >> TAIL_OFFSET_BITS;
        let offset = word & ((1 << TAIL_OFFSET_BITS) - 1);
        if offset + size <= self.page_size {
            Allocation::Ok(Address::new(page, offset))
        } else if offset <= self.page_size {
            Allocation::PageFull(page)
        } else {
            self.cancel_rejected_reservation(page, size);
            Allocation::Retry(page)
        }
    }

    #[cold]
    #[inline(never)]
    fn cancel_rejected_reservation(&self, page: u64, size: u64) {
        let mut current = self.tail.0.load(Ordering::Acquire);
        while current >> TAIL_OFFSET_BITS == page {
            // Keep the first boundary-crossing reservation as a closed-page
            // marker. Only this rejected reservation is removed. If another
            // caller opens a new page, its offset must never be decremented.
            match self.tail.0.compare_exchange_weak(
                current,
                current - size,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    /// Opens `old_page + 1` once its frame is free. Returns false if the
    /// caller must refresh its epoch and try again.
    pub fn new_page(&self, old_page: u64) -> bool {
        self.new_page_with(old_page, |_, _| {})
    }

    /// [`HybridLog::new_page`] that calls `before_evict(from, to)` before the
    /// head moves from `from` to `to`, so the read cache can unlink records
    /// in the closing pages first. Idempotent for a page already opened.
    pub fn new_page_with(&self, old_page: u64, before_evict: impl Fn(Address, Address)) -> bool {
        let new_page = old_page + 1;
        assert!(
            new_page <= Address::MAX_PAGE,
            "hybrid log address space exhausted"
        );
        if new_page >= self.safe_head.load().page() + self.frames.len() as u64 {
            self.shift_boundaries(old_page, &before_evict);
            return false;
        }
        // A concurrent reservation or page opener may win the CAS; retry
        // until this page is opened or the tail has already moved past it.
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
                    self.shift_boundaries(new_page, &before_evict);
                    break;
                }
                Err(observed) => current = observed,
            }
        }
        true
    }

    fn shift_boundaries(&self, tail_page: u64, before_evict: &impl Fn(Address, Address)) {
        self.shift_read_only_to(Address::new(
            tail_page.saturating_sub(self.mutable_pages),
            0,
        ));
        let frames = self.frames.len() as u64;
        let desired = Address::new((tail_page + 2).saturating_sub(frames), 0);
        let head = desired.min(self.flushed_until.load());
        let current = self.head.load();
        if head > current {
            before_evict(current, head);
        }
        if self.head.monotonic_update(head) {
            let log = self.storage.clone();
            self.epoch.bump_with_action(move || {
                // Clear the frames before publishing safe_head: `new_page`
                // reuses a frame as soon as safe_head passes its old page.
                let _order = log.head_order.lock().unwrap();
                let previous = log.safe_head.load();
                for page in previous.page()..head.page() {
                    let frame = log.frames[(page & log.frame_mask) as usize];
                    // SAFETY: every session has moved past the epoch in which
                    // `head` passed this page, so no reader holds the frame,
                    // and safe_head has not yet allowed its reuse.
                    unsafe { frame.write_bytes(0, log.page_size as usize) };
                }
                log.safe_head.monotonic_update(head);
            });
        }
    }
}

impl HybridLog {
    /// Number of in-memory page frames.
    pub fn frames(&self) -> u64 {
        self.frames.len() as u64
    }

    /// Whether a fresh log can be restored without readers, queued actions or
    /// I/O retaining its storage. The owning store is exclusively borrowed.
    pub(crate) fn recovery_ready(&mut self) -> bool {
        let start = Address::new(0, FIRST_VALID_OFFSET);
        if self.tail() != start
            || [
                &self.begin,
                &self.head,
                &self.safe_head,
                &self.read_only,
                &self.safe_read_only,
                &self.flushed_until,
            ]
            .iter()
            .any(|boundary| boundary.load() != start)
        {
            return false;
        }
        Arc::get_mut(&mut self.storage).is_some_and(|storage| {
            !storage.recovery_written && !storage.flush_failed.load(Ordering::Acquire)
        })
    }

    /// Whether the address uses this log's page geometry and no cache tag.
    pub(crate) fn valid_address(&self, address: Address) -> bool {
        address.page() <= Address::MAX_PAGE && address.offset() < self.page_size
    }

    /// Copies recovered bytes into exclusively owned page storage. Only the
    /// store's exclusive recovery path may overwrite initialized log records.
    pub(crate) fn load_page(&mut self, page: u64, bytes: &[u8]) {
        assert!(bytes.len() as u64 <= self.page_size);
        assert!(page <= Address::MAX_PAGE);
        let storage =
            Arc::get_mut(&mut self.storage).expect("recovery requires exclusive log storage");
        let frame = storage.frames[(page & storage.frame_mask) as usize];
        storage.recovery_written = true;
        // SAFETY: the store excludes sessions/scanners via &mut self, and the
        // unique Arc excludes deferred actions and I/O that could use frames.
        // The source is initialized, disjoint and bounded by the page size.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), frame, bytes.len()) };
    }

    /// Restores the in-memory window after recovery (FASTER `RecoveryReset`):
    /// everything below `tail` is durable and read-only.
    pub(crate) fn recovery_reset(&mut self, begin: Address, head: Address, tail: Address) {
        assert!(begin <= head && head <= tail);
        assert!(
            [begin, head, tail]
                .into_iter()
                .all(|address| self.valid_address(address))
        );
        let storage =
            Arc::get_mut(&mut self.storage).expect("recovery requires exclusive log storage");
        storage.recovery_written = true;
        for boundary in [&storage.begin, &storage.head, &storage.safe_head] {
            boundary.store(if std::ptr::eq(boundary, &storage.begin) {
                begin
            } else {
                head
            });
        }
        for boundary in [
            &storage.read_only,
            &storage.safe_read_only,
            &storage.flushed_until,
        ] {
            boundary.store(tail);
        }
        storage.tail.0.store(
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

    /// Releases device storage below `begin` (FASTER `Truncate`).
    pub fn truncate(&self, begin: Address, done: Box<dyn FnOnce(u64) + Send>) {
        self.device.truncate(self.device_offset(begin), done);
    }

    /// Byte offset of `address` on the device.
    #[inline(always)]
    pub fn device_offset(&self, address: Address) -> u64 {
        self.storage.device_offset(address)
    }

    /// Reads `len` bytes at `address` from the device.
    pub fn read_async(&self, address: Address, len: usize, done: super::device::ReadCallback) {
        self.device
            .read_async(self.device_offset(address), len, done);
    }

    /// Makes everything below `address` read-only. Once every session has
    /// observed the shift, the newly read-only range is flushed.
    ///
    /// # Panics
    /// Before changing state, if the offset is outside the configured page,
    /// the address carries a cache tag, or it is beyond the current tail.
    /// A page-end boundary must use offset zero on the following page.
    pub fn shift_read_only_to(&self, address: Address) {
        assert!(
            self.valid_address(address) && address <= self.tail(),
            "read-only boundary must be within the configured log page and tail"
        );
        if self.read_only.monotonic_update(address) {
            let log = self.storage.clone();
            let device = self.device.clone();
            self.epoch.bump_with_action(move || {
                let _order = log.flush_order.lock().unwrap();
                let previous = log.safe_read_only.fetch_max(address);
                if previous < address {
                    log.flush_range(device.as_ref(), previous, address);
                }
            });
        }
    }
}

impl HybridLogStorage {
    #[inline(always)]
    fn device_offset(&self, address: Address) -> u64 {
        (address.page() << self.page_bits) | address.offset()
    }

    /// Flushes `[from, to)`, which is read-only and therefore stable, split at
    /// page boundaries. A trailing partial sector is copied so that bytes past
    /// `to`, which may still be written, are never read.
    fn flush_range(self: &Arc<Self>, device: &dyn Device, from: Address, to: Address) {
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
                    device,
                    start..range_end,
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
                    device,
                    range_start..end,
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
        self: &Arc<Self>,
        device: &dyn Device,
        range: std::ops::Range<Address>,
        offset: u64,
        data: *const u8,
        len: usize,
        buffer: Option<AlignedBuffer>,
    ) {
        self.pending_flushes.fetch_add(1, Ordering::AcqRel);
        let log = self.clone();
        // A zero-padded partial sector must land before any later write of the
        // same sector, so wait for it; full-sector writes never overlap.
        let wait = buffer.is_some();
        let (sent, finished) = std::sync::mpsc::channel();
        let done = Box::new(move |result: std::io::Result<()>| {
            drop(buffer);
            match result {
                Ok(()) => log.complete_flush(range.start, range.end),
                // Leave the range unflushed; waiters observe `flush_failed`.
                Err(_) => log.flush_failed.store(true, Ordering::Release),
            }
            log.pending_flushes.fetch_sub(1, Ordering::AcqRel);
            let _ = sent.send(());
        });
        // SAFETY: `data` is either stable frame memory (see `flush_range`) or
        // `buffer`. The callback owns the storage and buffer until completion.
        unsafe { device.write_async(offset, data, len, done) };
        if wait {
            let _ = finished.recv();
        }
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

impl HybridLog {
    /// Whether a flush write has failed. `flushed_until` then stops below the
    /// failed range, so checkpoints report an I/O error and allocations that
    /// need the head to pass it cannot proceed.
    pub fn flush_failed(&self) -> bool {
        self.flush_failed.load(Ordering::Acquire)
    }
}

impl Drop for HybridLog {
    fn drop(&mut self) {
        while self.pending_flushes.load(Ordering::Acquire) != 0 {
            std::thread::yield_now();
        }
    }
}

impl Drop for HybridLogStorage {
    fn drop(&mut self) {
        let layout = HybridLog::frame_layout(self.page_size);
        for &frame in self.frames.iter() {
            // SAFETY: allocated in `new` with the same layout; all deferred
            // actions and I/O callbacks have released their storage owners.
            unsafe { dealloc(frame, layout) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_and_full_mutable_fractions_allow_bounded_rollover() {
        for frames in [4, 8, 16] {
            for fraction in [LogConfig::default().mutable_fraction, 1.0] {
                let epoch = Arc::new(LightEpoch::new());
                let log = HybridLog::new(
                    LogConfig {
                        page_bits: 12,
                        memory_pages: frames,
                        mutable_fraction: fraction,
                    },
                    epoch.clone(),
                    Arc::new(crate::core::device::NullDevice),
                );
                let slot = epoch.reserve().unwrap();
                epoch.protect(&slot);
                for _ in 0..frames * 4 {
                    let mut allocated = false;
                    for _ in 0..100 {
                        match log.allocate(log.page_size()) {
                            Allocation::Ok(_) => {
                                allocated = true;
                                break;
                            }
                            Allocation::PageFull(page) | Allocation::Retry(page) => {
                                log.new_page(page);
                                epoch.protect_and_drain(&slot);
                            }
                        }
                    }
                    assert!(
                        allocated,
                        "rollover stalled: {frames} frames, {fraction} mutable"
                    );
                }
                assert!(log.safe_head.load().page() > frames);
                epoch.release(slot);
            }
        }
    }

    #[test]
    fn deferred_work_does_not_keep_the_epoch_alive() {
        let epoch = Arc::new(LightEpoch::new());
        let log = HybridLog::new(
            LogConfig {
                page_bits: 12,
                memory_pages: 4,
                mutable_fraction: 0.5,
            },
            epoch.clone(),
            Arc::new(crate::core::device::NullDevice),
        );
        let slot = epoch.reserve().unwrap();
        epoch.protect(&slot);
        let storage = Arc::downgrade(&log.storage);
        let _ = log.allocate(448);
        log.shift_read_only_to(log.tail());
        drop(log);
        assert!(storage.upgrade().is_some(), "queued work owns its frames");
        let observer = Arc::downgrade(&epoch);
        drop(epoch);
        assert!(observer.upgrade().is_none(), "no epoch ownership cycle");
        assert!(
            storage.upgrade().is_none(),
            "discarded work releases its frames"
        );
    }

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
    fn recovery_mutation_rejects_deferred_and_inflight_flushes() {
        use crate::core::device::{ReadCallback, WriteCallback};
        use std::sync::Mutex;

        #[derive(Default)]
        struct DeferredWrite(Mutex<Option<WriteCallback>>);

        impl Device for DeferredWrite {
            unsafe fn write_async(&self, _: u64, _: *const u8, _: usize, done: WriteCallback) {
                assert!(self.0.lock().unwrap().replace(done).is_none());
            }

            fn read_async(&self, _: u64, _: usize, _: ReadCallback) {
                panic!("unexpected read");
            }
        }

        let epoch = Arc::new(LightEpoch::new());
        let slot = epoch.reserve().unwrap();
        epoch.protect(&slot);
        let device = Arc::new(DeferredWrite::default());
        let mut log = HybridLog::new(
            LogConfig {
                page_bits: 12,
                memory_pages: 4,
                mutable_fraction: 0.5,
            },
            epoch.clone(),
            device.clone(),
        );
        let begin = log.begin.load();
        let _ = log.allocate(448);
        let tail = log.tail();
        log.shift_read_only_to(tail);
        assert!(device.0.lock().unwrap().is_none());
        for _ in 0..2 {
            let overwritten = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                log.load_page(0, &[7; 512]);
            }));
            assert!(overwritten.is_err());
            let reset = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                log.recovery_reset(begin, begin, begin);
            }));
            assert!(reset.is_err());
            assert_eq!(log.tail(), tail);
            assert_eq!(log.read_only.load(), tail);
            assert_eq!(log.flushed_until.load(), begin);
            // SAFETY: the page is resident and initialized; neither rejected
            // recovery operation may modify it while the flush owns it.
            assert_eq!(unsafe { *log.get(begin) }, 0);
            epoch.protect_and_drain(&slot);
            assert!(device.0.lock().unwrap().is_some());
        }
        device.0.lock().unwrap().take().unwrap()(Ok(()));
        assert_eq!(log.flushed_until.load(), tail);
        epoch.release(slot);
        // Once both the action and callback have released their references,
        // the internal helper can access the exclusively owned storage.
        log.load_page(0, &[7; 512]);
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
                    Allocation::Retry(_) => {
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

    #[test]
    fn blocked_large_allocations_do_not_advance_or_wrap_tail() {
        let (_, log) = small_log();
        for offset in [log.page_size + 8, (1 << TAIL_OFFSET_BITS) - 8] {
            let tail = (3 << TAIL_OFFSET_BITS) | offset;
            log.tail.0.store(tail, Ordering::Release);
            for _ in 0..100 {
                assert!(matches!(log.allocate(log.page_size), Allocation::Retry(3)));
                assert_eq!(
                    log.tail.0.load(Ordering::Acquire),
                    tail,
                    "waiting for a frame must not consume offset bits"
                );
            }
        }
    }

    #[test]
    fn all_session_slots_and_public_cas_keep_the_tail_offset_bounded() {
        use super::super::epoch::MAX_SLOTS;
        let (epoch, log) = small_log();
        log.tail
            .0
            .store((3 << TAIL_OFFSET_BITS) | log.page_size, Ordering::Release);
        let ready = std::sync::Barrier::new(MAX_SLOTS + 1);
        let crossed = std::sync::Barrier::new(MAX_SLOTS + 1);
        std::thread::scope(|scope| {
            for _ in 0..MAX_SLOTS {
                scope.spawn(|| {
                    let slot = epoch.reserve().unwrap();
                    ready.wait();
                    crossed.wait();
                    // SAFETY: each reserved slot performs exactly one call;
                    // no failed reservation is retried on the closed page.
                    let result = unsafe { log.reserve_for_session(log.page_size) };
                    epoch.release(slot);
                    assert!(matches!(result, Allocation::Retry(3)));
                });
            }
            ready.wait();
            assert!(matches!(
                log.allocate(log.page_size),
                Allocation::PageFull(3)
            ));
            crossed.wait();
        });
        let expected = (3 << TAIL_OFFSET_BITS) | (2 * log.page_size);
        assert_eq!(log.tail.0.load(Ordering::Acquire), expected);
        for _ in 0..100 {
            assert!(matches!(log.allocate(log.page_size), Allocation::Retry(3)));
            assert_eq!(log.tail.0.load(Ordering::Acquire), expected);
        }
    }

    #[test]
    fn cancelling_rejected_reservation_preserves_new_page_tail() {
        let (_, log) = small_log();
        let expected = (4 << TAIL_OFFSET_BITS) | 24;
        log.tail.0.store(expected, Ordering::Release);
        log.cancel_rejected_reservation(3, log.page_size);
        assert_eq!(log.tail.0.load(Ordering::Acquire), expected);
    }

    #[test]
    fn released_slots_do_not_accumulate_rejected_reservations() {
        let (epoch, log) = small_log();
        let expected = (3 << TAIL_OFFSET_BITS) | (2 * log.page_size);
        log.tail.0.store(expected, Ordering::Release);
        for _ in 0..1000 {
            let slot = epoch.reserve().unwrap();
            // SAFETY: this slot owns only this reservation. Rejected internal
            // reservations must be cancelled before control leaves the method.
            let result = unsafe { log.reserve_for_session(log.page_size) };
            epoch.release(slot);
            assert!(matches!(result, Allocation::Retry(3)));
            assert_eq!(log.tail.0.load(Ordering::Acquire), expected);
        }
    }

    #[test]
    fn panicking_page_wait_cancels_reservation_before_slot_reuse() {
        use super::super::epoch::MAX_SLOTS;
        use super::super::store::{FasterKv, StoreConfig};

        let store = FasterKv::<u64, u64>::with_device(
            StoreConfig {
                index_buckets: 16,
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 4,
                    mutable_fraction: 0.5,
                },
                ..Default::default()
            },
            Arc::new(crate::core::device::NullDevice),
        );
        let occupied: Vec<_> = (0..MAX_SLOTS - 1)
            .map(|_| store.epoch.reserve_helper().unwrap())
            .collect();
        let slot = store.epoch.reserve().unwrap();
        store.epoch.protect(&slot);
        let page_size = store.hlog.page_size();
        let closed = (3 << TAIL_OFFSET_BITS) | (2 * page_size);
        store.hlog.tail.0.store(closed, Ordering::Release);
        // The old protection keeps this action queued until block_allocate
        // refreshes while waiting for the closed page's frame to become free.
        store
            .epoch
            .bump_with_action(|| panic!("injected page wait failure"));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.block_allocate(&slot, page_size)
        }));
        store.epoch.release(slot);
        assert!(result.is_err());
        assert_eq!(store.hlog.tail.0.load(Ordering::Acquire), closed);

        // Every other slot is still occupied, so this must reuse the slot
        // released after unwinding. No rejected size may survive its reuse.
        let reused = store.epoch.reserve().unwrap();
        // SAFETY: this exclusively owned slot makes only one reservation.
        let allocation = unsafe { store.hlog.reserve_for_session(page_size) };
        store.epoch.release(reused);
        for slot in occupied {
            store.epoch.release(slot);
        }
        assert!(matches!(allocation, Allocation::Retry(3)));
        assert_eq!(store.hlog.tail.0.load(Ordering::Acquire), closed);
    }

    #[test]
    fn blocked_store_reservations_remain_bounded_until_a_new_page_opens() {
        use super::super::store::{FasterKv, StoreConfig};
        use std::time::{Duration, Instant};

        const WRITERS: usize = 4;
        let store = FasterKv::<u64, u64>::with_device(
            StoreConfig {
                index_buckets: 16,
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 4,
                    mutable_fraction: 0.5,
                },
                ..Default::default()
            },
            Arc::new(crate::core::device::NullDevice),
        );
        let pin = store.epoch.reserve().unwrap();
        store.epoch.protect_and_drain(&pin);
        let page_size = store.hlog.page_size();
        for page in 0..4 {
            let tail = store.hlog.tail();
            assert_eq!(tail.page(), page);
            assert!(matches!(
                store.hlog.allocate(page_size - tail.offset()),
                Allocation::Ok(_)
            ));
            if page < 3 {
                assert!(store.hlog.new_page(page));
            }
        }
        let ready = std::sync::Barrier::new(WRITERS + 1);
        let minimum = (3 << TAIL_OFFSET_BITS) | (2 * page_size);
        let maximum = (3 << TAIL_OFFSET_BITS) | ((WRITERS as u64 + 1) * page_size);
        let (observed, public_retries, pages) = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..WRITERS {
                workers.push(scope.spawn(|| {
                    let slot = store.epoch.reserve().unwrap();
                    store.epoch.protect_and_drain(&slot);
                    ready.wait();
                    let address = store.block_allocate(&slot, page_size);
                    store.epoch.release(slot);
                    assert_eq!(address.offset(), 0);
                    address.page()
                }));
            }
            ready.wait();
            let deadline = Instant::now() + Duration::from_secs(5);
            while store.hlog.tail.0.load(Ordering::Acquire) < minimum && Instant::now() < deadline {
                std::thread::yield_now();
            }
            // Later arrivals can temporarily reserve before cancelling. The
            // invariant is the participant bound, not a constant closed offset.
            let mut observed = Vec::with_capacity(1002);
            observed.push(store.hlog.tail.0.load(Ordering::Acquire));
            for _ in 0..1000 {
                observed.push(store.hlog.tail.0.load(Ordering::Acquire));
                std::thread::yield_now();
            }
            let mut public_retries = true;
            for _ in 0..100 {
                public_retries &= matches!(store.hlog.allocate(page_size), Allocation::Retry(3));
            }
            observed.push(store.hlog.tail.0.load(Ordering::Acquire));
            // Always release the old reader before checking the verdict, so
            // even a red run can drain the allocation threads and retain output.
            store.epoch.release(pin);
            let pages: std::collections::BTreeSet<_> = workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect();
            (observed, public_retries, pages)
        });
        assert!(public_retries);
        assert!(
            observed
                .iter()
                .all(|word| (minimum..=maximum).contains(word)),
            "blocked reservations exceeded bounds {minimum}..={maximum}: observed {:?}..={:?}",
            observed.iter().min(),
            observed.iter().max()
        );
        assert_eq!(pages.len(), WRITERS);
        assert!(pages.iter().all(|page| *page >= 4));
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
                    Allocation::Retry(_) => {}
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
