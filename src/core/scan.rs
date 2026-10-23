//! Log scans (FASTER `log_scan.h`).
//!
//! A scan copies one page range at a time into an owned buffer: in-memory
//! pages are copied under epoch protection, older pages are read from the
//! device. Records in a copied page can be inspected without holding the
//! epoch, and several threads can share a scanner by taking pages in turn.
//! Scans cover `[begin, until)` with `until` capped at the safe read-only
//! address, so every scanned record is immutable apart from its header bits.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{Key, Value};
use super::record::{Layout, Record, RecordInfo};
use super::store::{FasterKv, Status};
use std::sync::mpsc;

pub struct LogScanner<'a, K: Key, V: Value> {
    store: &'a FasterKv<K, V>,
    slot: Option<SlotId>,
    next: Address,
    until: Address,
}

/// One page's worth of scanned records, copied out of the log.
pub struct ScanPage<K, V> {
    words: Vec<u64>,
    /// Address of `words[0]`.
    base: Address,
    error: Option<Status>,
    _types: std::marker::PhantomData<(K, V)>,
}

/// A record inside a [`ScanPage`].
///
/// Only typed access is exposed: raw words could overlap an atomic value and
/// allow a non-atomic read while another thread updates that value.
///
/// ```compile_fail
/// use raster::core::scan::ScannedRecord;
/// use std::sync::atomic::AtomicU64;
/// fn raw_value(record: &ScannedRecord<'_, u64, AtomicU64>) -> u64 {
///     record.words[2]
/// }
/// ```
pub struct ScannedRecord<'p, K, V> {
    pub address: Address,
    pub info: RecordInfo,
    pub key: &'p K,
    /// `None` for tombstones, whose initialized value only preserves record size.
    pub value: Option<&'p V>,
    raw: *const u64,
    size: usize,
}

// SAFETY: safe access is limited to shared references to K and V, whose Sync
// bounds permit moving those references across threads. The page remains
// borrowed for 'p and raw access has a separate unsafe contract.
unsafe impl<K: Sync, V: Sync> Send for ScannedRecord<'_, K, V> {}
// SAFETY: as above; sharing the view only shares its typed references.
unsafe impl<K: Sync, V: Sync> Sync for ScannedRecord<'_, K, V> {}

impl<K, V> ScannedRecord<'_, K, V> {
    /// Total encoded size in bytes, including the record header.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Raw bytes for internal compaction of a privately owned scan page.
    ///
    /// # Safety
    /// No key or value may be mutated, including through atomics or other
    /// interior mutability, for the lifetime of the returned slice.
    pub(crate) unsafe fn raw_words(&self) -> &[u64] {
        // SAFETY: the iterator validated this aligned range and its page stays
        // borrowed; the caller excludes mutation through the typed references.
        unsafe { std::slice::from_raw_parts(self.raw, self.size / 8) }
    }
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Scans records in `[begin, until)`, clamped to the valid, immutable log.
    pub fn scan(&self, begin: Address, until: Address) -> LogScanner<'_, K, V> {
        let slot = self.epoch.reserve_helper();
        let begin = begin.max(self.hlog.begin.load());
        let until = until.min(self.hlog.safe_read_only.load());
        LogScanner {
            store: self,
            slot,
            next: begin,
            until,
        }
    }
}

impl<K: Key, V: Value> LogScanner<'_, K, V> {
    /// Internal scans borrow their active session's protection for each page.
    pub(crate) fn for_session(
        store: &FasterKv<K, V>,
        begin: Address,
        until: Address,
    ) -> LogScanner<'_, K, V> {
        LogScanner {
            store,
            slot: None,
            next: begin.max(store.hlog.begin.load()),
            until: until.min(store.hlog.safe_read_only.load()),
        }
    }

    /// Selects a claimed page while retaining this scanner's helper slot.
    pub(crate) fn seek(&mut self, start: Address) {
        self.next = start;
    }
    pub fn until(&self) -> Address {
        self.until
    }

    /// Copies the next page range, or returns `None` at the end of the scan.
    /// Returns OutOfMemory if no helper epoch slot is available; callers may retry.
    pub fn next_page(&mut self) -> Result<Option<ScanPage<K, V>>, Status> {
        if self.next >= self.until {
            return Ok(None);
        }
        if self.slot.is_none() {
            self.slot = self.store.epoch.reserve_helper();
        }
        self.read_next(None)
    }

    pub(crate) fn next_page_with_slot(
        &mut self,
        store: &FasterKv<K, V>,
        slot: &SlotId,
    ) -> Result<Option<ScanPage<K, V>>, Status> {
        assert!(
            std::ptr::eq(store, self.store),
            "scanner belongs to another store"
        );
        assert!(
            store.epoch.is_protected(slot),
            "scanning requires an active session"
        );
        self.read_next(Some(slot))
    }

    fn read_next(&mut self, borrowed: Option<&SlotId>) -> Result<Option<ScanPage<K, V>>, Status> {
        if self.next >= self.until {
            return Ok(None);
        }
        let slot = borrowed.or(self.slot.as_ref()).ok_or(Status::OutOfMemory)?;
        let start = self.next;
        let page_end = Address::new(start.page() + 1, 0);
        let end = self.until.min(page_end);
        self.next = page_end;
        let len =
            (self.store.hlog.device_offset(end) - self.store.hlog.device_offset(start)) as usize;
        let mut words = vec![0u64; len.div_ceil(8)];
        if borrowed.is_none() {
            self.store.epoch.protect_and_drain(slot);
        }
        if start >= self.store.hlog.head.load() {
            // SAFETY: the range is resident and read-only, and our epoch
            // protection keeps its frame from being reused while we copy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.store.hlog.get(start),
                    words.as_mut_ptr().cast(),
                    len,
                )
            };
            if borrowed.is_none() {
                self.store.epoch.unprotect(slot);
            }
        } else {
            if borrowed.is_none() {
                self.store.epoch.unprotect(slot);
            }
            let (tx, rx) = mpsc::channel();
            self.store
                .hlog
                .read_async(start, len, Box::new(move |r| drop(tx.send(r))));
            let bytes = rx
                .recv()
                .map_err(|_| Status::IoError)?
                .map_err(|_| Status::IoError)?;
            if bytes.len() != len {
                return Err(Status::IoError);
            }
            // SAFETY: `words` holds at least `len` bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), words.as_mut_ptr().cast(), len)
            };
        }
        Ok(Some(ScanPage {
            words,
            base: start,
            error: None,
            _types: std::marker::PhantomData,
        }))
    }
}

impl<K, V> Drop for LogScanner<'_, K, V>
where
    K: Key,
    V: Value,
{
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.store.epoch.release(slot);
        }
    }
}

impl<K: Key, V: Value> ScanPage<K, V> {
    /// Structural decoding error observed while iterating this page.
    pub fn error(&self) -> Option<Status> {
        self.error
    }

    pub fn base(&self) -> Address {
        self.base
    }

    /// Records in this page in address order, including invalid ones. A null
    /// header marks the unused rest of the page.
    pub fn records(&mut self) -> impl Iterator<Item = ScannedRecord<'_, K, V>> {
        let min_size = Layout::<K, V>::min_io_size();
        let bytes = self.words.len() * 8;
        let base = self.base;
        let ptr = self.words.as_mut_ptr().cast::<u8>();
        let mut offset = 0usize;
        let error = &mut self.error;
        std::iter::from_fn(move || {
            if offset + min_size > bytes {
                if offset + 8 <= bytes {
                    // SAFETY: a full aligned header remains in the owned page.
                    if unsafe { ptr.add(offset).cast::<u64>().read() } != 0 {
                        *error = Some(Status::Corruption);
                    }
                }
                return None;
            }
            // SAFETY: the record lies inside our 8-byte aligned buffer, which
            // the iterator borrows for its lifetime.
            let record = unsafe { Record::<K, V>::from_ptr(ptr.add(offset)) };
            let info = record.info();
            if info.is_null() {
                return None;
            }
            // SAFETY: header reads are checked against the remaining page.
            let required = match unsafe { record.required_bytes(bytes - offset, bytes - offset) } {
                Ok(required) => required,
                Err(()) => {
                    *error = Some(Status::Corruption);
                    return None;
                }
            };
            if required > bytes - offset {
                *error = Some(Status::Corruption);
                return None;
            }
            // SAFETY: the fixed headers were validated above.
            let size = unsafe { record.size() } as usize;
            if offset + size > bytes {
                *error = Some(Status::Corruption);
                return None;
            }
            let address = base.add(offset as u64);
            offset += size;
            // SAFETY: a non-null header means the record was fully written
            // before it became read-only.
            let key = unsafe { &*(record.key() as *const K) };
            let value = (!info.tombstone()).then(|| {
                // SAFETY: as above.
                unsafe { &*(record.value() as *const V) }
            });
            Some(ScannedRecord {
                address,
                info,
                key,
                value,
                raw: ptr.wrapping_add(offset - size).cast(),
                size,
            })
        })
    }
}
