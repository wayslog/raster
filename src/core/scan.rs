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
    _types: std::marker::PhantomData<(K, V)>,
}

/// A record inside a [`ScanPage`].
pub struct ScannedRecord<'p, K, V> {
    pub address: Address,
    pub info: RecordInfo,
    pub key: &'p K,
    /// `None` for tombstones, whose value bytes were never written.
    pub value: Option<&'p V>,
    /// The record's raw words, header first.
    pub words: &'p [u64],
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Scans records in `[begin, until)`, clamped to the valid, immutable log.
    pub fn scan(&self, begin: Address, until: Address) -> LogScanner<'_, K, V> {
        let slot = self
            .epoch
            .reserve_helper()
            .expect("all epoch slots are in use");
        let begin = begin.max(self.hlog.begin.load());
        let until = until.min(self.hlog.safe_read_only.load());
        LogScanner {
            store: self,
            slot: Some(slot),
            next: begin,
            until,
        }
    }
}

impl<K: Key, V: Value> LogScanner<'_, K, V> {
    pub fn until(&self) -> Address {
        self.until
    }

    /// Copies the next page range, or returns `None` at the end of the scan.
    pub fn next_page(&mut self) -> Result<Option<ScanPage<K, V>>, Status> {
        if self.next >= self.until {
            return Ok(None);
        }
        let start = self.next;
        let page_end = Address::new(start.page() + 1, 0);
        let end = self.until.min(page_end);
        self.next = page_end;
        let len =
            (self.store.hlog.device_offset(end) - self.store.hlog.device_offset(start)) as usize;
        let mut words = vec![0u64; len.div_ceil(8)];
        let slot = self.slot.as_ref().expect("live scanner");
        self.store.epoch.protect_and_drain(slot);
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
            self.store.epoch.unprotect(slot);
        } else {
            self.store.epoch.unprotect(slot);
            let (tx, rx) = mpsc::channel();
            self.store
                .hlog
                .read_async(start, len, Box::new(move |r| drop(tx.send(r))));
            let bytes = rx
                .recv()
                .map_err(|_| Status::IoError)?
                .map_err(|_| Status::IoError)?;
            // SAFETY: `words` holds at least `len` bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), words.as_mut_ptr().cast(), len)
            };
        }
        Ok(Some(ScanPage {
            words,
            base: start,
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
        std::iter::from_fn(move || {
            if offset + min_size > bytes {
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
            let required = unsafe { record.required_bytes(bytes - offset, bytes - offset) }.ok()?;
            if required > bytes - offset {
                return None;
            }
            // SAFETY: the fixed headers were validated above.
            let size = unsafe { record.size() } as usize;
            if offset + size > bytes {
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
            // SAFETY: the record's words lie inside the buffer.
            let words = unsafe {
                std::slice::from_raw_parts(ptr.add(offset - size).cast::<u64>(), size / 8)
            };
            Some(ScannedRecord {
                address,
                info,
                key,
                value,
                words,
            })
        })
    }
}
