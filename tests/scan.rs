//! Log scans over disk and memory, after FASTER `cc/test/*scan*`.
use raster::core::address::Address;
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct Put {
    key: u64,
}
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<u64> for Put {
    fn key(&self) -> &u64 {
        &self.key
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl UpsertContext<u64, AtomicU64> for Put {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.key * 3));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.key * 3, Ordering::Relaxed);
        true
    }
}

#[test]
fn scan_returns_every_immutable_record_in_address_order() {
    let dir = std::env::temp_dir().join(format!("raster-scan-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let device = Arc::new(FileDevice::open(&dir, 20, 2).unwrap());
    let store = FasterKv::<u64, AtomicU64>::with_device(
        StoreConfig {
            index_buckets: 1 << 14,
            filepath: None,
            pre_allocate: true,
            read_cache: None,
            auto_compaction: None,
            log_size_budget: 0,
            checkpoint_dir: None,
            log: LogConfig {
                page_bits: 12,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        device,
    );
    const KEYS: u64 = 50_000;
    {
        let mut session = store.start_session();
        for key in 0..KEYS {
            assert_eq!(session.upsert(&mut Put { key }, key), Status::Ok);
        }
    }
    let head = store.hlog.head.load();
    let mut scanner = store.scan(Address::new(0, 0), Address::MAX);
    let until = scanner.until();
    assert!(until > head, "scan covers memory as well as disk");
    let (mut expected, mut last, mut disk, mut memory) = (0u64, Address::default(), 0, 0);
    while let Some(mut page) = scanner.next_page().unwrap() {
        for record in page.records() {
            assert!(record.address > last && record.address < until);
            last = record.address;
            assert!(!record.info.invalid() && !record.info.tombstone());
            assert_eq!(*record.key, expected);
            assert_eq!(record.value.unwrap().load(Ordering::Relaxed), expected * 3);
            if record.address < head {
                disk += 1;
            } else {
                memory += 1;
            }
            expected += 1;
        }
    }
    assert!(disk > 0 && memory > 0, "disk={disk} memory={memory}");
    assert!(expected > KEYS / 2, "scanned {expected} records");
    drop(scanner);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

mod common;
use common::varlen::{self as variable, TestDirectory};

#[test]
fn scan_variable_records_and_tombstones_in_address_order() {
    let dir = TestDirectory::new("scan");
    let store = variable::store(&dir, false, false);
    let mut session = store.start_session();
    for id in 0..600 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 1 + id as usize % 999), id),
            Status::Ok
        );
    }
    for id in (0..600).step_by(3) {
        assert_eq!(
            session.delete(&variable::request(id, id as usize % 37), 600 + id),
            Status::Ok
        );
    }
    let end = store.hlog.tail();
    store.hlog.shift_read_only_to(end);
    while store.hlog.safe_read_only.load() < end {
        session.refresh();
    }
    let mut scanner = store.scan(store.hlog.begin.load(), end);
    let (mut records, mut tombstones, mut last) = (0, 0, Address::INVALID);
    while let Some(mut page) = scanner.next_page().unwrap() {
        for record in page.records() {
            assert!(record.address > last);
            last = record.address;
            let key = record.key.bytes();
            let id = u64::from_le_bytes(key[..8].try_into().unwrap());
            assert_eq!(key, variable::request(id, 0).key_bytes);
            if record.info.tombstone() {
                tombstones += 1;
                assert!(record.value.is_none());
            } else if !record.info.invalid() {
                assert_eq!(
                    record.value.unwrap().snapshot(),
                    vec![88; 1 + id as usize % 999]
                );
            }
            records += 1;
        }
    }
    assert!(records >= 600);
    assert_eq!(tombstones, 200);
}
