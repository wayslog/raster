mod common;

use common::varlen::{TestDirectory, VarKey, VarRequest, VarValue};
use raster::core::cold_store::ColdStore;
use raster::core::f2::{F2Kv, F2Session};
use raster::core::hlog::LogConfig;
use raster::core::{FasterKv, ReadCacheConfig, ReadContext, Status, StoreConfig};

type Store = F2Kv<VarKey, VarValue>;

fn seeded_store(dir: &TestDirectory, seed_hot: bool) -> Store {
    let log = LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };
    let cold = ColdStore::open(&dir.0, 256, log, log).unwrap();
    {
        let mut session = cold.try_start_session().unwrap();
        for key in 0..128 {
            let mut request = VarRequest::<false>::new(key, 73, 400);
            request.data.fill(key as u8);
            let status = session.upsert(&mut request, key + 1);
            assert!(matches!(status, Status::Ok | Status::Pending));
            assert!(session.complete_pending(true));
        }
    }
    let hot = FasterKv::open(StoreConfig {
        index_buckets: 256,
        filepath: Some(dir.0.join("hot")),
        read_cache: seed_hot.then_some(ReadCacheConfig {
            pre_allocate: false,
            log,
        }),
        log,
        ..StoreConfig::default()
    })
    .unwrap();
    if seed_hot {
        let mut session = hot.start_session();
        let mut key = VarRequest::<false>::new(0, 73, 0);
        assert_eq!(session.upsert(&mut key, 1), Status::Ok);
        assert_eq!(session.delete(&key, 2), Status::Ok);
        for id in 200..328 {
            let mut filler = VarRequest::<false>::new(id, 73, 400);
            assert_eq!(session.upsert(&mut filler, id), Status::Ok);
        }
    }
    F2Kv::new(hot, cold)
}

fn read(
    session: &mut F2Session<'_, VarKey, VarValue>,
    key: u64,
    serial: u64,
) -> (bool, Status, Vec<u8>) {
    let mut request = VarRequest::<false>::new(key, 73, 0);
    let status = session.read(&mut request, serial);
    if status == Status::Pending {
        assert!(session.complete_pending(true));
    } else {
        ReadContext::on_complete(&mut request, status);
    }
    let (completed, value) = request.out.borrow_mut().take().expect("completed read");
    assert!(session.complete_pending(false));
    assert!(request.out.borrow().is_none());
    (status == Status::Pending, completed, value)
}

#[test]
fn hot_reads_writes_and_tombstones_take_precedence_over_cold_values() {
    let dir = TestDirectory::new("f2-routing");
    let store = seeded_store(&dir, false);
    let mut session = store.try_start_session().unwrap();
    assert_eq!(read(&mut session, 0, 1), (true, Status::Ok, vec![0; 400]));

    let mut hot = VarRequest::<false>::new(0, 73, 51);
    hot.data.fill(42);
    assert_eq!(session.upsert(&mut hot, 2), Status::Ok);
    assert_eq!(read(&mut session, 0, 3), (false, Status::Ok, vec![42; 51]));

    let cold_only = VarRequest::<false>::new(1, 73, 0);
    assert_eq!(session.delete(&cold_only, 4), Status::Ok);
    assert_eq!(read(&mut session, 1, 5), (false, Status::NotFound, vec![]));
    assert_eq!(read(&mut session, 1000, 6).1, Status::NotFound);
}

#[test]
fn pending_cold_reads_can_overlap_hot_writes_and_deletes() {
    let dir = TestDirectory::new("f2-read-race");
    let store = seeded_store(&dir, false);
    let mut reader = store.try_start_session().unwrap();
    let mut writer = store.try_start_session().unwrap();
    assert_ne!(reader.guid(), writer.guid());

    let mut upserted = VarRequest::<false>::new(0, 73, 0);
    let mut deleted = VarRequest::<false>::new(1, 73, 0);
    assert_eq!(reader.read(&mut upserted, 1), Status::Pending);
    assert_eq!(reader.read(&mut deleted, 2), Status::Pending);
    let mut replacement = VarRequest::<false>::new(0, 73, 16);
    replacement.data.fill(27);
    assert_eq!(writer.upsert(&mut replacement, 1), Status::Ok);
    assert_eq!(writer.delete(&deleted, 2), Status::Ok);

    reader.refresh();
    assert!(reader.complete_pending(true));
    assert_eq!(
        upserted.out.borrow_mut().take(),
        Some((Status::Ok, vec![0; 400]))
    );
    assert_eq!(
        deleted.out.borrow_mut().take(),
        Some((Status::Ok, vec![1; 400]))
    );
    assert_eq!(reader.serial(), 2);
    assert_eq!(read(&mut reader, 0, 3), (false, Status::Ok, vec![27; 16]));
    assert_eq!(read(&mut reader, 1, 4), (false, Status::NotFound, vec![]));
}

#[test]
fn competing_rmw_operations_retry_against_the_hot_winner() {
    let dir = TestDirectory::new("f2-rmw-race");
    let store = seeded_store(&dir, false);
    let mut first = store.try_start_session().unwrap();
    let mut second = store.try_start_session().unwrap();

    let mut one = VarRequest::<false>::new(0, 73, 0);
    one.increment = 1;
    one.suffix.push(17);
    let mut two = VarRequest::<false>::new(0, 73, 0);
    two.increment = 2;
    two.suffix.push(19);
    assert_eq!(first.rmw(&mut one, 1), Status::Pending);
    assert_eq!(second.rmw(&mut two, 1), Status::Pending);
    assert_eq!(second.serial(), 1);
    let mut unrelated = VarRequest::<false>::new(2000, 73, 32);
    assert_eq!(second.upsert(&mut unrelated, 2), Status::Ok);
    assert!(first.complete_pending(true));
    drop(second);
    assert_eq!(one.out.borrow_mut().take(), Some((Status::Ok, vec![])));
    assert_eq!(two.out.borrow_mut().take(), Some((Status::Ok, vec![])));

    let (_, status, data) = read(&mut first, 0, 2);
    assert_eq!(status, Status::Ok);
    assert_eq!(data.len(), 402);
    assert_eq!(u64::from_le_bytes(data[..8].try_into().unwrap()), 3);
    assert_eq!(&data[400..], &[17, 19]);
}

#[test]
fn rmw_after_a_hot_delete_starts_from_absence() {
    let dir = TestDirectory::new("f2-rmw-delete-race");
    let store = seeded_store(&dir, false);
    let mut updater = store.try_start_session().unwrap();
    let mut writer = store.try_start_session().unwrap();

    let mut pending = VarRequest::<false>::new(1, 73, 0);
    pending.suffix.push(12);
    assert_eq!(updater.rmw(&mut pending, 1), Status::Pending);
    assert_eq!(writer.delete(&pending, 1), Status::Ok);
    let mut newer = VarRequest::<false>::new(2000, 73, 32);
    assert_eq!(updater.upsert(&mut newer, 2), Status::Ok);
    assert!(updater.complete_pending(true));
    assert_eq!(updater.serial(), 2);
    assert_eq!(pending.out.borrow_mut().take(), Some((Status::Ok, vec![])));
    assert_eq!(read(&mut updater, 1, 3), (false, Status::Ok, vec![12]));

    let mut missing = VarRequest::<false>::new(1000, 73, 0);
    missing.suffix.push(9);
    let status = updater.rmw(&mut missing, 4);
    if status == Status::Pending {
        assert!(updater.complete_pending(true));
        assert_eq!(missing.out.borrow_mut().take(), Some((Status::Ok, vec![])));
    } else {
        assert_eq!(status, Status::Ok);
    }
    assert_eq!(read(&mut updater, 1000, 5), (false, Status::Ok, vec![9]));
}

#[test]
fn disk_hot_tombstones_stop_reads_and_seed_rmw_from_absence() {
    let dir = TestDirectory::new("f2-disk-tombstone");
    let store = seeded_store(&dir, true);
    let mut session = store.try_start_session().unwrap();
    assert_eq!(read(&mut session, 0, 1), (true, Status::NotFound, vec![]));

    let mut pending = VarRequest::<false>::new(0, 73, 0);
    pending.suffix.push(34);
    assert_eq!(session.rmw(&mut pending, 2), Status::Pending);
    assert!(session.complete_pending(true));
    assert_eq!(pending.out.borrow_mut().take(), Some((Status::Ok, vec![])));
    assert_eq!(read(&mut session, 0, 3), (false, Status::Ok, vec![34]));
}

mod inline_value {
    use super::TestDirectory;
    use raster::core::{
        FasterKv, KeyContext, RmwContext, Status, StoreConfig, UpsertContext, Value, ValueSlot,
        cold_store::ColdStore, f2::F2Kv, hlog::LogConfig,
    };

    struct Inline(u64);
    // SAFETY: The fixed header accepts all bit patterns, and initialization
    // writes the header and the complete inline allocation.
    unsafe impl Value for Inline {
        fn size(&self) -> u32 {
            16
        }
        fn initialize_empty(mut slot: ValueSlot<'_, Self>) {
            // SAFETY: The store supplies a complete 16-byte value slot.
            unsafe { slot.as_mut_ptr().cast::<[u64; 2]>().write([0, 1]) }
        }
    }

    #[derive(Clone)]
    struct Request(u64);
    // SAFETY: The request owns a complete immutable u64 key.
    unsafe impl KeyContext<u64> for Request {
        fn key(&self) -> &u64 {
            &self.0
        }
    }
    // SAFETY: put initializes the full 16-byte allocation.
    unsafe impl UpsertContext<u64, Inline> for Request {
        fn value_size(&self) -> u32 {
            16
        }
        fn put(&mut self, slot: ValueSlot<'_, Inline>) {
            Inline::initialize_empty(slot);
        }
        fn put_atomic(&mut self, _: &Inline) -> bool {
            false
        }
    }
    // SAFETY: Both initialization paths write the complete 16-byte value.
    unsafe impl RmwContext<u64, Inline> for Request {
        fn copy_value_size(&self, _: &Inline) -> u32 {
            16
        }
        fn initial_value_size(&self) -> u32 {
            16
        }
        fn rmw_initial(&mut self, slot: ValueSlot<'_, Inline>) {
            Inline::initialize_empty(slot);
        }
        fn rmw_copy(&mut self, old: &Inline, slot: ValueSlot<'_, Inline>) {
            assert_eq!(old.0, 0);
            Inline::initialize_empty(slot);
        }
        fn rmw_atomic(&mut self, _: &Inline) -> bool {
            false
        }
        fn on_complete(&mut self, status: Status) {
            assert_eq!(status, Status::Ok);
        }
    }

    #[test]
    fn copies_inline_allocation_from_cold_log() {
        let dir = TestDirectory::new("f2-inline-allocation");
        let log = LogConfig {
            page_bits: 12,
            memory_pages: 4,
            mutable_fraction: 0.5,
        };
        let cold = ColdStore::open(&dir.0, 256, log, log).unwrap();
        {
            let mut session = cold.try_start_session().unwrap();
            for key in 0..1024 {
                let status = session.upsert(&mut Request(key), key + 1);
                assert!(matches!(status, Status::Ok | Status::Pending));
                assert!(session.complete_pending(true));
            }
        }
        let hot = FasterKv::new(StoreConfig {
            log,
            index_buckets: 256,
            ..StoreConfig::default()
        });
        let store = F2Kv::new(hot, cold);
        let mut session = store.try_start_session().unwrap();
        assert_eq!(session.rmw(&mut Request(1023), 1), Status::Ok);
        assert_eq!(session.rmw(&mut Request(0), 2), Status::Pending);
        assert!(session.complete_pending(true));
    }
}

#[test]
fn hot_migration_and_cold_compaction_preserve_live_records_and_tombstones() {
    let dir = TestDirectory::new("f2-compaction");
    let store = seeded_store(&dir, false);
    let mut session = store.try_start_session().unwrap();
    let mut original = VarRequest::<false>::new(0, 73, 400);
    original.data.fill(21);
    assert_eq!(session.upsert(&mut original, 1), Status::Ok);
    let deleted = VarRequest::<false>::new(1, 73, 0);
    assert_eq!(session.delete(&deleted, 2), Status::Ok);
    let mut shadowed = VarRequest::<false>::new(2, 73, 400);
    shadowed.data.fill(31);
    assert_eq!(session.upsert(&mut shadowed, 3), Status::Ok);
    for id in 200..328 {
        let mut filler = VarRequest::<false>::new(id, 73, 400);
        assert_eq!(session.upsert(&mut filler, id), Status::Ok);
    }
    shadowed.data.fill(37);
    assert_eq!(session.upsert(&mut shadowed, 329), Status::Ok);

    let hot_cut = store.hot_compaction_limit();
    assert!(hot_cut.page() > 0);
    let result = store.compact_hot(hot_cut, 3).unwrap();
    assert_eq!(result.begin_address, hot_cut);
    assert_eq!(read(&mut session, 0, 330).1, Status::Ok);
    assert_eq!(read(&mut session, 1, 331).1, Status::NotFound);
    assert_eq!(
        read(&mut session, 2, 332),
        (false, Status::Ok, vec![37; 400])
    );
    assert_eq!(read(&mut session, 3, 333).2, vec![3; 400]);

    let mut pending = VarRequest::<false>::new(3, 73, 0);
    assert_eq!(session.read(&mut pending, 334), Status::Pending);
    let cold_cut = store.cold_compaction_limit();
    assert!(cold_cut.page() > 0);
    let result = store.compact_cold(cold_cut, 3).unwrap();
    assert_eq!(result.begin_address, cold_cut);
    assert!(session.complete_pending(true));
    assert_eq!(
        pending.out.borrow_mut().take(),
        Some((Status::Ok, vec![3; 400]))
    );
    assert_eq!(read(&mut session, 0, 335).2, vec![21; 400]);
    assert_eq!(read(&mut session, 1, 336).1, Status::NotFound);
    assert_eq!(read(&mut session, 3, 337).2, vec![3; 400]);
}

#[test]
fn pending_rmw_restarts_when_migration_clears_its_hot_index_slot() {
    let dir = TestDirectory::new("f2-rmw-migration");
    let store = seeded_store(&dir, false);
    let mut reader = store.try_start_session().unwrap();
    let mut writer = store.try_start_session().unwrap();

    let mut pending = VarRequest::<false>::new(0, 73, 0);
    pending.suffix.push(7);
    assert_eq!(reader.rmw(&mut pending, 1), Status::Pending);
    let mut replacement = VarRequest::<false>::new(0, 73, 400);
    replacement.data.fill(42);
    assert_eq!(writer.upsert(&mut replacement, 1), Status::Ok);
    for key in 200..328 {
        let mut filler = VarRequest::<false>::new(key, 73, 400);
        assert_eq!(writer.upsert(&mut filler, key), Status::Ok);
    }

    let until = store.hot_compaction_limit();
    assert!(until.page() > 0);
    assert_eq!(store.compact_hot(until, 2).unwrap().begin_address, until);
    assert!(reader.complete_pending(true));
    assert_eq!(pending.out.borrow_mut().take(), Some((Status::Ok, vec![])));
    let mut expected = vec![42; 400];
    expected.push(7);
    assert_eq!(read(&mut reader, 0, 2), (false, Status::Ok, expected));
}

#[test]
fn failed_migration_keeps_the_source_boundary_and_can_retry() {
    use raster::core::address::Address;
    use raster::core::device::{Device, FileDevice, ReadCallback, WriteCallback};
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct ScanFailure {
        inner: FileDevice,
        enabled: AtomicBool,
    }

    impl Device for ScanFailure {
        unsafe fn write_async(
            &self,
            offset: u64,
            data: *const u8,
            len: usize,
            done: WriteCallback,
        ) {
            // SAFETY: The underlying device gets the same write request.
            unsafe { self.inner.write_async(offset, data, len, done) };
        }

        fn read_async(&self, offset: u64, len: usize, done: ReadCallback) {
            if offset == 4096 && len == 4096 && self.enabled.load(Ordering::Acquire) {
                done(Err(io::Error::other("compaction scan failure")));
            } else {
                self.inner.read_async(offset, len, done);
            }
        }
    }

    let dir = TestDirectory::new("f2-migration-retry");
    let log = LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };
    let device = Arc::new(ScanFailure {
        inner: FileDevice::open_with(dir.0.join("hot"), 30, 2, false).unwrap(),
        enabled: AtomicBool::new(true),
    });
    let hot = FasterKv::with_device(
        StoreConfig {
            index_buckets: 256,
            log,
            ..StoreConfig::default()
        },
        device.clone(),
    );
    let begin = hot.hlog.begin.load();
    let store = F2Kv::new(hot, ColdStore::open(&dir.0, 256, log, log).unwrap());
    let mut session = store.try_start_session().unwrap();
    for key in 0..128 {
        let mut request = VarRequest::<false>::new(key, 73, 400);
        request.data.fill(key as u8);
        assert_eq!(session.upsert(&mut request, key + 1), Status::Ok);
    }
    let until = store.hot_compaction_limit();
    assert!(until.page() > 1);
    assert_eq!(store.compact_hot(until, 1), Err(Status::IoError));
    assert_eq!(
        store
            .compact_hot(Address::default(), 1)
            .unwrap()
            .begin_address,
        begin
    );
    assert_eq!(read(&mut session, 0, 129).2, vec![0; 400]);

    device.enabled.store(false, Ordering::Release);
    assert_eq!(store.compact_hot(until, 2).unwrap().begin_address, until);
    assert_eq!(read(&mut session, 0, 130).2, vec![0; 400]);
    assert_eq!(read(&mut session, 25, 131).2, vec![25; 400]);
}

#[test]
fn background_compaction_advances_both_configured_layers() {
    use raster::core::address::Address;
    use raster::core::auto_compact::AutoCompactionConfig;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    let dir = TestDirectory::new("f2-background-compaction");
    let store = Arc::new(seeded_store(&dir, false));
    let mut session = store.try_start_session().unwrap();
    for key in 200..328 {
        let mut request = VarRequest::<false>::new(key, 73, 400);
        request.data.fill(12);
        assert_eq!(session.upsert(&mut request, key), Status::Ok);
    }
    let config = AutoCompactionConfig {
        check_interval: Duration::from_millis(5),
        threads: 2,
        ..AutoCompactionConfig::with_budget(16 << 10)
    };
    let worker = store.spawn_auto_compaction(Some(config), Some(config));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let hot = store.compact_hot(Address::default(), 1);
        let cold = store.compact_cold(Address::default(), 1);
        if hot.is_ok_and(|result| result.begin_address.page() > 0)
            && cold.is_ok_and(|result| result.begin_address.page() > 0)
        {
            break;
        }
        assert!(Instant::now() < deadline, "F2 compaction did not advance");
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(worker);
    assert_eq!(read(&mut session, 200, 328).2, vec![12; 400]);
    assert_eq!(read(&mut session, 3, 329).2, vec![3; 400]);
}

#[test]
fn pending_cold_collision_chain_survives_compaction_cutoff() {
    let dir = TestDirectory::new("f2-cold-collision-compaction");
    let log = LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };
    let cold = ColdStore::<VarKey<true>, VarValue>::open(&dir.0, 1, log, log).unwrap();
    {
        let mut session = cold.try_start_session().unwrap();
        for key in 0..40 {
            let mut request = VarRequest::<true>::new(key, 73, 400);
            request.data.fill(key as u8);
            assert_eq!(session.upsert(&mut request, key + 1), Status::Ok);
        }
    }
    let hot = FasterKv::new(StoreConfig {
        log,
        index_buckets: 1,
        ..StoreConfig::default()
    });
    let store = F2Kv::new(hot, cold);
    let mut session = store.try_start_session().unwrap();
    let mut old = VarRequest::<true>::new(1, 73, 0);
    assert_eq!(session.read(&mut old, 1), Status::Pending);
    let until = store.cold_compaction_limit();
    assert!(until.page() > 0);
    assert_eq!(store.compact_cold(until, 3).unwrap().begin_address, until);
    assert!(session.complete_pending(true));
    assert_eq!(
        old.out.borrow_mut().take(),
        Some((Status::Ok, vec![1; 400]))
    );

    let status = session.read(&mut old, 2);
    if status == Status::Pending {
        assert!(session.complete_pending(true));
    } else {
        ReadContext::on_complete(&mut old, status);
    }
    assert_eq!(
        old.out.borrow_mut().take(),
        Some((Status::Ok, vec![1; 400]))
    );
}
