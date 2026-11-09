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
