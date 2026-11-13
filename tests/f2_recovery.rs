mod common;

use common::varlen::{TestDirectory, VarKey, VarRequest, VarValue};
use raster::core::f2::{F2Config, F2Kv, F2Session};
use raster::core::hlog::LogConfig;
use raster::core::{ReadContext, Status};

type Store = F2Kv<VarKey, VarValue>;

fn config(dir: &TestDirectory) -> F2Config {
    let log = LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };
    F2Config {
        hot_index_buckets: 256,
        cold_index_buckets: 256,
        hot_log: log,
        cold_log: log,
        cold_index_log: log,
        ..F2Config::new(&dir.0)
    }
}

fn read(session: &mut F2Session<'_, VarKey, VarValue>, key: u64, serial: u64) -> (Status, Vec<u8>) {
    let mut request = VarRequest::<false>::new(key, 73, 0);
    let status = session.read(&mut request, serial);
    if status == Status::Pending {
        assert!(session.complete_pending(true));
    } else {
        ReadContext::on_complete(&mut request, status);
    }
    request.out.borrow_mut().take().unwrap()
}

#[test]
fn joint_checkpoint_reopens_migrated_data_and_user_session_prefix() {
    let dir = TestDirectory::new("f2-recovery");
    let config = config(&dir);
    let mut store = Store::open(config.clone()).unwrap();
    let guid = {
        let mut session = store.try_start_session().unwrap();
        for key in 0..96 {
            let mut request = VarRequest::<false>::new(key, 73, 400);
            request.data.fill(key as u8);
            assert_eq!(session.upsert(&mut request, key + 1), Status::Ok);
        }
        session.guid()
    };
    let until = store.hot_compaction_limit();
    assert_eq!(store.compact_hot(until, 2).unwrap().begin_address, until);
    let deleter = {
        let mut session = store.try_start_session().unwrap();
        assert_eq!(
            session.delete(&VarRequest::<false>::new(1, 73, 0), 8),
            Status::Ok
        );
        session.guid()
    };
    let token = store.checkpoint().unwrap();
    drop(store);

    let mut reopened = Store::open(config.clone()).unwrap();
    assert!(reopened.try_start_session().is_none());
    assert_eq!(reopened.latest_checkpoint(), Ok(token));
    let sessions = reopened.recover(token).unwrap();
    assert_eq!(sessions.len(), 2);
    assert!(sessions.contains(&guid));
    assert!(sessions.contains(&deleter));
    {
        let (mut session, serial) = reopened.continue_session(guid).unwrap();
        assert_eq!(serial, 96);
        assert!(reopened.continue_session(guid).is_none());
        assert_eq!(read(&mut session, 0, 97), (Status::Ok, vec![0; 400]));
        assert_eq!(read(&mut session, 1, 98), (Status::NotFound, vec![]));
        let mut replacement = VarRequest::<false>::new(0, 73, 20);
        replacement.data.fill(42);
        assert_eq!(session.upsert(&mut replacement, 99), Status::Ok);
    }
    let newest = reopened.checkpoint().unwrap();
    drop(reopened);

    let mut previous = Store::open(config).unwrap();
    assert_eq!(previous.latest_checkpoint(), Ok(newest));
    previous.recover(token).unwrap();
    assert_eq!(previous.latest_checkpoint(), Ok(token));
    let (mut session, serial) = previous.continue_session(guid).unwrap();
    assert_eq!(serial, 96);
    assert_eq!(read(&mut session, 0, 97), (Status::Ok, vec![0; 400]));
}

#[test]
fn migration_and_failed_checkpoint_report_separate_results() {
    let dir = TestDirectory::new("f2-catalog-write-failure");
    let config = config(&dir);
    let mut store = Store::open(config.clone()).unwrap();
    let key = 0;
    let mut request = VarRequest::<false>::new(key, 73, 24);
    request.data.fill(17);
    {
        let mut session = store.try_start_session().unwrap();
        assert_eq!(session.upsert(&mut request, 1), Status::Ok);
    }
    let published = store.checkpoint().unwrap();
    {
        let mut session = store.try_start_session().unwrap();
        request.data.fill(18);
        assert_eq!(session.upsert(&mut request, 2), Status::Ok);
        for key in 1..96 {
            let mut filler = VarRequest::<false>::new(key, 73, 400);
            assert_eq!(session.upsert(&mut filler, key + 2), Status::Ok);
        }
    }
    std::fs::create_dir(dir.0.join("f2-checkpoints.tmp")).unwrap();
    let cutoff = store.hot_compaction_limit();
    assert!(cutoff.page() > 0);
    let result = store.compact_hot_with_checkpoint(cutoff, 2).unwrap();
    assert_eq!(result.begin_address, cutoff);
    assert_eq!(result.checkpoint, Some(Err(Status::IoError)));
    assert_eq!(store.latest_checkpoint(), Ok(published));
    {
        let mut session = store.try_start_session().unwrap();
        assert_eq!(read(&mut session, key, 1), (Status::Ok, vec![18; 24]));
    }
    drop(store);

    let mut reopened = Store::open(config).unwrap();
    reopened.recover(published).unwrap();
    let mut session = reopened.try_start_session().unwrap();
    assert_eq!(read(&mut session, key, 1), (Status::Ok, vec![17; 24]));
}
