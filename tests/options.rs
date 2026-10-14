mod common;
use common::varlen::{self as variable, TestDirectory};
use raster::core::*;

#[test]
fn operation_options_preserve_default_behavior_and_control_missing_keys() {
    let dir = TestDirectory::new("options");
    let store = variable::store(&dir, false, false);
    let mut session = store.start_session();
    let mut op = variable::request(1, 41);
    op.increment = 1;
    let tail = store.hlog.tail();
    assert_eq!(
        session.rmw_with_options(
            &mut op,
            1,
            RmwOptions {
                create_if_not_exists: false
            }
        ),
        Status::NotFound
    );
    assert_eq!(store.hlog.tail(), tail);
    assert_eq!(session.delete(&op, 2), Status::NotFound);
    assert_eq!(
        session.delete_with_options(
            &op,
            3,
            DeleteOptions {
                force_tombstone: true
            }
        ),
        Status::Ok
    );
    assert!(store.hlog.tail() > tail);
    assert_eq!(session.read(&mut op, 4), Status::NotFound);
    assert_eq!(
        session.read_with_options(
            &mut op,
            5,
            ReadOptions {
                abort_if_tombstone: true
            }
        ),
        Status::Aborted
    );
    assert_eq!(
        session.rmw_with_options(
            &mut op,
            6,
            RmwOptions {
                create_if_not_exists: false
            }
        ),
        Status::NotFound
    );
    assert_eq!(session.rmw(&mut op, 7), Status::Ok);
    assert_eq!(op.read(&mut session, 8).0, Status::Ok);
}

#[test]
fn pending_read_retains_abort_if_tombstone() {
    let dir = TestDirectory::new("options-pending");
    let store = variable::store(&dir, false, false);
    let mut session = store.start_session();
    let mut deleted = variable::request(0, 71);
    assert_eq!(
        session.delete_with_options(
            &deleted,
            0,
            DeleteOptions {
                force_tombstone: true
            }
        ),
        Status::Ok
    );
    for id in 1..500 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 600), id),
            Status::Ok
        );
    }
    assert_eq!(
        session.read_with_options(
            &mut deleted,
            501,
            ReadOptions {
                abort_if_tombstone: true
            }
        ),
        Status::Pending
    );
    assert!(session.complete_pending(true));
    assert_eq!(deleted.out.borrow_mut().take().unwrap().0, Status::Aborted);
    let tail = store.hlog.tail();
    deleted.expected_rmw_status = Status::NotFound;
    assert_eq!(
        session.rmw_with_options(
            &mut deleted,
            502,
            RmwOptions {
                create_if_not_exists: false
            }
        ),
        Status::Pending
    );
    assert!(session.complete_pending(true));
    assert_eq!(deleted.out.borrow_mut().take().unwrap().0, Status::NotFound);
    assert_eq!(store.hlog.tail(), tail);
}

#[test]
fn diagnostics_count_sessions_and_primary_bucket_chains() {
    let dir = TestDirectory::new("diagnostics");
    let store = variable::store(&dir, false, false);
    assert_eq!(store.num_active_sessions(), 0);
    assert_eq!(store.index_distribution().unwrap().get(&0), Some(&256));
    let mut session = store.start_session();
    let second = store.start_session();
    assert_eq!(store.num_active_sessions(), 2);
    drop(second);
    for id in 0..400 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 8), id),
            Status::Ok
        );
    }
    let histogram = store.index_distribution().unwrap();
    assert_eq!(histogram.values().sum::<usize>(), 256);
    assert_eq!(
        histogram
            .iter()
            .map(|(entries, buckets)| entries * buckets)
            .sum::<usize>(),
        400
    );
    assert_eq!(store.num_active_sessions(), 1);
}

#[test]
fn operation_options_survive_cache_and_checkpoint_transitions() {
    use raster::core::state::Phase;
    for cache in [false, true] {
        let dir = TestDirectory::new(if cache {
            "options-cpr-cache"
        } else {
            "options-cpr-disk"
        });
        let store = variable::store(&dir, false, cache);
        let mut session = store.start_session();
        let mut deleted = variable::request(0, 71);
        assert_eq!(
            session.delete_with_options(
                &deleted,
                0,
                DeleteOptions {
                    force_tombstone: true
                }
            ),
            Status::Ok
        );
        for id in 1..500 {
            assert_eq!(
                session.upsert(&mut variable::request(id, 600), id),
                Status::Ok
            );
        }
        let mut live = variable::request(1, 0);
        assert_eq!(live.read(&mut session, 500).0, Status::Ok);
        if cache {
            assert_eq!(
                session.read_with_options(
                    &mut live,
                    501,
                    ReadOptions {
                        abort_if_tombstone: true
                    }
                ),
                Status::Ok
            );
            assert_eq!(live.data, vec![88; 600]);
        }
        let mut blocker = store.start_session();
        store.checkpoint_hybrid_log(None).unwrap();
        session.refresh();
        assert_eq!(store.system_state().phase, Phase::Prepare);
        let abort = ReadOptions {
            abort_if_tombstone: true,
        };
        assert_eq!(
            session.read_with_options(&mut deleted, 502, abort),
            Status::Pending
        );
        while session.pending_count() != 0 {
            session.complete_pending(false);
        }
        assert_eq!(deleted.out.borrow_mut().take().unwrap().0, Status::Aborted);
        let tail = store.hlog.tail();
        deleted.expected_rmw_status = Status::NotFound;
        assert_eq!(
            session.rmw_with_options(
                &mut deleted,
                503,
                RmwOptions {
                    create_if_not_exists: false
                }
            ),
            Status::Pending
        );
        while session.pending_count() != 0 {
            session.complete_pending(false);
        }
        assert_eq!(deleted.out.borrow_mut().take().unwrap().0, Status::NotFound);
        assert_eq!(store.hlog.tail(), tail);
        let mut missing = variable::request(1000, 24);
        assert_eq!(
            session.delete_with_options(
                &missing,
                504,
                DeleteOptions {
                    force_tombstone: true
                }
            ),
            Status::Ok
        );
        assert_eq!(
            session.read_with_options(&mut missing, 505, abort),
            Status::Aborted
        );
        blocker.refresh();
        session.refresh();
        assert_ne!(store.system_state().phase, Phase::Prepare);
        assert_eq!(
            session.read_with_options(&mut deleted, 506, abort),
            Status::Pending
        );
        while session.pending_count() != 0 {
            session.complete_pending(false);
        }
        assert_eq!(deleted.out.borrow_mut().take().unwrap().0, Status::Aborted);
        while store.system_state().phase != Phase::Rest {
            session.complete_pending(false);
            blocker.complete_pending(false);
        }
        assert_eq!(missing.read(&mut session, 507).0, Status::NotFound);
    }
}
