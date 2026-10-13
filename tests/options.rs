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
