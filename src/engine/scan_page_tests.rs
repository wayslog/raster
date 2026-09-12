//! Native device page reading access scan cursor;Not yet a replacement for public scanner buffering and lifecycle acceptance.
use super::*;
use crate::{
    api::completion::TicketState, engine::io_hub::CompletionHub, log::read_page::PageRead,
};

#[test]
fn native_disk_page_scan_output_is_independent_and_shared_polling_does_not_execute_another_session_callback()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    wait(&mut session, &checkpoint);
    let frontiers = store.inner.log.frontiers().unwrap();
    assert!(frontiers.head > frontiers.begin);
    assert_eq!(frontiers.tail.0 % 4096, 0);
    let Submission::Pending(mut user_read) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("Cold page user reads must be suspended")
    };
    let id = store
        .inner
        .io
        .reserve(SessionId::generate().unwrap())
        .unwrap();
    let mut outputs = Vec::new();
    for number in frontiers.begin.0 / 4096..frontiers.tail.0 / 4096 {
        let page = PageId(number);
        let mut read = PageRead::new(page, 4096, CompletionHub::route(id)).unwrap();
        let stop = Instant::now() + Duration::from_secs(10);
        let page = loop {
            assert!(Instant::now() < stop, "disk scan read timeout");
            match read.submit_next(&store.inner.storage) {
                Ok(_) | Err(Error::Busy) => {}
                Err(error) => panic!("Submit scan read failed:{error:?}"),
            }
            store
                .inner
                .io
                .poll(&*store.inner.storage.device, PollBudget::default())
                .unwrap();
            if let Some(completion) = store.inner.io.take(id).unwrap() {
                read.accept(&store.inner.storage, completion).unwrap();
            }
            if let Some(page) = read.finish(&store.inner.storage).unwrap() {
                break page;
            }
            std::thread::yield_now();
        };
        assert!(!read.has_inflight());
        let mut cursor = page
            .into_scan(LogAddress(number * 4096), LogAddress((number + 1) * 4096))
            .unwrap();
        while let Some(record) = cursor.next_record(&*store.inner.schema).unwrap() {
            assert!(!record.invalid && !record.tombstone);
            outputs.push(record);
        }
    }
    store.inner.io.release(id).unwrap();
    // Other routes have been polled,But the user context must be advanced by its own session.
    assert!(matches!(
        user_read.try_take().unwrap(),
        TicketState::Pending
    ));
    assert!(matches!(
        session.wait(&mut user_read, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    assert_eq!(outputs.len(), 400);
    for (key, record) in outputs.iter().enumerate() {
        assert_eq!(record.key, key as u64);
        assert_eq!(record.value, Some(key as u64));
        if key > 0 {
            assert!(record.address > outputs[key - 1].address);
        }
    }
}

#[test]
fn expose_the_three_modes_to_complete_the_traversal_and_release_the_scanning_quota_after_the_end() {
    use crate::api::scan::{Buffering, ScanOptions};
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    session.close(deadline()).unwrap();
    let mut outputs = Vec::new();
    for mode in [
        Buffering::Unbuffered,
        Buffering::SinglePage,
        Buffering::DoublePage,
    ] {
        let mut scanner = store
            .scan(ScanOptions {
                begin: report.begin,
                end: report.end,
                buffering: mode,
            })
            .unwrap();
        assert!(matches!(store.shutdown(deadline()), Err(Error::Busy)));
        let mut values = Vec::new();
        while let Some(record) = scanner.next_record().unwrap() {
            values.push((record.address, record.key, record.value));
        }
        assert!(scanner.next_record().unwrap().is_none());
        scanner.close().unwrap();
        assert_eq!(values.len(), 400);
        for (key, (_, actual, value)) in values.iter().enumerate() {
            assert_eq!(*actual, key as u64);
            assert_eq!(*value, Some(key as u64));
        }
        outputs.push(values);
    }
    assert_eq!(outputs[0], outputs[1]);
    assert_eq!(outputs[1], outputs[2]);
    store.shutdown(deadline()).unwrap();
}

#[test]
fn public_disk_scan_retains_the_invalid_record_flag_and_omits_its_value() {
    use crate::api::scan::{Buffering, ScanOptions};
    use std::io::{Seek, SeekFrom, Write};
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    session.close(deadline()).unwrap();
    assert!(store.inner.log.frontiers().unwrap().head > LogAddress(0));
    let path = store
        .inner
        .config
        .storage
        .root
        .join(store.inner.storage.segment_path(0, Generation(0)));
    let bytes = std::fs::read(&path).unwrap();
    let size = PageFrame::encoded_size(4096).unwrap();
    let frame = PageFrame::decode(&bytes[..size], PageId(0), 4096).unwrap();
    let mut payload = frame.payload.to_vec();
    let (address, mut record) = frame.records().unwrap().remove(0);
    record.header.invalid = true;
    let offset = address.0 as usize;
    let length = record.header.encoded_len().unwrap();
    record
        .encode(&mut payload[offset..offset + length])
        .unwrap();
    let changed = PageFrame {
        page: PageId(0),
        version: frame.version,
        payload: &payload,
    }
    .encode()
    .unwrap();
    // Inject a verified invalid Physics slot;Immutable checkpoint material is not modified.
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&changed).unwrap();
    drop(file);
    let mut scanner = store
        .scan(ScanOptions {
            begin: report.begin,
            end: report.end,
            buffering: Buffering::DoublePage,
        })
        .unwrap();
    let first = scanner.next_record().unwrap().unwrap();
    assert_eq!(first.key, 0);
    assert!(first.invalid && !first.tombstone);
    assert!(first.value.is_none());
    let second = scanner.next_record().unwrap().unwrap();
    assert_eq!(second.key, 1);
    assert_eq!(second.value, Some(1));
    scanner.close().unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn public_read_ahead_and_pending_users_read_shared_routes_and_close_without_swallowing_user_completion()
 {
    use crate::api::scan::{Buffering, ScanOptions};
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let frontiers = store.inner.log.frontiers().unwrap();
    let Submission::Pending(mut ticket) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("Requires cold page reads");
    };
    let mut scan = store
        .scan(ScanOptions {
            begin: frontiers.begin,
            end: frontiers.tail,
            buffering: Buffering::DoublePage,
        })
        .unwrap();
    assert_eq!(scan.next_record().unwrap().unwrap().key, 0);
    assert!(matches!(ticket.try_take().unwrap(), TicketState::Pending));
    scan.close().unwrap();
    assert!(matches!(
        session.wait(&mut ticket, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
