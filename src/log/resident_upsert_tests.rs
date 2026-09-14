use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Weak, mpsc},
    time::Duration,
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;
type Owner = value::PageValue<crate::schema::SharedValue<Schema>>;

struct Put {
    value: u64,
    counts: Option<(Weak<Owner>, Rc<Cell<usize>>)>,
}
impl Put {
    fn plain(value: u64) -> Self {
        Self {
            value,
            counts: None,
        }
    }
}
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        if let Some((owner, count)) = &self.counts {
            count.set(owner.strong_count());
        }
        value
            .view_mut()
            .store(self.value, std::sync::atomic::Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.value))
    }
}
fn store() -> RasterKV<Schema> {
    let mut config = crate::config::Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Put::plain(42)),
        Ok(Submission::Ready(Ok(Outcome::Success(42))))
    ));
    drop(session);
    store
}

#[test]
fn a_warmed_upsert_respects_the_requested_read_only_frontier_before_sealing_finishes() {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Put::plain(43)),
        Ok(Submission::Ready(Ok(Outcome::Success(43))))
    ));
    let old_address = *store
        .inner
        .log
        .records
        .lock()
        .unwrap()
        .keys()
        .next()
        .unwrap();
    let end = store.inner.log.pad_tail().unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (freeze, submission) = std::thread::scope(|scope| {
        let store = &store;
        let holder = scope.spawn(move || {
            store
                .inner
                .log
                .lease(old_address)
                .unwrap()
                .read(|old| {
                    assert_eq!(old, 43);
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let freeze = store.inner.log.advance_read_only(end);
        let submission = session.upsert(Serial(1), Put::plain(44));
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        (freeze, submission)
    });
    assert!(matches!(freeze, Err(Error::Busy)));
    assert_eq!(store.inner.log.frontiers().unwrap().read_only, end);
    assert!(
        matches!(submission, Ok(Submission::Ready(Ok(Outcome::Success(44))))),
        "a requested read-only source must append instead of waiting for an in-place permit"
    );
    assert_eq!(
        store
            .inner
            .log
            .lease(old_address)
            .unwrap()
            .read(|old| old)
            .unwrap(),
        43
    );
}

#[test]
fn resident_upsert_does_not_add_a_record_owner_reference() {
    let store = store();
    let owner = Arc::downgrade(
        store
            .inner
            .log
            .records
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap(),
    );
    let count = Rc::new(Cell::new(0));
    let mut session = store.start_session(Default::default()).unwrap();
    for serial in 0..3 {
        assert!(matches!(
            session.upsert(
                Serial(serial),
                Put {
                    value: 43,
                    counts: Some((owner.clone(), count.clone()))
                }
            ),
            Ok(Submission::Ready(Ok(Outcome::Success(43))))
        ));
        assert_eq!(
            count.get(),
            1,
            "resident Upsert added an owning record reference"
        );
    }
}

#[test]
fn warmed_resident_upsert_does_not_wait_for_the_record_directory_lock() {
    let store = store();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (go_tx, go_rx) = mpsc::sync_channel(1);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    std::thread::scope(|scope| {
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            assert!(matches!(
                session.upsert(Serial(0), Put::plain(43)),
                Ok(Submission::Ready(Ok(Outcome::Success(43))))
            ));
            started_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            let completed = matches!(
                session.upsert(Serial(1), Put::plain(44)),
                Ok(Submission::Ready(Ok(Outcome::Success(44))))
            );
            result_tx.send(completed).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let directory = store.inner.log.records.lock().unwrap();
        go_tx.send(()).unwrap();
        let while_locked = result_rx.recv_timeout(Duration::from_secs(5));
        drop(directory);
        worker.join().unwrap();
        assert_eq!(
            while_locked,
            Ok(true),
            "a warmed Upsert waited for the record directory lock"
        );
    });
}
