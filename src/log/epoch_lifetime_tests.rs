use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    config::Config,
    schema::{
        KeyCodec, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Put;
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((42, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn engine_epoch_alone_must_retain_a_retired_record_before_borrowed_access_is_safe() {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    let guard = store
        .inner
        .epoch
        .enter_records(session.participant.as_ref().unwrap())
        .unwrap();
    let entry = store.inner.index.prepare(U64Key.hash(&7)).unwrap();
    let crate::index::IndexHead::Log(address) = entry.head else {
        panic!("missing record")
    };
    let lease = store.inner.log.lease(address).unwrap();
    let weak = Arc::downgrade(&lease.value);
    drop(lease);
    assert!(weak.upgrade().is_some());
    store
        .inner
        .index
        .compare_publish(entry, crate::index::IndexHead::Empty)
        .unwrap();
    assert_eq!(
        store.inner.index.prepare(U64Key.hash(&7)).unwrap().head,
        crate::index::IndexHead::Empty
    );
    store.inner.log.retire(address).unwrap();
    assert!(
        weak.upgrade().is_some(),
        "an active engine epoch does not retain the retired PageValue owner"
    );
    drop(guard);
    assert_eq!(store.inner.log.collect_retired().unwrap(), 1);
    assert!(weak.upgrade().is_none());
}

struct CountRead {
    owner: std::sync::Weak<value::PageValue<crate::schema::SharedValue<Schema>>>,
    count: std::rc::Rc<std::cell::Cell<usize>>,
}
impl Keyed<Schema> for CountRead {
    fn key(&self) -> &u64 {
        &7
    }
}
impl ReadOperation<Schema> for CountRead {
    type Output = u64;
    fn read(&mut self, value: crate::schema::ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.count.set(self.owner.strong_count());
        Ok(*value.view())
    }
}
#[test]
fn public_resident_reads_do_not_add_a_record_owner_reference() {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
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
    let count = std::rc::Rc::new(std::cell::Cell::new(0));
    for serial in 1..=3 {
        assert!(matches!(
            session.read(
                Serial(serial),
                CountRead {
                    owner: owner.clone(),
                    count: count.clone()
                },
                ReadOptions::default()
            ),
            Ok(Submission::Ready(Ok(Outcome::Success(42))))
        ));
        assert_eq!(count.get(), 1, "resident Read added an owning reference");
    }
}

impl ReadOperation<Schema> for Put {
    type Output = u64;
    fn read(&mut self, value: crate::schema::ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
#[test]
fn warmed_public_read_does_not_wait_for_the_record_directory_lock() {
    let mut config = Config::default();
    config.index.buckets = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut preload = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        preload.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    drop(preload);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            assert!(matches!(
                session.read(Serial(0), Put, ReadOptions::default()),
                Ok(Submission::Ready(Ok(Outcome::Success(42))))
            ));
            started_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            let value = match session.read(Serial(1), Put, ReadOptions::default()) {
                Ok(Submission::Ready(Ok(Outcome::Success(value)))) => value,
                _ => panic!("Expected a completed resident read"),
            };
            result_tx.send(value).unwrap();
        });
        started_rx.recv().unwrap();
        let state = store.inner.log.records.lock().unwrap();
        go_tx.send(()).unwrap();
        let while_locked = result_rx.recv_timeout(std::time::Duration::from_secs(5));
        drop(state);
        worker.join().unwrap();
        assert_eq!(
            while_locked,
            Ok(42),
            "A warmed Read waited for the record directory lock"
        );
    });
}

#[test]
fn closing_a_retained_session_releases_its_hint_owner() {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(_)))
    ));
    assert_eq!(Arc::weak_count(&store.inner.log.state), 0);
    assert!(matches!(
        session.read(Serial(1), Put, ReadOptions::default()),
        Ok(Submission::Ready(Ok(_)))
    ));
    assert_eq!(Arc::weak_count(&store.inner.log.state), 1);
    session
        .close(Deadline(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        ))
        .unwrap();
    assert_eq!(Arc::weak_count(&store.inner.log.state), 0);
    assert!(session.participant.is_none());
}

#[test]
fn warmed_public_read_does_not_wait_for_the_log_boundary_lock() {
    let mut config = Config::default();
    config.index.buckets = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut preload = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        preload.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    drop(preload);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let store = &store;
        let worker = scope.spawn(move || {
            let mut session = store.start_session(Default::default()).unwrap();
            assert!(matches!(
                session.read(Serial(0), Put, ReadOptions::default()),
                Ok(Submission::Ready(Ok(Outcome::Success(42))))
            ));
            started_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            let value = match session.read(Serial(1), Put, ReadOptions::default()) {
                Ok(Submission::Ready(Ok(Outcome::Success(value)))) => value,
                _ => panic!("Expected a completed resident read"),
            };
            result_tx.send(value).unwrap();
        });
        started_rx.recv().unwrap();
        let state = store.inner.log.state.write().unwrap();
        go_tx.send(()).unwrap();
        let while_locked = result_rx.recv_timeout(std::time::Duration::from_secs(5));
        drop(state);
        worker.join().unwrap();
        assert_eq!(
            while_locked,
            Ok(42),
            "A warmed Read waited for the log boundary lock"
        );
    });
}
