use super::Engine;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        ValueRead,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Weak},
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
    fn update_in_place(
        &mut self,
        _: crate::schema::ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}

struct CountRmw {
    engine: Weak<Engine<Schema>>,
    observed: Rc<Cell<usize>>,
}
impl Keyed<Schema> for CountRmw {
    fn key(&self) -> &u64 {
        &7
    }
}
impl RmwOperation<Schema> for CountRmw {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        Err(Error::InvalidState("Expected a preloaded value"))
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.observed.set(self.engine.strong_count());
        let next = value.view().wrapping_add(1);
        Ok((next, next))
    }
    fn update_in_place(
        &mut self,
        mut value: crate::schema::ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.observed.set(self.engine.strong_count());
        let next = value
            .view_mut()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .wrapping_add(1);
        Ok(UpdateDecision::Updated(next))
    }
}

#[test]
fn synchronous_rmw_borrows_the_existing_engine_owner() {
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
        session.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    let engine = Arc::downgrade(&store.inner);
    let observed = Rc::new(Cell::new(0));
    let existing = engine.strong_count();
    for serial in 1..=3 {
        assert!(matches!(
            session.rmw(
                Serial(serial),
                CountRmw {
                    engine: engine.clone(),
                    observed: observed.clone(),
                },
                RmwOptions::default()
            ),
            Ok(Submission::Ready(Ok(Outcome::Success(value)))) if value == 42 + serial
        ));
        assert_eq!(
            observed.get(),
            existing,
            "synchronous RMW cloned Engine ownership"
        );
        assert_eq!(engine.strong_count(), existing);
    }
}

#[test]
fn pending_rmw_owns_the_engine_until_completion_after_store_drop() {
    use crate::schema::KeyCodec;
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
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
        session.upsert(Serial(0), Put),
        Ok(Submission::Ready(Ok(Outcome::Success(()))))
    ));
    let entry = store.inner.index.prepare(U64Key.hash(&7)).unwrap();
    let crate::index::IndexHead::Log(address) = entry.head else {
        panic!("missing resident record")
    };
    let engine = Arc::downgrade(&store.inner);
    let observed = Rc::new(Cell::new(0));
    let existing = engine.strong_count();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let holder_engine = store.inner.clone();
    std::thread::scope(|scope| {
        let holder = scope.spawn(move || {
            let lease = holder_engine.log.lease(address).unwrap();
            drop(holder_engine);
            lease
                .read(|_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let submission = session.rmw(
            Serial(1),
            CountRmw {
                engine: engine.clone(),
                observed: observed.clone(),
            },
            RmwOptions::default(),
        );
        let retained = engine.strong_count();
        let called_while_locked = observed.get();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let Submission::Pending(mut ticket) = submission.map_err(|error| error.reason).unwrap()
        else {
            panic!("held value must suspend RMW")
        };
        assert_eq!(
            retained,
            existing + 1,
            "pending RMW did not acquire independent ownership"
        );
        assert_eq!(called_while_locked, 0);
        drop(store);
        assert!(matches!(
            session
                .wait(
                    &mut ticket,
                    Deadline(Instant::now() + Duration::from_secs(5))
                )
                .unwrap(),
            Ok(Outcome::Success(43))
        ));
        assert_eq!(observed.get(), existing);
        assert_eq!(engine.strong_count(), existing - 1);
    });
    drop(session);
    assert_eq!(
        engine.strong_count(),
        0,
        "completed RMW leaked Engine ownership"
    );
}
