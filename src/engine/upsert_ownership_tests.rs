use super::Engine;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Weak, atomic::Ordering},
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;

struct CountUpsert {
    engine: Weak<Engine<Schema>>,
    observed: Rc<Cell<usize>>,
}
impl Keyed<Schema> for CountUpsert {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for CountUpsert {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        self.observed.set(self.engine.strong_count());
        Ok((42, 42))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.observed.set(self.engine.strong_count());
        value.view_mut().store(42, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(42))
    }
}

fn store() -> RasterKV<Schema> {
    let mut config = crate::config::Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap()
}

#[test]
fn synchronous_upsert_borrows_the_engine_for_append_and_in_place_update() {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let engine = Arc::downgrade(&store.inner);
    let existing = engine.strong_count();
    let observed = Rc::new(Cell::new(0));
    let mut counts = Vec::new();
    for serial in 0..2 {
        assert!(matches!(
            session.upsert(
                Serial(serial),
                CountUpsert {
                    engine: engine.clone(),
                    observed: observed.clone(),
                }
            ),
            Ok(Submission::Ready(Ok(Outcome::Success(42))))
        ));
        counts.push(observed.replace(0));
        assert_eq!(engine.strong_count(), existing);
    }
    assert_eq!(counts, vec![existing, existing]);
}

#[test]
fn pending_upsert_owns_the_engine_until_completion_after_store_drop() {
    use crate::schema::KeyCodec;
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    let engine = Arc::downgrade(&store.inner);
    let observed = Rc::new(Cell::new(0));
    assert!(matches!(
        session.upsert(
            Serial(0),
            CountUpsert {
                engine: engine.clone(),
                observed: observed.clone(),
            }
        ),
        Ok(Submission::Ready(Ok(Outcome::Success(42))))
    ));
    observed.set(0);
    let entry = store.inner.index.prepare(U64Key.hash(&7)).unwrap();
    let crate::index::IndexHead::Log(address) = entry.head else {
        panic!("missing resident record")
    };
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
        let submission = session.upsert(
            Serial(1),
            CountUpsert {
                engine: engine.clone(),
                observed: observed.clone(),
            },
        );
        let retained = engine.strong_count();
        let called_while_locked = observed.get();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let Submission::Pending(mut ticket) = submission.map_err(|error| error.reason).unwrap()
        else {
            panic!("held value must suspend Upsert")
        };
        assert_eq!(retained, existing + 1);
        assert_eq!(called_while_locked, 0);
        drop(store);
        assert!(matches!(
            session
                .wait(
                    &mut ticket,
                    Deadline(Instant::now() + Duration::from_secs(5))
                )
                .unwrap(),
            Ok(Outcome::Success(42))
        ));
        assert_eq!(observed.get(), existing);
        assert_eq!(engine.strong_count(), existing - 1);
    });
    drop(session);
    assert_eq!(engine.strong_count(), 0);
}
