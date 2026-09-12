use super::*;
use crate::{
    RasterKV,
    api::operation::Keyed,
    schema::builtin::{AtomicU64Value, SchemaPair, U64Key},
};
use std::{cell::RefCell, rc::Rc};
type SchemaType = SchemaPair<U64Key, AtomicU64Value>;
struct Probe {
    store: RasterKV<SchemaType>,
    observations: Rc<RefCell<Vec<usize>>>,
    value: u64,
}
impl Keyed<SchemaType> for Probe {
    fn key(&self) -> &u64 {
        &1
    }
}
impl Probe {
    fn observe(&self) {
        self.observations
            .borrow_mut()
            .push(Arc::strong_count(&self.store.inner));
    }
}
impl UpsertOperation<SchemaType> for Probe {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        self.observe();
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, SchemaType>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.observe();
        value.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.value))
    }
}
#[test]
fn ready_upsert_borrows_the_engine_for_append_and_update_callbacks() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let observations = Rc::new(RefCell::new(Vec::new()));
    let mut expected = Vec::new();
    for (serial, value) in [7, 42, u64::MAX].into_iter().enumerate() {
        let request = Probe {
            store: store.clone(),
            observations: observations.clone(),
            value,
        };
        expected.push(Arc::strong_count(&store.inner));
        assert!(
            matches!(session.upsert(Serial(serial as u64), request).map_err(|r|r.reason).unwrap(), Submission::Ready(Ok(Outcome::Success(v))) if v==value)
        );
    }
    assert_eq!(*observations.borrow(), expected);
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    assert_eq!(store.diagnostics().unwrap().pending_requests, 0);
}

#[test]
fn pending_upsert_keeps_engine_ownership_without_cloning_it_during_progress() {
    use crate::schema::KeyCodec;
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let observations = Rc::new(RefCell::new(Vec::new()));
    let request = |value| Probe {
        store: store.clone(),
        observations: observations.clone(),
        value,
    };
    assert!(matches!(
        session
            .upsert(Serial(0), request(7))
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(7)))
    ));
    observations.borrow_mut().clear();
    let crate::index::IndexHead::Log(address) =
        store.inner.index.prepare(U64Key.hash(&1)).unwrap().head
    else {
        panic!("Expected a resident record")
    };
    let submission = std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let holder = scope.spawn(move || {
            owner
                .inner
                .log
                .lease(address)
                .unwrap()
                .read(|_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let submission = session.upsert(Serial(1), request(42));
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        submission
    });
    let Submission::Pending(mut ticket) = submission.map_err(|r| r.reason).unwrap() else {
        panic!("Value permission contention must suspend the operation")
    };
    assert!(observations.borrow().is_empty());
    let owners = Arc::strong_count(&store.inner);
    assert!(matches!(
        session
            .wait(
                &mut ticket,
                Deadline(Instant::now() + Duration::from_secs(5))
            )
            .unwrap()
            .unwrap(),
        Outcome::Success(42)
    ));
    assert_eq!(*observations.borrow(), vec![owners]);
    assert!(ticket.try_take().is_err());
    session.poll(PollBudget::default()).unwrap();
    assert_eq!(observations.borrow().len(), 1);
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    assert_eq!(store.diagnostics().unwrap().pending_requests, 0);
}
