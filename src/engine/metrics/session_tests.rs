use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Probe {
    store: RasterKV<Schema>,
    observations: Arc<Mutex<Vec<(usize, usize, usize)>>>,
    value: u64,
}
impl Probe {
    fn observe(&self) {
        let observed = self.store.diagnostics().unwrap();
        self.observations.lock().unwrap().push((
            observed.active_requests,
            observed.pending_requests,
            Arc::strong_count(&self.store.inner.metrics),
        ));
    }
}
impl Keyed<Schema> for Probe {
    fn key(&self) -> &u64 {
        &7
    }
}
impl ReadOperation<Schema> for Probe {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.observe();
        Ok(*value.view())
    }
}
impl UpsertOperation<Schema> for Probe {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        self.observe();
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.observe();
        value.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.value))
    }
}
impl RmwOperation<Schema> for Probe {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        self.observe();
        Ok((1, 1))
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.observe();
        let next = value.view().wrapping_add(1);
        Ok((next, next))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.observe();
        let next = value
            .view_mut()
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        Ok(UpdateDecision::Updated(next))
    }
}
impl DeleteOperation<Schema> for Probe {
    type Output = u64;
    fn complete(self, _: DeleteOutcome) -> u64 {
        self.observe();
        1
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}
#[test]
fn actual_operations_keep_resource_counts_without_cloning_shared_metrics_ownership() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let owners = Arc::strong_count(&store.inner.metrics);
    let observations = Arc::new(Mutex::new(Vec::new()));
    let request = |value| Probe {
        store: store.clone(),
        observations: observations.clone(),
        value,
    };
    assert!(matches!(
        session.upsert(Serial(0), request(7)),
        Ok(Submission::Ready(Ok(Outcome::Success(7))))
    ));
    assert!(matches!(
        session.read(Serial(1), request(0), ReadOptions::default()),
        Ok(Submission::Ready(Ok(Outcome::Success(7))))
    ));
    assert!(matches!(
        session.upsert(Serial(2), request(11)),
        Ok(Submission::Ready(Ok(Outcome::Success(11))))
    ));
    assert!(matches!(
        session.rmw(Serial(3), request(0), RmwOptions::default()),
        Ok(Submission::Ready(Ok(Outcome::Success(12))))
    ));
    assert!(matches!(
        session.delete(Serial(4), request(0), DeleteOptions::default()),
        Ok(Submission::Ready(Ok(Outcome::Success(1))))
    ));
    assert_eq!(session.last_accepted(), Some(Serial(4)));
    let final_counts = store.diagnostics().unwrap();
    assert_eq!(
        (final_counts.active_requests, final_counts.pending_requests),
        (0, 0)
    );
    assert!(!store.statistics().enabled);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    assert_eq!(*observations.lock().unwrap(), vec![(1, 0, owners); 5]);
}
