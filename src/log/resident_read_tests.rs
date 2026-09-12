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
    cell::RefCell,
    rc::Rc,
    sync::{Arc, atomic::Ordering},
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Probe {
    store: RasterKV<Schema>,
    observed: Rc<RefCell<Vec<(usize, usize)>>>,
    value: u64,
}
impl Keyed<Schema> for Probe {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for Probe {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.value, ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        value.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(()))
    }
}
impl ReadOperation<Schema> for Probe {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.observed.borrow_mut().push((
            Arc::strong_count(&self.store.inner.log.state),
            Arc::strong_count(&self.store.inner.storage.identity),
        ));
        Ok(*value.view())
    }
}
#[test]
fn resident_head_read_avoids_query_ownership_and_observes_updates() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let observed = Rc::new(RefCell::new(Vec::new()));
    let request = |value| Probe {
        store: store.clone(),
        observed: observed.clone(),
        value,
    };
    let owners = (
        Arc::strong_count(&store.inner.log.state),
        Arc::strong_count(&store.inner.storage.identity),
    );
    for (n, value) in [7, 42, u64::MAX].into_iter().enumerate() {
        assert!(matches!(
            session
                .upsert(Serial(n as u64 * 2), request(value))
                .map_err(|r| r.reason)
                .unwrap(),
            Submission::Ready(Ok(Outcome::Success(())))
        ));
        assert!(
            matches!(session.read(Serial(n as u64 * 2 + 1), request(0), ReadOptions::default()).map_err(|r| r.reason).unwrap(), Submission::Ready(Ok(Outcome::Success(v))) if v == value)
        );
    }
    assert_eq!(*observed.borrow(), vec![owners; 3]);
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    assert_eq!(store.diagnostics().unwrap().pending_requests, 0);
}
