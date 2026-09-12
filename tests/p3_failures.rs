//! reject before accepting,callback/Exposed bounds on destructor panics and error effects.
use raster::{
    RasterKV, Submission,
    api::{operation::*, session::SessionOptions},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{cell::Cell, rc::Rc};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Request {
    key: u64,
    keys: Rc<Cell<usize>>,
    panic_key: bool,
    panic_drop: bool,
}
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        self.keys.set(self.keys.get() + 1);
        assert!(!self.panic_key, "key panic");
        &self.key
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        assert!(!self.panic_drop, "Destruction panic");
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*v.view())
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((9, 9))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn request() -> Request {
    Request {
        key: 1,
        keys: Rc::new(Cell::new(0)),
        panic_key: false,
        panic_drop: false,
    }
}
fn store() -> RasterKV<Schema> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap()
}
#[test]
fn serial_number_rejection_does_not_call_the_key_method_and_returns_the_original_request() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.read(Serial(7), request(), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    let r = request();
    let counter = r.keys.clone();
    let rejected = match s.read(Serial(7), r, ReadOptions::default()) {
        Err(r) => r,
        Ok(_) => panic!("Duplicate serial numbers should be rejected"),
    };
    assert_eq!(counter.get(), 0);
    assert!(Rc::ptr_eq(&counter, &rejected.request.keys));
    assert_eq!(s.last_accepted(), Some(Serial(7)));
}
#[test]
fn key_panic_rejection_does_not_consume_the_serial_number_and_shuts_down_the_engine() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    let mut r = request();
    r.panic_key = true;
    assert!(s.read(Serial(1), r, ReadOptions::default()).is_err());
    assert_eq!(s.last_accepted(), None);
    assert!(store.start_session(SessionOptions::default()).is_err());
}
#[test]
fn after_taking_effect_the_requested_destructor_panic_returns_that_it_has_taken_effect_and_will_not_be_expanded_to_the_caller()
 {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    let mut r = request();
    r.panic_drop = true;
    assert!(matches!(
        s.upsert(Serial(1), r).map_err(|r| r.reason).unwrap(),
        Submission::Ready(Err(OperationError {
            effect: Effect::Applied,
            ..
        }))
    ));
    assert_eq!(s.last_accepted(), Some(Serial(1)));
    assert!(store.start_session(SessionOptions::default()).is_err());
}
#[test]
fn synchronous_read_destructor_panic_returns_failure_and_closes_the_engine() {
    let store = store();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    session
        .upsert(Serial(0), request())
        .map_err(|r| r.reason)
        .unwrap();
    let mut read = request();
    read.panic_drop = true;
    assert!(matches!(
        session
            .read(Serial(1), read, ReadOptions::default())
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Err(OperationError {
            effect: Effect::NotApplied,
            ..
        }))
    ));
    assert_eq!(session.last_accepted(), Some(Serial(1)));
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    assert!(
        session
            .read(Serial(2), request(), ReadOptions::default())
            .is_err()
    );
    assert_eq!(session.last_accepted(), Some(Serial(1)));
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stage {
    None,
    Read,
    Replacement,
    Initial,
    Copy,
    Update,
    Delete,
}
struct Fault {
    stage: Stage,
    panic: bool,
    hits: Rc<Cell<usize>>,
}
impl Fault {
    fn hit(&self, stage: Stage) -> Result<(), Error> {
        if self.stage == stage {
            self.hits.set(self.hits.get() + 1);
            assert!(!self.panic, "stage panic:{stage:?}");
            return Err(Error::Codec("phase error"));
        }
        Ok(())
    }
}
impl Keyed<Schema> for Fault {
    fn key(&self) -> &u64 {
        &1
    }
}
impl ReadOperation<Schema> for Fault {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.hit(Stage::Read)?;
        Ok(*v.view())
    }
}
impl UpsertOperation<Schema> for Fault {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        self.hit(Stage::Replacement)?;
        Ok((1, 1))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        if self.stage == Stage::Update {
            v.view_mut().store(2, std::sync::atomic::Ordering::SeqCst);
            self.hit(Stage::Update)?;
        }
        Ok(UpdateDecision::Append)
    }
}
impl RmwOperation<Schema> for Fault {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        self.hit(Stage::Initial)?;
        Ok((1, 1))
    }
    fn copy_update(&mut self, v: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.hit(Stage::Copy)?;
        let n = v.view() + 1;
        Ok((n, n))
    }
    fn update_in_place(
        &mut self,
        v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        UpsertOperation::update_in_place(self, v)
    }
}
impl DeleteOperation<Schema> for Fault {
    type Output = u64;
    fn complete(self, _: DeleteOutcome) -> u64 {
        self.hit(Stage::Delete).unwrap();
        1
    }
}
#[test]
fn matrix_of_the_impact_of_errors_and_panics_at_each_stage_and_the_number_of_callbacks() {
    for stage in [
        Stage::Read,
        Stage::Replacement,
        Stage::Initial,
        Stage::Copy,
        Stage::Update,
        Stage::Delete,
    ] {
        for panic in [false, true] {
            if stage == Stage::Delete && !panic {
                continue;
            }
            let store = store();
            let mut s = store.start_session(SessionOptions::default()).unwrap();
            let hits = Rc::new(Cell::new(0));
            if stage != Stage::Initial {
                s.upsert(
                    Serial(0),
                    Fault {
                        stage: Stage::None,
                        panic: false,
                        hits: hits.clone(),
                    },
                )
                .map_err(|r| r.reason)
                .unwrap();
            }
            let request = Fault {
                stage,
                panic,
                hits: hits.clone(),
            };
            let submission = match stage {
                Stage::Read => s.read(Serial(1), request, ReadOptions::default()),
                Stage::Replacement => s.upsert(Serial(1), request),
                Stage::Delete => s.delete(Serial(1), request, DeleteOptions::default()),
                _ => s.rmw(Serial(1), request, RmwOptions::default()),
            }
            .map_err(|r| r.reason)
            .unwrap();
            let Submission::Ready(Err(error)) = submission else {
                panic!("Stage should end with error:{stage:?}")
            };
            let expected = match stage {
                Stage::Update => Effect::Unknown,
                Stage::Delete => Effect::Applied,
                _ => Effect::NotApplied,
            };
            assert_eq!(error.effect, expected, "{stage:?} panic={panic}");
            assert_eq!(hits.get(), 1);
            assert_eq!(s.last_accepted(), Some(Serial(1)));
            if panic || stage == Stage::Update {
                assert!(store.start_session(SessionOptions::default()).is_err());
                assert!(
                    s.read(
                        Serial(2),
                        Fault {
                            stage: Stage::None,
                            panic: false,
                            hits: hits.clone()
                        },
                        ReadOptions::default()
                    )
                    .is_err()
                );
            } else {
                let output = s
                    .read(
                        Serial(2),
                        Fault {
                            stage: Stage::None,
                            panic: false,
                            hits: hits.clone(),
                        },
                        ReadOptions::default(),
                    )
                    .map_err(|r| r.reason)
                    .unwrap();
                if stage == Stage::Initial {
                    assert!(matches!(
                        output,
                        Submission::Ready(Ok(raster::api::completion::Outcome::NotFound))
                    ));
                } else {
                    assert!(matches!(
                        output,
                        Submission::Ready(Ok(raster::api::completion::Outcome::Success(1)))
                    ));
                }
            }
            assert_eq!(hits.get(), 1);
        }
    }
}
struct LocalOutput(Rc<String>);
impl Keyed<Schema> for LocalOutput {
    fn key(&self) -> &u64 {
        &1
    }
}
impl ReadOperation<Schema> for LocalOutput {
    type Output = Rc<String>;
    fn read(&mut self, _: ValueRead<'_, Schema>) -> Result<Rc<String>, Error> {
        Ok(self.0.clone())
    }
}
#[test]
fn non_sending_context_and_output_remain_in_the_calling_thread_via_real_reads() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.upsert(Serial(0), request())
        .map_err(|r| r.reason)
        .unwrap();
    let output = Rc::new("Results of this thread".to_string());
    let Submission::Ready(Ok(raster::api::completion::Outcome::Success(result))) = s
        .read(
            Serial(1),
            LocalOutput(output.clone()),
            ReadOptions::default(),
        )
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("Expected read success")
    };
    assert!(Rc::ptr_eq(&output, &result));
}
