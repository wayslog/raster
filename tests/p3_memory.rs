//! Expose memory four operations,Tombstone option and concurrent update verification.
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::sync::atomic::Ordering;
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Read(u64);
impl Keyed<Schema> for Read {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl ReadOperation<Schema> for Read {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*v.view())
    }
}
struct Put {
    key: u64,
    value: u64,
    append: bool,
    fail: bool,
}
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        if self.append {
            return Ok(UpdateDecision::Append);
        }
        v.view_mut().store(self.value, Ordering::SeqCst);
        if self.fail {
            return Err(Error::Codec("Fault after modification"));
        }
        Ok(UpdateDecision::Updated(self.value))
    }
}
fn put(key: u64, value: u64) -> Put {
    Put {
        key,
        value,
        append: false,
        fail: false,
    }
}
fn store() -> RasterKV<Schema> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap()
}
fn outcome<T: 'static>(s: Submission<T>) -> Outcome<T> {
    match s {
        Submission::Ready(Ok(o)) => o,
        _ => panic!("Expected synchronization business results"),
    }
}

#[derive(Debug)]
struct UnreadableKey(u8);
impl Keyed<Schema> for UnreadableKey {
    fn key(&self) -> &u64 {
        panic!("Illegal serial numbers must not read user keys")
    }
}
impl ReadOperation<Schema> for UnreadableKey {
    type Output = ();
    fn read(&mut self, _: ValueRead<'_, Schema>) -> Result<(), Error> {
        panic!("Rejecting requests must not execute read callbacks")
    }
}
impl UpsertOperation<Schema> for UnreadableKey {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        panic!("Rejection request must not calculate replacement value")
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        panic!("Rejected requests may not be updated in place")
    }
}
impl RmwOperation<Schema> for UnreadableKey {
    type Output = ();
    fn initial(&mut self) -> Result<(u64, ()), Error> {
        panic!("Rejection request must not calculate initial value")
    }
    fn copy_update(&mut self, _: ValueRead<'_, Schema>) -> Result<(u64, ()), Error> {
        panic!("Rejecting requests may not copy updates")
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        panic!("Rejection of requests cannot be read or rewritten in place.")
    }
}
impl DeleteOperation<Schema> for UnreadableKey {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {
        panic!("Rejection of a request shall not notify completion of deletion")
    }
}

#[test]
fn four_operation_illegal_sequence_numbers_are_rejected_before_the_user_key_and_jump_numbers_are_still_acceptable_afterward()
 {
    let store = store();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        outcome(
            session
                .upsert(Serial(10), put(7, 41))
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(41)
    ));
    for serial in [Serial(0), Serial(9), Serial(10)] {
        let rejected = [
            session.read(serial, UnreadableKey(1), Default::default()),
            session.upsert(serial, UnreadableKey(2)),
            session.rmw(serial, UnreadableKey(3), Default::default()),
            session.delete(serial, UnreadableKey(4), Default::default()),
        ];
        for (ordinal, result) in rejected.into_iter().enumerate() {
            let Err(rejected) = result else {
                panic!("Illegal serial numbers must be rejected before accepted")
            };
            assert!(matches!(
                rejected.reason,
                Error::InvalidState("operation_serial_must_increase_strictly")
            ));
            assert_eq!(usize::from(rejected.request.0), ordinal + 1);
            assert_eq!(session.last_accepted(), Some(Serial(10)));
        }
    }
    assert!(matches!(
        outcome(
            session
                .upsert(Serial(11), put(7, 42))
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(42)
    ));
    assert!(matches!(
        outcome(
            session
                .read(Serial(100), Read(7), Default::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(42)
    ));
    let deadline = || Deadline(std::time::Instant::now() + std::time::Duration::from_secs(2));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn empty_storage_inserts_in_place_update_and_append_replacement_readable() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        outcome(
            s.read(Serial(0), Read(7), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::NotFound
    ));
    for (serial, value, append) in [(1, 10, false), (3, 20, false), (5, 30, true)] {
        let mut p = put(7, value);
        p.append = append;
        assert!(
            matches!(outcome(s.upsert(Serial(serial),p).map_err(|r|r.reason).unwrap()),Outcome::Success(v) if v==value)
        );
        assert!(
            matches!(outcome(s.read(Serial(serial+1),Read(7),ReadOptions::default()).map_err(|r|r.reason).unwrap()),Outcome::Success(v) if v==value)
        );
    }
    assert!(s.upsert(Serial(6), put(7, 99)).is_err());
    assert_eq!(s.last_accepted(), Some(Serial(6)));
    assert!(matches!(
        outcome(
            s.read(Serial(8), Read(7), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(30)
    ));
}
#[test]
fn errors_that_may_be_modified_fail_to_close_and_are_not_repeated() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.upsert(Serial(1), put(1, 1))
        .map_err(|r| r.reason)
        .unwrap();
    let mut p = put(1, 2);
    p.fail = true;
    assert!(matches!(
        s.upsert(Serial(2), p).map_err(|r| r.reason).unwrap(),
        Submission::Ready(Err(OperationError {
            effect: Effect::Unknown,
            ..
        }))
    ));
    assert!(s.upsert(Serial(3), put(1, 3)).is_err());
    assert_eq!(s.last_accepted(), Some(Serial(2)));
    assert!(store.start_session(SessionOptions::default()).is_err());
}
#[test]
fn different_keys_of_the_same_tag_are_read_separately_through_the_public_interface() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.upsert(Serial(1), put(8969, 17))
        .map_err(|r| r.reason)
        .unwrap();
    s.upsert(Serial(2), put(9239, 29))
        .map_err(|r| r.reason)
        .unwrap();
    for (serial, key, expected) in [(3, 8969, 17), (4, 9239, 29)] {
        assert!(
            matches!(outcome(s.read(Serial(serial),Read(key),ReadOptions::default()).map_err(|r|r.reason).unwrap()),Outcome::Success(v) if v==expected)
        );
    }
}
struct Add {
    key: u64,
    delta: u64,
    copy: bool,
}
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.delta, self.delta))
    }
    fn copy_update(&mut self, v: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        let n = v.view().wrapping_add(self.delta);
        Ok((n, n))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        if self.copy {
            return Ok(UpdateDecision::Append);
        }
        Ok(UpdateDecision::Updated(
            v.view_mut()
                .fetch_add(self.delta, Ordering::SeqCst)
                .wrapping_add(self.delta),
        ))
    }
}
struct Delete(u64);
impl Keyed<Schema> for Delete {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl DeleteOperation<Schema> for Delete {
    type Output = DeleteOutcome;
    fn complete(self, outcome: DeleteOutcome) -> DeleteOutcome {
        outcome
    }
}
#[test]
fn four_operation_tombstone_condition_creation_and_copy_update_closed_loop() {
    use raster::api::completion::AbortReason;
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        outcome(
            s.rmw(
                Serial(1),
                Add {
                    key: 1,
                    delta: 4,
                    copy: false
                },
                RmwOptions {
                    create_if_missing: false
                }
            )
            .map_err(|r| r.reason)
            .unwrap()
        ),
        Outcome::NotFound
    ));
    for (serial, delta, copy, expected) in [
        (2, u64::MAX, false, u64::MAX),
        (3, 2, false, 1),
        (4, 5, true, 6),
    ] {
        assert!(
            matches!(outcome(s.rmw(Serial(serial),Add {key:1,delta,copy},RmwOptions::default()).map_err(|r|r.reason).unwrap()),Outcome::Success(n) if n==expected)
        );
    }
    assert!(matches!(
        outcome(
            s.delete(Serial(5), Delete(1), DeleteOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(DeleteOutcome::TombstoneWritten)
    ));
    assert!(matches!(
        outcome(
            s.read(Serial(6), Read(1), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::NotFound
    ));
    assert!(matches!(
        outcome(
            s.read(
                Serial(7),
                Read(1),
                ReadOptions {
                    abort_if_tombstone: true
                }
            )
            .map_err(|r| r.reason)
            .unwrap()
        ),
        Outcome::Aborted(AbortReason::Tombstone)
    ));
    assert!(matches!(
        outcome(
            s.delete(Serial(8), Delete(1), DeleteOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(DeleteOutcome::TombstoneWritten)
    ));
    assert!(matches!(
        outcome(
            s.rmw(
                Serial(9),
                Add {
                    key: 1,
                    delta: 4,
                    copy: false
                },
                RmwOptions {
                    create_if_missing: false
                }
            )
            .map_err(|r| r.reason)
            .unwrap()
        ),
        Outcome::NotFound
    ));
    assert!(matches!(
        outcome(
            s.rmw(
                Serial(10),
                Add {
                    key: 1,
                    delta: 8,
                    copy: false
                },
                RmwOptions::default()
            )
            .map_err(|r| r.reason)
            .unwrap()
        ),
        Outcome::Success(8)
    ));
    s.delete(
        Serial(11),
        Delete(99),
        DeleteOptions {
            force_tombstone: true,
        },
    )
    .map_err(|r| r.reason)
    .unwrap();
    assert!(matches!(
        outcome(
            s.read(
                Serial(12),
                Read(99),
                ReadOptions {
                    abort_if_tombstone: true
                }
            )
            .map_err(|r| r.reason)
            .unwrap()
        ),
        Outcome::Aborted(AbortReason::Tombstone)
    ));
    s.upsert(Serial(13), put(99, 42))
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(
        outcome(
            s.read(Serial(14), Read(99), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(42)
    ));
}
#[test]
fn accumulation_of_the_same_key_across_multiple_sessions_will_not_lose_updates() {
    let store = store();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let store = &store;
            scope.spawn(move || {
                let mut session = store.start_session(SessionOptions::default()).unwrap();
                for serial in 0..100 {
                    let mut request = Add {
                        key: 7,
                        delta: 1,
                        copy: serial % 3 == 0,
                    };
                    loop {
                        match session.rmw(Serial(serial), request, RmwOptions::default()) {
                            Ok(result) => {
                                assert!(matches!(outcome(result), Outcome::Success(_)));
                                break;
                            }
                            Err(rejected) => {
                                assert!(matches!(rejected.reason, Error::Busy));
                                request = rejected.request;
                                std::thread::yield_now();
                            }
                        }
                    }
                }
            });
        }
    });
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        outcome(
            s.read(Serial(0), Read(7), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(400)
    ));
}
