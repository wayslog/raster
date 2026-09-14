use super::*;
use crate::device::{IoOperation, IoRequest, memory::MemoryDevice};
mod synchronous_operations {
    use super::*;
    use crate::{
        RasterKV, Submission,
        api::{Outcome, operation::*, session::SessionOptions},
        schema::{
            ValueRead, ValueUpdate,
            builtin::{AtomicU64Value, SchemaPair, U64Key},
        },
    };
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    type Schema = SchemaPair<U64Key, AtomicU64Value>;
    struct Checked {
        key: u64,
        value: u64,
        hub: Arc<CompletionHub>,
        callbacks: Arc<AtomicUsize>,
    }
    impl Checked {
        fn observe(&self) {
            assert_eq!(self.hub.state.lock().unwrap().mailboxes.len(), 0);
            assert_eq!(self.hub.occupied.load(Ordering::Acquire), 1);
            self.callbacks.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Keyed<Schema> for Checked {
        fn key(&self) -> &u64 {
            &self.key
        }
    }
    impl UpsertOperation<Schema> for Checked {
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
    impl ReadOperation<Schema> for Checked {
        type Output = u64;
        fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
            self.observe();
            Ok(*value.view())
        }
    }
    impl RmwOperation<Schema> for Checked {
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
    impl DeleteOperation<Schema> for Checked {
        type Output = ();
        fn complete(self, _: DeleteOutcome) {
            self.observe();
        }
    }
    fn ready<T>(submission: Submission<T>) -> T {
        match submission {
            Submission::Ready(Ok(Outcome::Success(output))) => output,
            _ => panic!("expected synchronous success"),
        }
    }
    #[test]
    fn warmed_public_reads_reuse_an_unpublished_reservation() {
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .device(Box::new(crate::device::null::NullDeviceFactory))
            .create()
            .unwrap();
        let hub = store.inner.io.clone();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let request = || Checked {
            key: 1,
            value: 7,
            hub: hub.clone(),
            callbacks: callbacks.clone(),
        };
        let mut session = store.start_session(SessionOptions::default()).unwrap();
        ready(
            session
                .upsert(Serial(0), request())
                .map_err(|e| e.reason)
                .unwrap(),
        );
        for serial in 1..=32 {
            assert_eq!(
                ready(
                    session
                        .read(Serial(serial), request(), ReadOptions::default())
                        .map_err(|e| e.reason)
                        .unwrap()
                ),
                7
            );
        }
        assert_eq!(callbacks.load(Ordering::Acquire), 33);
        assert_eq!(hub.next.load(Ordering::Acquire), 1);
        assert_eq!(
            hub.capacity_acquisitions.load(Ordering::Acquire),
            1,
            "warmed Ready operations repeatedly acquire global capacity"
        );
        assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
        session
            .close(Deadline(Instant::now() + Duration::from_secs(5)))
            .unwrap();
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    }
    struct PlainRead;
    impl Keyed<Schema> for PlainRead {
        fn key(&self) -> &u64 {
            &1
        }
    }
    impl ReadOperation<Schema> for PlainRead {
        type Output = u64;
        fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
            Ok(*value.view())
        }
    }
    fn warmed_store() -> (RasterKV<Schema>, crate::Session<Schema>) {
        let mut config = crate::config::Config::default();
        config.session.max_sessions = 1;
        config.session.max_pending = 1;
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(crate::device::null::NullDeviceFactory))
            .create()
            .unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        ready(
            session
                .upsert(
                    Serial(0),
                    Checked {
                        key: 1,
                        value: 7,
                        hub: store.inner.io.clone(),
                        callbacks: Arc::new(AtomicUsize::new(0)),
                    },
                )
                .map_err(|e| e.reason)
                .unwrap(),
        );
        (store, session)
    }
    #[test]
    fn a_reused_credit_transfers_to_pending_and_is_released_after_completion() {
        use crate::schema::KeyCodec;
        let (store, mut session) = warmed_store();
        let hub = &store.inner.io;
        let entry = store.inner.index.prepare(U64Key.hash(&1)).unwrap();
        let crate::index::IndexHead::Log(address) = entry.head else {
            panic!("missing record")
        };
        let mut ticket = std::thread::scope(|scope| {
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
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
            let submission = session
                .read(Serial(1), PlainRead, ReadOptions::default())
                .map_err(|e| e.reason)
                .unwrap();
            let Submission::Pending(ticket) = submission else {
                panic!("expected Pending")
            };
            assert_eq!(ticket.id().slot, 0);
            assert!(session.runtime.reusable_route.is_none());
            assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
            assert_eq!(hub.capacity_acquisitions.load(Ordering::Acquire), 1);
            assert!(matches!(
                session.read(Serial(2), PlainRead, ReadOptions::default()),
                Err(Rejected {
                    reason: Error::Busy,
                    ..
                })
            ));
            assert_eq!(hub.next.load(Ordering::Acquire), 1);
            release_tx.send(()).unwrap();
            holder.join().unwrap();
            ticket
        });
        assert!(matches!(
            session
                .wait(
                    &mut ticket,
                    Deadline(Instant::now() + Duration::from_secs(5))
                )
                .unwrap()
                .unwrap(),
            Outcome::Success(7)
        ));
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
        assert_eq!(
            ready(
                session
                    .read(Serial(2), PlainRead, ReadOptions::default())
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            7
        );
        assert_eq!(hub.next.load(Ordering::Acquire), 2);
        assert_eq!(hub.capacity_acquisitions.load(Ordering::Acquire), 2);
        drop(session);
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    }
    #[test]
    fn dropping_an_idle_session_returns_its_credit_before_replacement_enrollment() {
        let (store, session) = warmed_store();
        let hub = &store.inner.io;
        assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
        drop(session);
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
        let mut replacement = store.start_session(Default::default()).unwrap();
        assert_eq!(
            ready(
                replacement
                    .read(Serial(0), PlainRead, ReadOptions::default())
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            7
        );
        assert_eq!(hub.next.load(Ordering::Acquire), 2);
        drop(replacement);
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    }
    #[test]
    fn a_preissued_reservation_survives_exhaustion_but_a_fresh_request_is_rejected() {
        let (store, mut session) = warmed_store();
        let hub = &store.inner.io;
        hub.next.store(u64::MAX, Ordering::Release);
        assert_eq!(
            ready(
                session
                    .read(Serial(1), PlainRead, ReadOptions::default())
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            7
        );
        let mut issued = session.runtime.reusable_route.take().unwrap();
        hub.activate_operation(&mut issued).unwrap();
        assert!(hub.retain_operation(&mut issued).is_none());
        hub.release_operation(&mut issued).unwrap();
        assert!(matches!(
            session.read(Serial(2), PlainRead, ReadOptions::default()),
            Err(Rejected {
                reason: Error::CapacityExceeded,
                ..
            })
        ));
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
        assert_eq!(session.runtime.current.last_accepted, Some(Serial(1)));
        assert!(session.runtime.reusable_route.is_none());
    }
    struct CountedPut(Arc<AtomicUsize>);
    impl Keyed<Schema> for CountedPut {
        fn key(&self) -> &u64 {
            &1
        }
    }
    impl UpsertOperation<Schema> for CountedPut {
        type Output = u64;
        fn replacement(&mut self) -> Result<(u64, u64), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok((42, 42))
        }
        fn update_in_place(
            &mut self,
            _: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<u64>, Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(UpdateDecision::Append)
        }
    }
    #[test]
    fn a_full_global_pool_rejects_before_callbacks_and_does_not_consume_the_serial() {
        let mut config = crate::config::Config::default();
        config.log.page_bytes = 4096;
        config.log.memory_pages = 4;
        config.session.max_sessions = 1;
        config.session.max_pending = 1;
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(crate::device::null::NullDeviceFactory))
            .create()
            .unwrap();
        let hub = &store.inner.io;
        let mut session = store.start_session(Default::default()).unwrap();
        let mut occupied: Vec<_> = (0..hub.capacity)
            .map(|_| hub.reserve(SessionId([9; 16])).unwrap())
            .collect();
        let count = Arc::new(AtomicUsize::new(0));
        let before = hub.next.load(Ordering::Acquire);
        assert!(matches!(
            session.upsert(Serial(0), CountedPut(count.clone())),
            Err(Rejected {
                reason: Error::Busy,
                ..
            })
        ));
        assert_eq!(count.load(Ordering::Acquire), 0);
        assert_eq!(session.runtime.current.last_accepted, None);
        assert_eq!(hub.next.load(Ordering::Acquire), before);
        hub.release(occupied.pop().unwrap()).unwrap();
        assert_eq!(
            ready(
                session
                    .upsert(Serial(0), CountedPut(count.clone()))
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            42
        );
        assert_eq!(count.load(Ordering::Acquire), 1);
        assert_eq!(hub.occupied.load(Ordering::Acquire), hub.capacity);
        session
            .close(Deadline(Instant::now() + Duration::from_secs(5)))
            .unwrap();
        for id in occupied {
            hub.release(id).unwrap();
        }
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    }
    #[test]
    fn four_operations_hold_capacity_without_registering_mailboxes_or_losing_statistics() {
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .device(Box::new(crate::device::null::NullDeviceFactory))
            .create()
            .unwrap();
        store.enable_stats_collection();
        let hub = store.inner.io.clone();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let request = |value| Checked {
            key: 1,
            value,
            hub: hub.clone(),
            callbacks: callbacks.clone(),
        };
        let mut session = store.start_session(SessionOptions::default()).unwrap();
        assert_eq!(
            ready(
                session
                    .upsert(Serial(0), request(7))
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            7
        );
        assert_eq!(
            ready(
                session
                    .upsert(Serial(1), request(9))
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            9
        );
        assert_eq!(
            ready(
                session
                    .read(Serial(2), request(0), ReadOptions::default())
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            9
        );
        assert_eq!(
            ready(
                session
                    .rmw(Serial(3), request(0), RmwOptions::default())
                    .map_err(|e| e.reason)
                    .unwrap()
            ),
            10
        );
        ready(
            session
                .delete(Serial(4), request(0), DeleteOptions::default())
                .map_err(|e| e.reason)
                .unwrap(),
        );
        assert_eq!(callbacks.load(Ordering::SeqCst), 5);
        assert_eq!(hub.next.load(Ordering::Acquire), 1);
        assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
        assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
        assert_eq!(hub.capacity_acquisitions.load(Ordering::Acquire), 1);
        let statistics = store.statistics();
        assert!(statistics.measurements_complete);
        for (operation, count) in [
            (&statistics.upserts, 2),
            (&statistics.reads, 1),
            (&statistics.rmw, 1),
            (&statistics.deletes, 1),
        ] {
            assert_eq!(operation.accepted, count);
            assert_eq!(operation.completed, count);
            assert_eq!(operation.io_completions, 0);
        }
        let deadline = || Deadline(Instant::now() + Duration::from_secs(5));
        session.close(deadline()).unwrap();
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
        store.shutdown(deadline()).unwrap();
    }
}
#[test]
fn unregistered_operations_and_registered_io_share_one_capacity() {
    let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
    let session = SessionId([1; 16]);
    let mut operation = hub.reserve_operation(session).unwrap();
    assert!(operation.registered_id().is_none());
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
    let io = hub.reserve(session).unwrap();
    assert!(matches!(hub.reserve(session), Err(Error::Busy)));
    assert!(matches!(hub.reserve_operation(session), Err(Error::Busy)));
    assert_eq!(hub.next.load(Ordering::Acquire), 2);
    hub.activate_operation(&mut operation).unwrap();
    hub.activate_operation(&mut operation).unwrap();
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 2);
    assert!(hub.take(operation.id()).unwrap().is_none());
    hub.release_operation(&mut operation).unwrap();
    assert!(hub.release_operation(&mut operation).is_err());
    assert!(hub.activate_operation(&mut operation).is_err());
    hub.release(io).unwrap();
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    let mut next = hub.reserve_operation(session).unwrap();
    assert!(next.id().slot > io.slot);
    hub.release_operation(&mut next).unwrap();
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
}
#[test]
fn reserving_an_operation_does_not_wait_for_the_mailbox_table() {
    let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let result = std::thread::scope(|scope| {
        let held = hub.state.lock().unwrap();
        let worker = scope.spawn(|| {
            send.send(hub.reserve_operation(SessionId([1; 16])))
                .unwrap();
        });
        let result = receive.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        worker.join().unwrap();
        result
    });
    let mut route = result
        .expect("Operation admission waited for mailbox storage")
        .unwrap();
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
    hub.release_operation(&mut route).unwrap();
}
#[test]
fn operation_route_exhaustion_and_wrong_owner_do_not_lose_capacity() {
    let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
    let other = CompletionHub::new(StoreId([2; 16]), 2).unwrap();
    hub.next.store(u64::MAX - 1, Ordering::Release);
    let mut route = hub.reserve_operation(SessionId([1; 16])).unwrap();
    assert_eq!(route.id().slot, u64::MAX - 1);
    assert!(other.activate_operation(&mut route).is_err());
    assert!(other.release_operation(&mut route).is_err());
    assert!(matches!(
        hub.reserve_operation(route.id().session),
        Err(Error::CapacityExceeded)
    ));
    assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
    assert_eq!(other.occupied.load(Ordering::Acquire), 0);
    hub.release_operation(&mut route).unwrap();
    assert!(matches!(
        hub.reserve(route.id().session),
        Err(Error::CapacityExceeded)
    ));
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
}
#[test]
fn poisoned_registration_rejects_new_work_and_returns_only_unregistered_credits() {
    for registered in [false, true] {
        let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
        let mut route = hub.reserve_operation(SessionId([1; 16])).unwrap();
        if registered {
            hub.activate_operation(&mut route).unwrap();
        }
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = hub.state.lock().unwrap();
                panic!("injected mailbox table poison");
            })
            .is_err()
        );
        assert!(hub.activate_operation(&mut route).is_err());
        assert!(hub.reserve_operation(route.id().session).is_err());
        assert_eq!(hub.release_operation(&mut route).is_err(), registered);
        assert_eq!(
            hub.occupied.load(Ordering::Acquire),
            usize::from(registered)
        );
    }
}
#[test]
fn concurrent_unregistered_operations_cannot_multiply_the_capacity() {
    let hub = CompletionHub::new(StoreId([1; 16]), 3).unwrap();
    let mut assigned = std::collections::BTreeSet::new();
    for _ in 0..32 {
        let start = std::sync::Barrier::new(12);
        let routes = std::thread::scope(|scope| {
            let workers: Vec<_> = (1..=12)
                .map(|session| {
                    let hub = &hub;
                    let start = &start;
                    scope.spawn(move || {
                        start.wait();
                        hub.reserve_operation(SessionId([session; 16]))
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut accepted = Vec::new();
        for route in routes {
            match route {
                Ok(route) => {
                    assert!(assigned.insert(route.id().slot));
                    accepted.push(route);
                }
                Err(Error::Busy) => {}
                Err(error) => panic!("unexpected reservation error: {error:?}"),
            }
        }
        assert_eq!(accepted.len(), 3);
        for mut route in accepted {
            hub.release_operation(&mut route).unwrap();
        }
        assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    }
    assert_eq!(hub.next.load(Ordering::Acquire), assigned.len() as u64);
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
}
#[test]
fn released_inline_routes_cannot_consume_late_completions_or_displace_overflow() {
    let hub = CompletionHub::new(StoreId([1; 16]), 7).unwrap();
    let device = MemoryDevice::new(16, 64).unwrap();
    let session = SessionId([2; 16]);
    let original: Vec<_> = (0..7).map(|_| hub.reserve(session).unwrap()).collect();
    assert!(matches!(hub.reserve(session), Err(Error::Busy)));
    for id in &original {
        device
            .submit(IoRequest {
                route: CompletionHub::route(*id),
                operation: IoOperation::CreateDirectory(format!("route-{}", id.slot).into()),
            })
            .unwrap();
    }
    hub.release(original[0]).unwrap();
    hub.release(original[3]).unwrap();
    let first = hub.reserve(session).unwrap();
    let second = hub.reserve(session).unwrap();
    assert!(first.slot > original[6].slot && second.slot > first.slot);
    assert!(matches!(hub.reserve(session), Err(Error::Busy)));
    assert_eq!(hub.poll(&device, PollBudget::default()).unwrap(), 7);
    for id in [first, second] {
        assert!(hub.take(id).unwrap().is_none());
        assert_eq!(hub.completion_count(id).unwrap(), 0);
    }
    for index in [1, 2, 4, 5, 6] {
        let id = original[index];
        assert!(hub.take(id).unwrap().unwrap().result.is_ok());
        assert_eq!(hub.completion_count(id).unwrap(), 1);
        hub.release(id).unwrap();
    }
    for id in [first, second] {
        hub.release(id).unwrap();
    }
    assert!(hub.release(original[0]).is_err());
    assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
}
#[test]
fn after_other_session_polling_the_results_will_remain_in_the_original_mailbox_and_cannot_be_collected_if_the_identity_is_wrong()
 {
    let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
    let device = MemoryDevice::new(4, 64).unwrap();
    let first = hub.reserve(SessionId([1; 16])).unwrap();
    let second = hub.reserve(SessionId([2; 16])).unwrap();
    assert!(matches!(hub.reserve(SessionId([3; 16])), Err(Error::Busy)));
    for id in [first, second] {
        device
            .submit(IoRequest {
                route: CompletionHub::route(id),
                operation: IoOperation::CreateDirectory(format!("directory{}", id.slot).into()),
            })
            .unwrap();
    }
    assert_eq!(hub.poll(&device, PollBudget::default()).unwrap(), 2);
    let mut wrong = first;
    wrong.session = second.session;
    assert!(hub.take(wrong).is_err());
    assert!(hub.release(wrong).is_err());
    assert!(hub.take(second).unwrap().unwrap().result.is_ok());
    assert!(hub.take(first).unwrap().unwrap().result.is_ok());
    assert!(hub.take(first).unwrap().is_none());
    hub.release(first).unwrap();
    let next = hub.reserve(first.session).unwrap();
    assert!(next.slot > second.slot);
}
#[test]
fn late_completion_after_logging_out_only_recycles_the_buffer_and_does_not_accidentally_throw_new_requests()
 {
    let hub = CompletionHub::new(StoreId([1; 16]), 1).unwrap();
    let device = MemoryDevice::new(4, 64).unwrap();
    let old = hub.reserve(SessionId([1; 16])).unwrap();
    device
        .submit(IoRequest {
            route: CompletionHub::route(old),
            operation: IoOperation::Read {
                file: crate::device::FileId {
                    slot: 999,
                    generation: Generation(0),
                },
                offset: 0,
                buffer: crate::device::AlignedBuffer::new_zeroed(8, 8).unwrap(),
            },
        })
        .unwrap();
    hub.release(old).unwrap();
    let new = hub.reserve(old.session).unwrap();
    hub.poll(&device, PollBudget::default()).unwrap();
    assert!(hub.take(new).unwrap().is_none());
}
#[test]
fn repeated_uncollected_completion_and_unknown_routing_errors_are_reported_but_other_mailboxes_are_not_covered_or_lost()
 {
    let hub = CompletionHub::new(StoreId([1; 16]), 4).unwrap();
    let device = MemoryDevice::new(4, 64).unwrap();
    let first = hub.reserve(SessionId([1; 16])).unwrap();
    let second = hub.reserve(SessionId([2; 16])).unwrap();
    let submit = |route, name: &str| {
        device
            .submit(IoRequest {
                route,
                operation: IoOperation::CreateDirectory(name.into()),
            })
            .unwrap()
    };
    let first_io = submit(CompletionHub::route(first), "first time");
    submit(CompletionHub::route(first), "Repeat");
    let second_io = submit(CompletionHub::route(second), "another session");
    assert!(matches!(
        hub.poll(&device, PollBudget::default()),
        Err(Error::InvalidState(_))
    ));
    assert_eq!(hub.take(first).unwrap().unwrap().id, first_io);
    assert!(hub.take(first).unwrap().is_none());
    assert_eq!(hub.take(second).unwrap().unwrap().id, second_io);
    submit(CompletionRoute(999), "unknown route");
    assert!(matches!(
        hub.poll(&device, PollBudget::default()),
        Err(Error::InvalidState(_))
    ));
    assert!(hub.take(first).unwrap().is_none());
    assert!(hub.take(second).unwrap().is_none());
}

#[test]
fn unused_reservations_preserve_capacity_and_published_routes_cannot_be_reused() {
    let hub = CompletionHub::new(StoreId([1; 16]), 1).unwrap();
    let other = CompletionHub::new(StoreId([2; 16]), 1).unwrap();
    let session = SessionId([3; 16]);
    let mut old = hub.reserve_operation(session).unwrap();
    let old_id = old.id();
    assert!(other.retain_operation(&mut old).is_none());
    let mut held = hub.retain_operation(&mut old).unwrap();
    assert!(hub.retain_operation(&mut old).is_none());
    assert!(hub.release_operation(&mut old).is_err());
    assert!(hub.reuse_operation(&mut old, session).is_err());
    assert!(other.reuse_operation(&mut held, session).is_err());
    assert!(hub.reuse_operation(&mut held, SessionId([4; 16])).is_err());
    assert_eq!(hub.next.load(Ordering::Acquire), 1);
    assert!(matches!(hub.reserve(session), Err(Error::Busy)));
    hub.reuse_operation(&mut held, session).unwrap();
    assert_eq!(held.id(), old_id);
    assert_eq!(hub.occupied.load(Ordering::Acquire), 1);
    assert_eq!(hub.capacity_acquisitions.load(Ordering::Acquire), 1);
    hub.activate_operation(&mut held).unwrap();
    assert!(hub.retain_operation(&mut held).is_none());
    assert!(hub.reuse_operation(&mut held, session).is_err());
    assert!(hub.take(old_id).unwrap().is_none());
    hub.release_operation(&mut held).unwrap();
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
    assert_eq!(other.occupied.load(Ordering::Acquire), 0);
}
#[test]
fn poisoning_prevents_reservation_reuse_but_allows_unregistered_cleanup() {
    let hub = CompletionHub::new(StoreId([1; 16]), 1).unwrap();
    let session = SessionId([2; 16]);
    let mut route = hub.reserve_operation(session).unwrap();
    let mut held = hub.retain_operation(&mut route).unwrap();
    assert!(
        std::panic::catch_unwind(|| {
            let _lock = hub.state.lock().unwrap();
            panic!("injected mailbox poison");
        })
        .is_err()
    );
    assert!(hub.reuse_operation(&mut held, session).is_err());
    assert_eq!(hub.next.load(Ordering::Acquire), 1);
    hub.release_operation(&mut held).unwrap();
    assert_eq!(hub.occupied.load(Ordering::Acquire), 0);
}
