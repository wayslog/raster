//! Use public Session/Ticket Driver has deprecated record reading,Flash preparation is still completed by the internal test interface.
use crate::{
    RasterKV, Submission,
    api::{
        completion::{Outcome, TicketState},
        operation::*,
        session::SessionOptions,
    },
    config::Config,
    device::*,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{cell::Cell, rc::Rc, sync::Arc};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Factory(Arc<memory::MemoryDevice>);
struct Handle(Arc<memory::MemoryDevice>);
impl DeviceFactory for Factory {
    fn open(&self, _: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(Handle(self.0.clone())))
    }
}
impl Device for Handle {
    fn capabilities(&self) -> DeviceCapabilities {
        self.0.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        self.0.submit(request)
    }
    fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
        self.0.poll(budget, out)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.0.shutdown(deadline)
    }
}
struct Put(u64);
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
struct Read {
    key: u64,
    calls: Rc<Cell<usize>>,
    thread: std::thread::ThreadId,
}
impl Keyed<Schema> for Read {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadOperation<Schema> for Read {
    type Output = Rc<u64>;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<Rc<u64>, Error> {
        assert_eq!(std::thread::current().id(), self.thread);
        self.calls.set(self.calls.get() + 1);
        Ok(Rc::new(*value.view()))
    }
}
fn request(key: u64, calls: &Rc<Cell<usize>>) -> Read {
    Read {
        key,
        calls: calls.clone(),
        thread: std::thread::current().id(),
    }
}
fn complete(device: &dyn Device) -> IoCompletion {
    let mut out = vec![];
    device.poll(PollBudget::default(), &mut out).unwrap();
    assert_eq!(out.len(), 1);
    out.pop().unwrap()
}
fn setup() -> RasterKV<Schema> {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    config.session.max_pending = 1;
    config.session.max_results = 1;
    let device = Arc::new(memory::MemoryDevice::new(16, 65536).unwrap());
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(Factory(device.clone())))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..60 {
        assert!(matches!(
            session
                .upsert(Serial(key), Put(key))
                .map_err(|r| r.reason)
                .unwrap(),
            Submission::Ready(Ok(_))
        ));
    }
    device
        .submit(IoRequest {
            route: CompletionRoute(900),
            operation: IoOperation::CreateDirectory("segments".into()),
        })
        .unwrap();
    complete(&*device).result.unwrap();
    let path = store.inner.storage.segment_path(0, Generation(0));
    device
        .submit(IoRequest {
            route: CompletionRoute(900),
            operation: IoOperation::Open {
                path,
                create_new: true,
            },
        })
        .unwrap();
    let IoOutcome::Opened(file) = complete(&*device).result.unwrap() else {
        panic!("open")
    };
    store.inner.storage.bind(0, Generation(0), file).unwrap();
    store.inner.log.advance_read_only(LogAddress(4096)).unwrap();
    let mut flush = store
        .inner
        .log
        .begin_flush(
            &store.inner.storage,
            CompletionRoute(900),
            CheckpointVersion(0),
        )
        .unwrap();
    loop {
        flush.submit_next(&store.inner.storage).unwrap();
        flush
            .accept(&store.inner.storage, complete(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        if store
            .inner
            .log
            .finish_flush(&store.inner.storage, &mut flush)
            .unwrap()
        {
            break;
        }
    }
    assert_eq!(store.inner.log.evict_next().unwrap().completed, 1);
    store
}
#[test]
fn session_polling_isolates_callbacks_and_results_budget_is_released_after_collection() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let a = Rc::new(Cell::new(0));
    let Submission::Pending(mut ta) = first
        .read(Serial(0), request(0, &a), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending");
    };
    let worker_store = store.clone();
    let second = crate::engine::session_actor::Actor::new(move || {
        let mut second = worker_store.start_session(Default::default()).unwrap();
        let b = Rc::new(Cell::new(0));
        let Submission::Pending(tb) = second
            .read(Serial(0), request(1, &b), ReadOptions::default())
            .map_err(|r| r.reason)
            .unwrap()
        else {
            panic!("should_pending");
        };
        (second, tb, b)
    });
    assert!(
        first
            .read(Serial(1), request(0, &a), ReadOptions::default())
            .is_err()
    );
    for _ in 0..4 {
        first.poll(PollBudget::default()).unwrap();
    }
    assert_eq!(a.get(), 1);
    second.call(|(_, tb, b)| {
        assert_eq!(b.get(), 0);
        assert!(matches!(tb.try_take(), Ok(TicketState::Pending)));
    });
    assert!(
        first
            .read(Serial(1), request(0, &a), ReadOptions::default())
            .is_err()
    );
    assert_eq!(first.last_accepted(), Some(Serial(0)));
    assert!(
        matches!(ta.try_take(), Ok(TicketState::Ready(Ok(Outcome::Success(value)))) if *value==0)
    );
    assert!(
        first
            .read(Serial(1), request(0, &a), ReadOptions::default())
            .is_ok()
    );
    second.call(|(second, tb, b)| {
        for _ in 0..4 { second.poll(PollBudget::default()).unwrap(); }
        assert_eq!(b.get(), 1);
        assert!(matches!(tb.try_take(), Ok(TicketState::Ready(Ok(Outcome::Success(value)))) if *value==1));
    });
}
#[test]
fn discarding_tickets_still_performs_reads_while_discarding_sessions_explicitly_terminates_unexecuted_requests()
 {
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(ticket) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    drop(ticket);
    for _ in 0..4 {
        session.poll(PollBudget::default()).unwrap();
    }
    assert_eq!(calls.get(), 1);
    let Submission::Pending(mut ticket) = session
        .read(Serial(1), request(1, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    drop(session);
    assert!(matches!(
        ticket.try_take(),
        Ok(TicketState::Ready(Err(OperationError {
            cause: Error::SessionAbandoned,
            effect: Effect::NotApplied
        })))
    ));
    assert_eq!(calls.get(), 1);
    let mut another = store.start_session(SessionOptions::default()).unwrap();
    another.poll(PollBudget::default()).unwrap();
}

#[test]
fn public_write_polling_drives_automatic_disk_flushing_and_a_two_page_budget_to_read_back_multi_window_data()
 {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    config.session.max_pending = 1;
    config.session.max_results = 1;
    let device = Arc::new(memory::MemoryDevice::new(16, 131072).unwrap());
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(Factory(device)))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..500 {
        assert!(matches!(
            session
                .upsert(Serial(key), Put(key))
                .map_err(|r| r.reason)
                .unwrap(),
            Submission::Ready(Ok(_))
        ));
        for _ in 0..16 {
            session.poll(PollBudget::default()).unwrap();
        }
    }
    assert!(store.inner.log.frontiers().unwrap().safe_head.0 >= 7 * 4096);
    let calls = Rc::new(Cell::new(0));
    let mut disk_reads = 0;
    for key in 0..500 {
        match session
            .read(
                Serial(500 + key),
                request(key, &calls),
                ReadOptions::default(),
            )
            .map_err(|r| r.reason)
            .unwrap()
        {
            Submission::Ready(result) => {
                assert!(matches!(result, Ok(Outcome::Success(value)) if *value == key))
            }
            Submission::Pending(mut ticket) => {
                disk_reads += 1;
                let mut done = false;
                for _ in 0..16 {
                    session.poll(PollBudget::default()).unwrap();
                    if let TicketState::Ready(result) = ticket.try_take().unwrap() {
                        assert!(matches!(result, Ok(Outcome::Success(value)) if *value == key));
                        done = true;
                        break;
                    }
                }
                assert!(done, "Disk read did not end within budget");
            }
        }
    }
    assert!(disk_reads > 400);
    assert_eq!(calls.get(), 500);
}

struct CountedPut {
    key: u64,
    value: u64,
    calls: Rc<Cell<usize>>,
}
impl Keyed<Schema> for CountedPut {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertOperation<Schema> for CountedPut {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        self.calls.set(self.calls.get() + 1);
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        panic!("This test only inserts new keys or replaces obsolete cold keys")
    }
}
#[test]
fn insufficient_writes_are_suspended_and_the_replacement_callback_is_not_called_repeatedly_after_recovery()
 {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    config.session.max_pending = 1;
    config.session.max_results = 1;
    let device = Arc::new(memory::MemoryDevice::new(16, 131072).unwrap());
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(Factory(device)))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let mut pending = 0;
    for serial in 0..401 {
        let key = if serial == 400 { 0 } else { serial };
        let value = if serial == 400 { 999 } else { serial };
        let result = session
            .upsert(
                Serial(serial),
                CountedPut {
                    key,
                    value,
                    calls: calls.clone(),
                },
            )
            .map_err(|r| r.reason)
            .unwrap();
        match result {
            Submission::Ready(result) => {
                assert!(matches!(result, Ok(Outcome::Success(output)) if output == value))
            }
            Submission::Pending(mut ticket) => {
                pending += 1;
                let mut done = false;
                for _ in 0..100 {
                    session.poll(PollBudget::default()).unwrap();
                    if let TicketState::Ready(result) = ticket.try_take().unwrap() {
                        assert!(matches!(result, Ok(Outcome::Success(output)) if output == value));
                        done = true;
                        break;
                    }
                }
                assert!(done, "Writes to waiting space are not finalized");
            }
        }
        assert_eq!(calls.get(), serial as usize + 1);
    }
    assert!(pending >= 4);
    assert!(store.inner.log.frontiers().unwrap().safe_head.0 >= 4 * 4096);
    let reads = Rc::new(Cell::new(0));
    for key in 0..400 {
        let expected = if key == 0 { 999 } else { key };
        match session
            .read(
                Serial(401 + key),
                request(key, &reads),
                ReadOptions::default(),
            )
            .map_err(|r| r.reason)
            .unwrap()
        {
            Submission::Ready(result) => {
                assert!(matches!(result, Ok(Outcome::Success(value)) if *value == expected))
            }
            Submission::Pending(mut ticket) => {
                let mut done = false;
                for _ in 0..100 {
                    session.poll(PollBudget::default()).unwrap();
                    if let TicketState::Ready(result) = ticket.try_take().unwrap() {
                        assert!(
                            matches!(result, Ok(Outcome::Success(value)) if *value == expected)
                        );
                        done = true;
                        break;
                    }
                }
                assert!(done);
            }
        }
    }
    assert_eq!(calls.get(), 401);
}

struct Add {
    key: u64,
    delta: u64,
    copies: Rc<Cell<usize>>,
    initials: Rc<Cell<usize>>,
}
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        self.initials.set(self.initials.get() + 1);
        Ok((self.delta, self.delta))
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.copies.set(self.copies.get() + 1);
        let value = value.view().wrapping_add(self.delta);
        Ok((value, value))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn finish_number(session: &mut crate::Session<Schema>, submission: Submission<u64>) -> u64 {
    let result = match submission {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => {
            let mut result = None;
            for _ in 0..100 {
                session.poll(PollBudget::default()).unwrap();
                if let TicketState::Ready(value) = ticket.try_take().unwrap() {
                    result = Some(value);
                    break;
                }
            }
            result.expect("Pending updates are not finalized")
        }
    };
    match result.unwrap() {
        Outcome::Success(value) => value,
        _ => panic!("Expected successful update"),
    }
}
#[test]
fn when_substitution_occurs_during_cold_key_query_the_new_value_will_be_rechecked_before_calculation()
 {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let second = crate::engine::session_actor::session(&store);
    let copies = Rc::new(Cell::new(0));
    let initials = Rc::new(Cell::new(0));
    let pending = first
        .rmw(
            Serial(0),
            Add {
                key: 0,
                delta: 1,
                copies: copies.clone(),
                initials: initials.clone(),
            },
            RmwOptions::default(),
        )
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(pending, Submission::Pending(_)));
    second.call(|second| {
        let replacement = second
            .upsert(
                Serial(0),
                CountedPut {
                    key: 0,
                    value: 100,
                    calls: Rc::new(Cell::new(0)),
                },
            )
            .map_err(|r| r.reason)
            .unwrap();
        assert_eq!(finish_number(second, replacement), 100);
    });
    assert_eq!(finish_number(&mut first, pending), 101);
    assert_eq!(copies.get(), 1);
    assert_eq!(initials.get(), 0);
    let missing = first
        .rmw(
            Serial(1),
            Add {
                key: 999,
                delta: 1,
                copies,
                initials: initials.clone(),
            },
            RmwOptions {
                create_if_missing: false,
            },
        )
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(missing, Submission::Ready(Ok(Outcome::NotFound))));
    assert_eq!(initials.get(), 0);
}
#[test]
fn read_modify_and_write_new_ones_across_the_capacity_window_and_the_two_cold_key_increments_will_not_be_lost()
 {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let copies = Rc::new(Cell::new(0));
    let initials = Rc::new(Cell::new(0));
    let request = |key| Add {
        key,
        delta: 1,
        copies: copies.clone(),
        initials: initials.clone(),
    };
    let a = first
        .rmw(Serial(0), request(0), RmwOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(a, Submission::Pending(_)));
    let worker_store = store.clone();
    let second = crate::engine::session_actor::Actor::new(move || {
        let mut second = worker_store.start_session(Default::default()).unwrap();
        let copies = Rc::new(Cell::new(0));
        let b = second
            .rmw(
                Serial(0),
                Add {
                    key: 0,
                    delta: 1,
                    copies: copies.clone(),
                    initials: Rc::new(Cell::new(0)),
                },
                RmwOptions::default(),
            )
            .map_err(|r| r.reason)
            .unwrap();
        assert!(matches!(b, Submission::Pending(_)));
        (second, Some(b), copies)
    });
    assert_eq!(finish_number(&mut first, a), 1);
    second.call(|(second, b, _)| assert_eq!(finish_number(second, b.take().unwrap()), 2));
    let mut waiting = 0;
    for i in 0..400 {
        let result = first
            .rmw(Serial(i + 1), request(i + 1000), RmwOptions::default())
            .map_err(|r| r.reason)
            .unwrap();
        if matches!(result, Submission::Pending(_)) {
            waiting += 1;
        }
        assert_eq!(finish_number(&mut first, result), 1);
    }
    assert!(waiting > 3);
    assert!(initials.get() >= 400);
    assert_eq!(copies.get() + second.call(|(_, _, copies)| copies.get()), 2);
    second.call(|(second, _, _)| {
        let reads = Rc::new(Cell::new(0));
        for key in [0, 1000, 1199, 1399] {
            let result = second.read(Serial(2000 + key), self::request(key, &reads), ReadOptions::default()).map_err(|r| r.reason).unwrap();
            let result = match result {
                Submission::Ready(result) => result,
                Submission::Pending(mut ticket) => second.wait(&mut ticket, wait_deadline()).unwrap(),
            };
            assert!(matches!(result, Ok(Outcome::Success(value)) if *value == if key == 0 { 2 } else { 1 }));
        }
    });
}

struct Erase {
    key: u64,
    calls: Rc<Cell<usize>>,
    panic: bool,
}
impl Keyed<Schema> for Erase {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl DeleteOperation<Schema> for Erase {
    type Output = u64;
    fn complete(self, _: DeleteOutcome) -> u64 {
        self.calls.set(self.calls.get() + 1);
        assert!(!self.panic, "Delete completion callback panic");
        self.key
    }
}
#[test]
fn after_cold_key_blind_deletion_the_update_is_visible_and_the_tombstone_is_forced_to_wait_for_space()
 {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let second = crate::engine::session_actor::session(&store);
    let calls = Rc::new(Cell::new(0));
    let delete = |key| Erase {
        key,
        calls: calls.clone(),
        panic: false,
    };
    let pending = first
        .delete(Serial(0), delete(0), DeleteOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(pending, Submission::Ready(_)));
    assert_eq!(finish_number(&mut first, pending), 0);
    second.call(|second| {
        let replace = second
            .upsert(
                Serial(0),
                CountedPut {
                    key: 0,
                    value: 100,
                    calls: Rc::new(Cell::new(0)),
                },
            )
            .map_err(|r| r.reason)
            .unwrap();
        assert_eq!(finish_number(second, replace), 100);
    });
    let read = first
        .read(
            Serial(1),
            request(0, &Rc::new(Cell::new(0))),
            Default::default(),
        )
        .map_err(|r| r.reason)
        .unwrap();
    let read = match read {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => first.wait(&mut ticket, wait_deadline()).unwrap(),
    };
    assert!(matches!(read, Ok(Outcome::Success(value)) if *value == 100));
    let forced = first
        .delete(
            Serial(2),
            delete(0),
            DeleteOptions {
                force_tombstone: true,
            },
        )
        .map_err(|r| r.reason)
        .unwrap();
    assert_eq!(finish_number(&mut first, forced), 0);
    let missing = first
        .delete(Serial(3), delete(999), DeleteOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(missing, Submission::Ready(Ok(Outcome::NotFound))));
    assert_eq!(calls.get(), 2);
    let mut waiting = 0;
    for i in 0..400 {
        let result = first
            .delete(
                Serial(i + 4),
                delete(i + 1000),
                DeleteOptions {
                    force_tombstone: true,
                },
            )
            .map_err(|r| r.reason)
            .unwrap();
        if matches!(result, Submission::Pending(_)) {
            waiting += 1;
        }
        assert_eq!(finish_number(&mut first, result), i + 1000);
        assert_eq!(calls.get(), i as usize + 3);
    }
    assert!(waiting >= 3);
    second.call(|second| {
        for key in [0, 1000, 1399] {
            let read = second
                .read(
                    Serial(1000 + key),
                    request(key, &Rc::new(Cell::new(0))),
                    ReadOptions {
                        abort_if_tombstone: true,
                    },
                )
                .map_err(|r| r.reason)
                .unwrap();
            let result = match read {
                Submission::Ready(result) => result,
                Submission::Pending(mut ticket) => {
                    let mut result = None;
                    for _ in 0..100 {
                        second.poll(PollBudget::default()).unwrap();
                        if let TicketState::Ready(value) = ticket.try_take().unwrap() {
                            result = Some(value);
                            break;
                        }
                    }
                    result.unwrap()
                }
            };
            assert!(matches!(
                result,
                Ok(Outcome::Aborted(
                    crate::api::completion::AbortReason::Tombstone
                ))
            ));
        }
    });
}
#[test]
fn the_disk_deletion_completion_callback_panic_only_ends_once_and_retains_the_effective_tombstone()
{
    use crate::schema::KeyCodec;
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let submission = session
        .delete(
            Serial(0),
            Erase {
                key: 0,
                calls: calls.clone(),
                panic: true,
            },
            DeleteOptions::default(),
        )
        .map_err(|r| r.reason)
        .unwrap();
    let result = match submission {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => {
            let result = session.wait(&mut ticket, wait_deadline()).unwrap();
            assert!(matches!(ticket.try_take(), Err(TicketError::AlreadyTaken)));
            result
        }
    };
    assert!(matches!(
        result,
        Err(OperationError {
            effect: Effect::Applied,
            ..
        })
    ));
    assert_eq!(calls.get(), 1);
    assert!(store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    let head =
        super::Engine::<Schema>::head(store.inner.index.prepare(U64Key.hash(&0)).unwrap()).unwrap();
    assert!(
        store
            .inner
            .log
            .find(&U64Key, &0, head)
            .unwrap()
            .unwrap()
            .is_tombstone()
    );
}

fn wait_deadline() -> Deadline {
    Deadline(std::time::Instant::now() + std::time::Duration::from_secs(5))
}
#[test]
fn the_wait_timed_out_to_retain_the_request_and_the_error_session_cannot_advance_the_ticket() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let other_store = setup();
    let mut other = other_store
        .start_session(SessionOptions::default())
        .unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = first
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending");
    };
    assert!(matches!(
        other.wait(&mut ticket, wait_deadline()),
        Err(Error::InvalidState(_))
    ));
    assert_eq!(calls.get(), 0);
    assert!(matches!(
        first.wait(&mut ticket, Deadline(std::time::Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert!(matches!(ticket.try_take(), Ok(TicketState::Pending)));
    first.close(wait_deadline()).unwrap();
    let mut next = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        next.wait(&mut ticket, wait_deadline()),
        Err(Error::InvalidState(_))
    ));
    assert!(
        matches!(first.wait(&mut ticket, wait_deadline()).unwrap(), Ok(Outcome::Success(value)) if *value==0)
    );
    assert_eq!(calls.get(), 1);
    assert!(matches!(
        first.wait(&mut ticket, wait_deadline()),
        Err(Error::InvalidState(_))
    ));
    next.close(wait_deadline()).unwrap();
    other.close(wait_deadline()).unwrap();
}
#[test]
fn stop_accepting_after_closing_timeout_but_can_still_drain_and_collect_results_after_closing() {
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    assert!(matches!(
        session.close(Deadline(std::time::Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert!(
        session
            .read(Serial(1), request(1, &calls), ReadOptions::default())
            .is_err()
    );
    assert!(session.close(wait_deadline()).unwrap().drained);
    assert_eq!(calls.get(), 1);
    // Completed results will not be lost because the waiting deadline has passed.
    assert!(
        matches!(session.wait(&mut ticket, Deadline(std::time::Instant::now())).unwrap(), Ok(Outcome::Success(value)) if *value==0)
    );
    assert!(session.close(wait_deadline()).unwrap().drained);
}
#[test]
fn the_single_emptying_report_remains_and_the_expiration_time_does_not_consume_the_bill_results() {
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let mut ticket = None;
    for i in 0..200 {
        let result = session
            .upsert(
                Serial(i),
                CountedPut {
                    key: 1000 + i,
                    value: i,
                    calls: calls.clone(),
                },
            )
            .map_err(|r| r.reason)
            .unwrap();
        if let Submission::Pending(value) = result {
            ticket = Some(value);
            break;
        }
    }
    let mut ticket = ticket.expect("The log should be suspended when it is full.");
    assert!(matches!(
        session.complete_pending(WaitMode::Once).unwrap(),
        DrainReport::Pending(_)
    ));
    assert!(matches!(
        session.complete_pending(WaitMode::Until(Deadline(std::time::Instant::now()))),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(
        session
            .complete_pending(WaitMode::Until(wait_deadline()))
            .unwrap(),
        DrainReport::Drained
    );
    assert!(matches!(
        ticket.try_take(),
        Ok(TicketState::Ready(Ok(Outcome::Success(_))))
    ));
}

#[test]
fn recovery_can_still_be_completed_after_waiting_for_timeout_during_device_delay() {
    struct PausedDevice {
        inner: Arc<dyn Device>,
        paused: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Device for PausedDevice {
        fn capabilities(&self) -> DeviceCapabilities {
            self.inner.capabilities()
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            self.inner.submit(request)
        }
        fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
            if self.paused.load(std::sync::atomic::Ordering::SeqCst) {
                Ok(())
            } else {
                self.inner.poll(budget, out)
            }
        }
        fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
            self.inner.shutdown(deadline)
        }
    }
    let mut store = setup();
    let paused = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let engine = Arc::get_mut(&mut store.inner).expect("Test session exited");
    engine.storage.device = Arc::new(PausedDevice {
        inner: engine.storage.device.clone(),
        paused: paused.clone(),
    });
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    assert!(matches!(
        session.wait(
            &mut ticket,
            Deadline(std::time::Instant::now() + std::time::Duration::from_millis(3))
        ),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(calls.get(), 0);
    assert!(matches!(ticket.try_take(), Ok(TicketState::Pending)));
    paused.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(
        matches!(session.wait(&mut ticket, wait_deadline()).unwrap(), Ok(Outcome::Success(value)) if *value == 0)
    );
    assert_eq!(calls.get(), 1);
}

#[derive(Default)]
struct ShutdownProbe {
    paused: std::sync::atomic::AtomicBool,
    submitted: std::sync::atomic::AtomicUsize,
    returned: std::sync::atomic::AtomicUsize,
    shutdowns: std::sync::atomic::AtomicUsize,
}
struct ProbedDevice {
    inner: Arc<dyn Device>,
    probe: Arc<ShutdownProbe>,
}
impl Device for ProbedDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let result = self.inner.submit(request);
        if result.is_ok() {
            self.probe
                .submitted
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        result
    }
    fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
        if self.probe.paused.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let before = out.len();
        let result = self.inner.poll(budget, out);
        self.probe
            .returned
            .fetch_add(out.len() - before, std::sync::atomic::Ordering::SeqCst);
        result
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.probe
            .shutdowns
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.shutdown(deadline)
    }
}
fn shutdown_setup() -> (
    RasterKV<Schema>,
    Arc<ShutdownProbe>,
    Arc<memory::MemoryDevice>,
) {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    let device = Arc::new(memory::MemoryDevice::new(16, 65536).unwrap());
    let mut store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(Factory(device.clone())))
        .create()
        .unwrap();
    let probe = Arc::new(ShutdownProbe::default());
    Arc::get_mut(&mut store.inner).unwrap().storage.device = Arc::new(ProbedDevice {
        inner: device.clone(),
        probe: probe.clone(),
    });
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..60 {
        assert!(matches!(
            session
                .upsert(Serial(key), Put(key))
                .map_err(|r| r.reason)
                .unwrap(),
            Submission::Ready(Ok(_))
        ));
    }
    // The first poll only freezes and encodes the first page;Catalog has not been submitted to the device yet,open or write operation.
    session.poll(PollBudget::default()).unwrap();
    session.close(wait_deadline()).unwrap();
    (store, probe, device)
}
#[test]
fn storage_shutdown_completes_the_directory_opening_and_writing_stages_of_the_started_page() {
    use std::sync::atomic::Ordering::SeqCst;
    for rounds in 0..4 {
        let (store, probe, _) = shutdown_setup();
        for _ in 0..rounds {
            store
                .inner
                .io
                .poll(&*store.inner.storage.device, PollBudget::default())
                .unwrap();
            store.inner.progress_storage().unwrap();
        }
        assert!(store.shutdown(wait_deadline()).unwrap().device_drained);
        assert_eq!(
            store.inner.log.frontiers().unwrap().flushed_until,
            LogAddress(4096)
        );
        assert_eq!(probe.submitted.load(SeqCst), 3);
        assert_eq!(probe.returned.load(SeqCst), 3);
        assert!(store.start_session(SessionOptions::default()).is_err());
        assert!(
            store
                .shutdown(Deadline(std::time::Instant::now()))
                .unwrap()
                .device_drained
        );
        assert_eq!(probe.shutdowns.load(SeqCst), 1);
    }
}
#[test]
fn storage_drain_timeout_will_not_shut_down_the_device_early_and_can_continue_after_recovery() {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, probe, _) = shutdown_setup();
    probe.paused.store(true, SeqCst);
    assert!(matches!(
        store.shutdown(Deadline(
            std::time::Instant::now() + std::time::Duration::from_millis(3)
        )),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(probe.submitted.load(SeqCst), 1);
    assert_eq!(probe.returned.load(SeqCst), 0);
    assert_eq!(probe.shutdowns.load(SeqCst), 0);
    assert!(store.start_session(SessionOptions::default()).is_err());
    probe.paused.store(false, SeqCst);
    assert!(store.shutdown(wait_deadline()).unwrap().device_drained);
    assert_eq!(probe.submitted.load(SeqCst), probe.returned.load(SeqCst));
    assert_eq!(
        store.inner.log.frontiers().unwrap().flushed_until,
        LogAddress(4096)
    );
}
#[test]
fn if_the_flash_fails_the_device_will_still_be_emptied_but_the_success_boundary_will_not_be_published_and_it_will_not_automatically_retry()
 {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, probe, device) = shutdown_setup();
    device
        .inject_next(memory::MemoryFault::Fail(std::io::ErrorKind::Other))
        .unwrap();
    assert!(matches!(store.shutdown(wait_deadline()), Err(Error::Io(_))));
    assert_eq!(
        store.inner.log.frontiers().unwrap().flushed_until,
        LogAddress(0)
    );
    assert_eq!(probe.submitted.load(SeqCst), 1);
    assert_eq!(probe.returned.load(SeqCst), 1);
    assert!(store.inner.failed.load(SeqCst));
    assert!(store.shutdown(wait_deadline()).unwrap().device_drained);
    assert_eq!(probe.shutdowns.load(SeqCst), 1);
    assert_eq!(probe.submitted.load(SeqCst), 1);
}

#[test]
fn when_the_engine_has_failed_it_only_returns_the_buffer_in_transit_and_does_not_advance_the_brush_boundary()
 {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, probe, _) = shutdown_setup();
    // Directory and opening completed,Page write has not yet returned.
    for _ in 0..3 {
        store
            .inner
            .io
            .poll(&*store.inner.storage.device, PollBudget::default())
            .unwrap();
        store.inner.progress_storage().unwrap();
    }
    assert_eq!(probe.submitted.load(SeqCst), 3);
    assert_eq!(probe.returned.load(SeqCst), 2);
    store.inner.failed.store(true, SeqCst);
    assert!(store.shutdown(wait_deadline()).unwrap().device_drained);
    assert_eq!(probe.submitted.load(SeqCst), probe.returned.load(SeqCst));
    assert_eq!(
        store.inner.log.frontiers().unwrap().flushed_until,
        LogAddress(0)
    );
    assert!(store.start_session(SessionOptions::default()).is_err());
}
#[test]
fn late_reads_after_session_abandonment_are_drained_by_storage_shutdown_and_do_not_call_the_user() {
    use std::sync::atomic::Ordering::SeqCst;
    let mut store = setup();
    let probe = Arc::new(ShutdownProbe::default());
    let engine = Arc::get_mut(&mut store.inner).unwrap();
    engine.storage.device = Arc::new(ProbedDevice {
        inner: engine.storage.device.clone(),
        probe: probe.clone(),
    });
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    drop(session);
    assert!(store.shutdown(wait_deadline()).unwrap().device_drained);
    assert_eq!(probe.submitted.load(SeqCst), 1);
    assert_eq!(probe.returned.load(SeqCst), 1);
    assert_eq!(calls.get(), 0);
    assert!(matches!(
        ticket.try_take(),
        Ok(TicketState::Ready(Err(OperationError {
            cause: Error::SessionAbandoned,
            effect: Effect::NotApplied
        })))
    ));
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn native_file_storage_is_turned_off_flushing_started_pages_and_retaining_verifiable_frames() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-shutdown-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..60 {
        assert!(matches!(
            session
                .upsert(Serial(key), Put(key))
                .map_err(|r| r.reason)
                .unwrap(),
            Submission::Ready(Ok(_))
        ));
    }
    session.poll(PollBudget::default()).unwrap();
    session.close(wait_deadline()).unwrap();
    assert!(store.shutdown(wait_deadline()).unwrap().device_drained);
    assert_eq!(
        session.poll(PollBudget::default()).unwrap(),
        Progress::default()
    );
    let bytes = std::fs::read(
        root.0
            .join(store.inner.storage.segment_path(0, Generation(0))),
    )
    .unwrap();
    let frame = crate::format::PageFrame::decode(&bytes, PageId(0), 4096).unwrap();
    let records = frame.records().unwrap();
    assert!(!records.is_empty());
    for (_, record) in records {
        assert_eq!(record.key, record.value);
    }
    assert_eq!(
        store.inner.log.frontiers().unwrap().flushed_until,
        LogAddress(4096)
    );
}

#[test]
fn unit_budget_fairness_advances_multiple_requests_and_uncollected_results_still_exert_back_pressure_after_emptying()
 {
    let mut store = setup();
    let config = &mut Arc::get_mut(&mut store.inner).unwrap().config;
    config.session.max_pending = 3;
    config.session.max_results = 3;
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let mut tickets = Vec::new();
    for key in 0..3 {
        let Submission::Pending(ticket) = session
            .read(
                Serial(key * 2),
                request(key, &calls),
                ReadOptions::default(),
            )
            .map_err(|r| r.reason)
            .unwrap()
        else {
            panic!("should_pending")
        };
        tickets.push(ticket);
    }
    assert!(
        session
            .read(Serial(6), request(3, &calls), ReadOptions::default())
            .is_err()
    );
    assert_eq!(session.last_accepted(), Some(Serial(4)));
    let budget = PollBudget(std::num::NonZeroUsize::new(1).unwrap());
    let deadline = wait_deadline();
    let mut completed = 0;
    while completed != 3 {
        assert!(
            !deadline.expired(),
            "Unit budget must not starve other requests"
        );
        let progress = session.poll(budget).unwrap();
        assert!(progress.completed <= 1);
        completed += progress.completed;
        assert_eq!(progress.remaining, 3 - completed);
    }
    assert_eq!(calls.get(), 3);
    assert_eq!(
        session
            .complete_pending(WaitMode::Until(wait_deadline()))
            .unwrap(),
        DrainReport::Drained
    );
    assert!(
        session
            .read(Serial(6), request(59, &calls), ReadOptions::default())
            .is_err()
    );
    assert_eq!(session.last_accepted(), Some(Serial(4)));
    assert!(
        matches!(tickets[0].try_take(), Ok(TicketState::Ready(Ok(Outcome::Success(value)))) if *value == 0)
    );
    assert!(
        matches!(session.read(Serial(6), request(59, &calls), ReadOptions::default()).map_err(|r|r.reason).unwrap(), Submission::Ready(Ok(Outcome::Success(value))) if *value == 59)
    );
    session.close(wait_deadline()).unwrap();
    for (key, ticket) in tickets.iter_mut().enumerate().skip(1) {
        assert!(
            matches!(session.wait(ticket, wait_deadline()).unwrap(), Ok(Outcome::Success(value)) if *value == key as u64)
        );
    }
    store.shutdown(wait_deadline()).unwrap();
}

#[test]
fn concurrency_shutdown_allows_only_one_pusher_and_another_call_immediately_returns_busy() {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, probe, _) = shutdown_setup();
    probe.paused.store(true, SeqCst);
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || worker_store.shutdown(wait_deadline()));
    let deadline = wait_deadline();
    while probe.submitted.load(SeqCst) == 0 {
        assert!(
            !deadline.expired(),
            "Closing the thread should start submitting to the background I/O"
        );
        std::thread::yield_now();
    }
    let concurrent = store.shutdown(wait_deadline());
    probe.paused.store(false, SeqCst);
    assert!(matches!(concurrent, Err(Error::Busy)));
    assert!(worker.join().unwrap().unwrap().device_drained);
    assert_eq!(probe.shutdowns.load(SeqCst), 1);
    assert_eq!(probe.submitted.load(SeqCst), probe.returned.load(SeqCst));
}

#[test]
fn real_suspended_reading_retains_the_old_identity_and_serial_number_after_cross_version_splitting_and_completes_it_independently()
 {
    let mut store = setup();
    let config = &mut Arc::get_mut(&mut store.inner).unwrap().config;
    config.session.max_pending = 2;
    config.session.max_results = 2;
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut old_ticket) = session
        .read(Serial(7), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    let old_id = old_ticket.id();
    let action = begin_test_cut(&store, &mut [&mut session]);
    assert!(
        session
            .runtime
            .switch_version(CheckpointVersion(1))
            .unwrap()
    );
    let old = session.runtime.previous.as_ref().unwrap();
    let task = old.tasks.values().next().unwrap();
    assert_eq!(task.id(), old_id);
    assert_eq!(task.version(), CheckpointVersion(0));
    assert_eq!(task.serial(), Serial(7));
    assert_eq!(old.last_accepted, Some(Serial(7)));
    assert!(session.runtime.current.tasks.is_empty());
    assert!(matches!(
        session.runtime.switch_version(CheckpointVersion(2)),
        Err(Error::Busy)
    ));
    assert_eq!(session.runtime.current.version, CheckpointVersion(1));
    let Submission::Pending(mut new_ticket) = session
        .read(Serial(9), request(1, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    assert_eq!(
        session
            .runtime
            .current
            .tasks
            .values()
            .next()
            .unwrap()
            .version(),
        CheckpointVersion(1)
    );
    assert_eq!(
        session
            .runtime
            .cut(CheckpointVersion(0))
            .unwrap()
            .last_accepted,
        Some(Serial(7))
    );
    assert_eq!(session.last_accepted(), Some(Serial(9)));
    assert_eq!(session.runtime.pending(), 2);
    assert!(
        matches!(session.wait(&mut new_ticket,wait_deadline()).unwrap(),Ok(Outcome::Success(value)) if *value==1)
    );
    assert!(
        matches!(session.wait(&mut old_ticket,wait_deadline()).unwrap(),Ok(Outcome::Success(value)) if *value==0)
    );
    assert_eq!(calls.get(), 2);
    assert_eq!(
        session
            .runtime
            .cut(CheckpointVersion(0))
            .unwrap()
            .old_pending,
        0
    );
    assert_eq!(
        session
            .runtime
            .cut(CheckpointVersion(0))
            .unwrap()
            .last_accepted,
        Some(Serial(7))
    );
    finish_test_cut(&store, action, &mut [&mut session]);
    assert!(
        !session
            .runtime
            .switch_version(CheckpointVersion(1))
            .unwrap()
    );
    assert!(
        session
            .runtime
            .switch_version(CheckpointVersion(3))
            .is_err()
    );
    assert!(
        session
            .runtime
            .switch_version(CheckpointVersion(2))
            .unwrap()
    );
    assert_eq!(
        session.runtime.previous.as_ref().unwrap().version,
        CheckpointVersion(1)
    );
    assert_eq!(
        session
            .runtime
            .cut(CheckpointVersion(1))
            .unwrap()
            .last_accepted,
        Some(Serial(9))
    );
    assert!(session.runtime.cut(CheckpointVersion(0)).is_err());
    session.close(wait_deadline()).unwrap();
    store.shutdown(wait_deadline()).unwrap();
}
#[test]
fn after_the_maintenance_version_is_advanced_the_new_registration_session_inherits_the_atomically_returned_version()
 {
    use crate::coordination::{Action, Phase};
    let store = setup();
    let coordinator = &store.inner.coordinator;
    let id = coordinator.start_action(Action::CheckpointLog).unwrap();
    for phase in [
        Phase::Prepare,
        Phase::InProgress,
        Phase::WaitPending,
        Phase::WaitFlush,
    ] {
        coordinator.advance(id, phase).unwrap();
    }
    coordinator.finish_action(id).unwrap();
    // Only the coordinator component is driven here,No checkpoint written,There is no public success checkpoint API.
    let session = store.start_session(SessionOptions::default()).unwrap();
    assert_eq!(session.runtime.current.version, CheckpointVersion(1));
    assert!(session.runtime.previous.is_none());
}

#[test]
fn gracefully_close_sessions_that_preserve_pre_action_and_in_action_segmentation_without_faking_abandon_failures()
 {
    use crate::coordination::{Action, Phase};
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let first_id = first.id();
    let calls = Rc::new(Cell::new(0));
    first
        .read(Serial(7), request(59, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    first.close(wait_deadline()).unwrap();
    let mut second = store.start_session(SessionOptions::default()).unwrap();
    let second_id = second.id();
    second
        .read(Serial(11), request(59, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    let coordinator = &store.inner.coordinator;
    let id = coordinator.start_action(Action::CheckpointLog).unwrap();
    assert!(matches!(
        coordinator.advance(id, Phase::Prepare),
        Err(Error::Busy)
    ));
    second.close(wait_deadline()).unwrap();
    assert!(coordinator.action_failure(id).unwrap().is_none());
    for phase in [Phase::Prepare, Phase::InProgress, Phase::WaitPending] {
        coordinator.advance(id, phase).unwrap();
    }
    let cuts = coordinator.cuts(id).unwrap();
    assert!(cuts.iter().any(|cut| cut.session == first_id
        && cut.last_accepted == Some(Serial(7))
        && cut.old_pending == 0));
    assert!(cuts.iter().any(|cut| cut.session == second_id
        && cut.last_accepted == Some(Serial(11))
        && cut.old_pending == 0));
    coordinator.advance(id, Phase::WaitFlush).unwrap();
    coordinator.finish_action(id).unwrap();
    store.shutdown(wait_deadline()).unwrap();
}

#[test]
fn the_new_sequence_number_accepted_after_splitting_does_not_enter_the_old_checkpoint_and_closes_splitting()
 {
    use crate::coordination::{Action, Phase};
    let mut store = setup();
    let config = &mut Arc::get_mut(&mut store.inner).unwrap().config;
    config.session.max_pending = 2;
    config.session.max_results = 2;
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = session
        .read(Serial(7), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    let coordinator = &store.inner.coordinator;
    let id = coordinator.start_action(Action::CheckpointLog).unwrap();
    coordinator
        .acknowledge(
            id,
            session.runtime.cut(CheckpointVersion(0)).unwrap(),
            Phase::Prepare,
        )
        .unwrap();
    coordinator.advance(id, Phase::Prepare).unwrap();
    session
        .runtime
        .switch_version(CheckpointVersion(1))
        .unwrap();
    coordinator
        .acknowledge(
            id,
            session.runtime.cut(CheckpointVersion(0)).unwrap(),
            Phase::InProgress,
        )
        .unwrap();
    coordinator.advance(id, Phase::InProgress).unwrap();
    assert!(matches!(
        session
            .read(Serial(9), request(59, &calls), ReadOptions::default())
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(_))
    ));
    session.close(wait_deadline()).unwrap();
    assert!(
        matches!(session.wait(&mut ticket,wait_deadline()).unwrap(),Ok(Outcome::Success(value)) if *value==0)
    );
    coordinator.advance(id, Phase::WaitPending).unwrap();
    assert!(
        coordinator
            .cuts(id)
            .unwrap()
            .iter()
            .any(|cut| cut.session == session.id()
                && cut.last_accepted == Some(Serial(7))
                && cut.old_pending == 0)
    );
    coordinator.advance(id, Phase::WaitFlush).unwrap();
    coordinator.finish_action(id).unwrap();
    let next = coordinator.start_action(Action::CheckpointLog).unwrap();
    assert!(
        coordinator
            .cuts(next)
            .unwrap()
            .iter()
            .any(|cut| cut.session == session.id() && cut.last_accepted == Some(Serial(9)))
    );
}

#[test]
fn new_version_replacement_and_read_modify_do_not_modify_the_old_record_in_place_but_the_same_version_can_still_be_updated()
 {
    use std::sync::atomic::Ordering::SeqCst;
    struct Write {
        key: u64,
        value: u64,
        updates: Rc<Cell<usize>>,
    }
    impl Keyed<Schema> for Write {
        fn key(&self) -> &u64 {
            &self.key
        }
    }
    impl UpsertOperation<Schema> for Write {
        type Output = ();
        fn replacement(&mut self) -> Result<(u64, ()), Error> {
            Ok((self.value, ()))
        }
        fn update_in_place(
            &mut self,
            mut value: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<()>, Error> {
            self.updates.set(self.updates.get() + 1);
            value.view_mut().store(self.value, SeqCst);
            Ok(UpdateDecision::Updated(()))
        }
    }
    impl RmwOperation<Schema> for Write {
        type Output = ();
        fn initial(&mut self) -> Result<(u64, ()), Error> {
            Ok((self.value, ()))
        }
        fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, ()), Error> {
            Ok((value.view().wrapping_add(self.value), ()))
        }
        fn update_in_place(
            &mut self,
            mut value: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<()>, Error> {
            self.updates.set(self.updates.get() + 1);
            value.view_mut().fetch_add(self.value, SeqCst);
            Ok(UpdateDecision::Updated(()))
        }
    }
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let locate = |key| {
        let entry = store
            .inner
            .index
            .prepare(crate::schema::KeyCodec::hash(&U64Key, &key))
            .unwrap();
        let head = super::Engine::<Schema>::head(entry).unwrap();
        store.inner.log.find(&U64Key, &key, head).unwrap().unwrap()
    };
    let old_put = locate(59);
    let old_rmw = locate(58);
    let _action = begin_test_cut(&store, &mut [&mut session]);
    session
        .runtime
        .switch_version(CheckpointVersion(1))
        .unwrap();
    let updates = Rc::new(Cell::new(0));
    assert!(matches!(
        session
            .upsert(
                Serial(0),
                Write {
                    key: 59,
                    value: 100,
                    updates: updates.clone()
                }
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert!(matches!(
        session
            .rmw(
                Serial(1),
                Write {
                    key: 58,
                    value: 1,
                    updates: updates.clone()
                },
                RmwOptions::default()
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert_eq!(updates.get(), 0);
    assert_eq!(old_put.version(), CheckpointVersion(0));
    assert_eq!(old_put.read(|v| v).unwrap(), 59);
    assert_eq!(old_rmw.read(|v| v).unwrap(), 58);
    let new_put = locate(59);
    assert_eq!(new_put.version(), CheckpointVersion(1));
    assert_eq!(locate(58).version(), CheckpointVersion(1));
    assert_eq!(locate(58).read(|v| v).unwrap(), 59);
    assert!(matches!(
        session
            .upsert(
                Serial(2),
                Write {
                    key: 59,
                    value: 200,
                    updates: updates.clone()
                }
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert!(matches!(
        session
            .rmw(
                Serial(3),
                Write {
                    key: 58,
                    value: 2,
                    updates: updates.clone()
                },
                RmwOptions::default()
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert_eq!(updates.get(), 2);
    assert_eq!(new_put.read(|v| v).unwrap(), 200);
    assert_eq!(old_put.read(|v| v).unwrap(), 59);
    assert_eq!(old_rmw.read(|v| v).unwrap(), 58);
}

type NumberActor = crate::engine::session_actor::Actor<(
    crate::Session<Schema>,
    Option<crate::Ticket<u64>>,
    Rc<Cell<usize>>,
)>;
fn number_actor(store: &RasterKV<Schema>) -> NumberActor {
    let store = store.clone();
    crate::engine::session_actor::Actor::new(move || {
        (
            store.start_session(Default::default()).unwrap(),
            None,
            Rc::new(Cell::new(0)),
        )
    })
}
fn begin_actor_cut(
    store: &RasterKV<Schema>,
    old: &mut crate::Session<Schema>,
    new: &NumberActor,
) -> MaintenanceId {
    let id = store
        .inner
        .coordinator
        .start_action(crate::coordination::Action::CheckpointLog)
        .unwrap();
    old.refresh().unwrap();
    new.call(|(session, _, _)| session.refresh().unwrap());
    store
        .inner
        .coordinator
        .advance(id, crate::coordination::Phase::Prepare)
        .unwrap();
    id
}
#[test]
fn four_operations_wait_for_the_old_version_of_the_same_key_to_be_terminated_and_read_to_re_obtain_the_link_head_after_permission()
 {
    struct NumberRead {
        key: u64,
        calls: Rc<Cell<usize>>,
    }
    impl Keyed<Schema> for NumberRead {
        fn key(&self) -> &u64 {
            &self.key
        }
    }
    impl ReadOperation<Schema> for NumberRead {
        type Output = u64;
        fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
            self.calls.set(self.calls.get() + 1);
            Ok(*value.view())
        }
    }
    for operation in 0..4 {
        let store = setup();
        let mut old = store.start_session(Default::default()).unwrap();
        let new = number_actor(&store);
        let old_calls = Rc::new(Cell::new(0));
        let Submission::Pending(mut old_ticket) = old
            .rmw(
                Serial(0),
                Add {
                    key: 0,
                    delta: 1,
                    copies: old_calls.clone(),
                    initials: Rc::new(Cell::new(0)),
                },
                RmwOptions::default(),
            )
            .map_err(|r| r.reason)
            .unwrap()
        else {
            panic!("Old requests should wait for disk reads");
        };
        let _action = begin_actor_cut(&store, &mut old, &new);
        new.call(move |(new, ticket, new_calls)| {
            new.runtime.switch_version(CheckpointVersion(1)).unwrap();
            let submission = match operation {
                0 => new
                    .read(
                        Serial(0),
                        NumberRead {
                            key: 0,
                            calls: new_calls.clone(),
                        },
                        ReadOptions::default(),
                    )
                    .map_err(|r| r.reason),
                1 => new
                    .upsert(
                        Serial(0),
                        CountedPut {
                            key: 0,
                            value: 100,
                            calls: new_calls.clone(),
                        },
                    )
                    .map_err(|r| r.reason),
                2 => new
                    .rmw(
                        Serial(0),
                        Add {
                            key: 0,
                            delta: 2,
                            copies: new_calls.clone(),
                            initials: Rc::new(Cell::new(0)),
                        },
                        RmwOptions::default(),
                    )
                    .map_err(|r| r.reason),
                _ => new
                    .delete(
                        Serial(0),
                        Erase {
                            key: 0,
                            calls: new_calls.clone(),
                            panic: false,
                        },
                        DeleteOptions::default(),
                    )
                    .map_err(|r| r.reason),
            }
            .unwrap();
            let Submission::Pending(mut submitted) = submission else {
                panic!("The new version must wait for the old version to be licensed");
            };
            for _ in 0..5 {
                new.poll(PollBudget::default()).unwrap();
            }
            assert_eq!(new_calls.get(), 0);
            assert!(matches!(submitted.try_take(), Ok(TicketState::Pending)));
            *ticket = Some(submitted);
        });
        assert_eq!(old_calls.get(), 0);
        assert!(matches!(
            old.wait(&mut old_ticket, wait_deadline()).unwrap(),
            Ok(Outcome::Success(1))
        ));
        assert_eq!(old_calls.get(), 1);
        let old_head = super::Engine::<Schema>::head(
            store
                .inner
                .index
                .prepare(crate::schema::KeyCodec::hash(&U64Key, &0))
                .unwrap(),
        )
        .unwrap();
        let old_value = store
            .inner
            .log
            .find(&U64Key, &0, old_head)
            .unwrap()
            .unwrap();
        let expected = [1, 100, 3, 0][operation];
        new.call(move |(new, ticket, new_calls)| {
            assert!(matches!(new.wait(ticket.as_mut().unwrap(), wait_deadline()).unwrap(), Ok(Outcome::Success(value)) if value == expected));
            assert_eq!(new_calls.get(), 1);
        });
        assert_eq!(old_value.read(|v| v).unwrap(), 1);
    }
}
#[test]
fn discarding_old_tickets_does_not_release_permission_and_phase_abandonment_causes_new_requests_to_fail_and_terminate()
 {
    let store = setup();
    let mut old = store.start_session(Default::default()).unwrap();
    let new = number_actor(&store);
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(ticket) = old
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending");
    };
    drop(ticket);
    let _action = begin_actor_cut(&store, &mut old, &new);
    new.call(|(new, ticket, replacements)| {
        new.runtime.switch_version(CheckpointVersion(1)).unwrap();
        let Submission::Pending(submitted) = new
            .upsert(
                Serial(0),
                CountedPut {
                    key: 0,
                    value: 9,
                    calls: replacements.clone(),
                },
            )
            .map_err(|r| r.reason)
            .unwrap()
        else {
            panic!("Should wait for old requests");
        };
        *ticket = Some(submitted);
        new.poll(PollBudget::default()).unwrap();
        assert_eq!(replacements.get(), 0);
    });
    drop(old);
    new.call(|(new, ticket, replacements)| {
        let ticket = ticket.as_mut().unwrap();
        assert!(matches!(
            new.wait(ticket, wait_deadline()),
            Err(Error::InvalidState(_))
        ));
        assert!(matches!(ticket.try_take(), Ok(TicketState::Ready(Err(_)))));
        assert_eq!(replacements.get(), 0);
    });
    assert_eq!(calls.get(), 0);
}

// Testing only drives the coordination phase,Not writing checkpoint material or claiming persistence was successful.
fn begin_test_cut(
    store: &RasterKV<Schema>,
    sessions: &mut [&mut crate::Session<Schema>],
) -> MaintenanceId {
    let id = store
        .inner
        .coordinator
        .start_action(crate::coordination::Action::CheckpointLog)
        .unwrap();
    for session in sessions {
        session.refresh().unwrap();
    }
    store
        .inner
        .coordinator
        .advance(id, crate::coordination::Phase::Prepare)
        .unwrap();
    id
}
fn finish_test_cut(
    store: &RasterKV<Schema>,
    id: MaintenanceId,
    sessions: &mut [&mut crate::Session<Schema>],
) {
    use crate::coordination::Phase;
    for session in sessions.iter_mut() {
        session.refresh().unwrap();
    }
    store
        .inner
        .coordinator
        .advance(id, Phase::InProgress)
        .unwrap();
    for session in sessions.iter_mut() {
        session.refresh().unwrap();
    }
    store
        .inner
        .coordinator
        .advance(id, Phase::WaitPending)
        .unwrap();
    store
        .inner
        .coordinator
        .advance(id, Phase::WaitFlush)
        .unwrap();
    store.inner.coordinator.finish_action(id).unwrap();
}

#[test]
fn commits_and_polls_automatically_observe_versions_and_old_requests_remain_sharded() {
    let store = setup();
    let mut old = store.start_session(Default::default()).unwrap();
    let new = number_actor(&store);
    let new_id = new.call(|(session, _, _)| session.id());
    let copies = Rc::new(Cell::new(0));
    let Submission::Pending(mut old_ticket) = old
        .rmw(
            Serial(7),
            Add {
                key: 0,
                delta: 1,
                copies: copies.clone(),
                initials: Rc::new(Cell::new(0)),
            },
            RmwOptions::default(),
        )
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("Old requests should be suspended");
    };
    let id = begin_actor_cut(&store, &mut old, &new);
    new.call(|(new, ticket, replacements)| {
        assert_eq!(new.runtime.current.version, CheckpointVersion(0));
        let Submission::Pending(submitted) = new
            .upsert(
                Serial(9),
                CountedPut {
                    key: 0,
                    value: 100,
                    calls: replacements.clone(),
                },
            )
            .map_err(|r| r.reason)
            .unwrap()
        else {
            panic!("New requests should wait for old requests");
        };
        *ticket = Some(submitted);
        assert_eq!(new.runtime.current.version, CheckpointVersion(1));
        assert_eq!(new.runtime.previous.as_ref().unwrap().last_accepted, None);
        assert_eq!(replacements.get(), 0);
    });
    assert!(matches!(
        old.wait(&mut old_ticket, wait_deadline()).unwrap(),
        Ok(Outcome::Success(1))
    ));
    assert_eq!(copies.get(), 1);
    assert_eq!(old.runtime.current.version, CheckpointVersion(1));
    assert_eq!(
        old.runtime.cut(CheckpointVersion(0)).unwrap().last_accepted,
        Some(Serial(7))
    );
    new.call(|(new, ticket, replacements)| {
        assert!(matches!(
            new.wait(ticket.as_mut().unwrap(), wait_deadline()).unwrap(),
            Ok(Outcome::Success(100))
        ));
        assert_eq!(replacements.get(), 1);
    });
    use crate::coordination::Phase;
    store
        .inner
        .coordinator
        .advance(id, Phase::InProgress)
        .unwrap();
    old.refresh().unwrap();
    new.call(|(new, _, _)| new.refresh().unwrap());
    store
        .inner
        .coordinator
        .advance(id, Phase::WaitPending)
        .unwrap();
    let cuts = store.inner.coordinator.cuts(id).unwrap();
    assert!(cuts.iter().any(|cut| cut.session == old.id()
        && cut.last_accepted == Some(Serial(7))
        && cut.old_pending == 0));
    assert!(
        cuts.iter().any(|cut| cut.session == new_id
            && cut.last_accepted.is_none()
            && cut.old_pending == 0)
    );
    store
        .inner
        .coordinator
        .advance(id, Phase::WaitFlush)
        .unwrap();
    store.inner.coordinator.finish_action(id).unwrap();
    old.close(wait_deadline()).unwrap();
    new.call(|(new, _, _)| new.close(wait_deadline()).unwrap());
    store.shutdown(wait_deadline()).unwrap();
}
#[test]
fn concurrent_phase_advancement_and_session_confirmation_are_interleaved_without_false_reporting_of_late_failures()
 {
    use crate::coordination::{Action, Phase};
    for _ in 0..16 {
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .device(Box::new(null::NullDeviceFactory))
            .create()
            .unwrap();
        let ready = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let mut workers = vec![];
            for _ in 0..2 {
                let store = &store;
                let ready = &ready;
                workers.push(scope.spawn(move || {
                    let mut session = store.start_session(SessionOptions::default()).unwrap();
                    ready.wait();
                    let deadline = wait_deadline();
                    loop {
                        assert!(
                            !deadline.expired(),
                            "Stages should be advanced within the deadline"
                        );
                        session.refresh().unwrap();
                        if store.inner.coordinator.snapshot().unwrap().phase == Phase::WaitFlush {
                            break;
                        }
                        std::thread::yield_now();
                    }
                    assert_eq!(session.runtime.current.version, CheckpointVersion(1));
                    session.close(wait_deadline()).unwrap();
                }));
            }
            ready.wait();
            let id = store
                .inner
                .coordinator
                .start_action(Action::CheckpointLog)
                .unwrap();
            for phase in [Phase::Prepare, Phase::InProgress, Phase::WaitPending] {
                let deadline = wait_deadline();
                loop {
                    match store.inner.coordinator.advance(id, phase) {
                        Ok(_) => break,
                        Err(Error::Busy) => {
                            assert!(!deadline.expired(), "Participants must confirm stage");
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("Stage advancement error:{error}"),
                    }
                }
            }
            for worker in workers {
                worker.join().unwrap();
            }
            assert!(
                store
                    .inner
                    .coordinator
                    .action_failure(id)
                    .unwrap()
                    .is_none()
            );
            store
                .inner
                .coordinator
                .advance(id, Phase::WaitFlush)
                .unwrap();
            store.inner.coordinator.finish_action(id).unwrap();
        });
        store.shutdown(wait_deadline()).unwrap();
    }
}

#[test]
fn maintenance_waits_to_advance_its_own_old_request_while_idle_participants_must_acknowledge_or_exit()
 {
    use crate::{
        api::maintenance::MaintenanceTicket,
        coordination::{Action, Phase},
    };
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let idle = crate::engine::session_actor::session(&store);
    let idle_id = idle.call(|session| session.id());
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut request_ticket) = session
        .rmw(
            Serial(7),
            Add {
                key: 0,
                delta: 1,
                copies: calls.clone(),
                initials: Rc::new(Cell::new(0)),
            },
            RmwOptions::default(),
        )
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    let id = store
        .inner
        .coordinator
        .start_action(Action::CheckpointLog)
        .unwrap();
    let (ticket, complete) = MaintenanceTicket::<()>::pair(store.inner.id, id);
    // The maintenance thread can deliver the device to complete,But cannot perform user callback or confirmation phase on behalf of session.
    store.maintenance().poll(PollBudget::default()).unwrap();
    assert_eq!(calls.get(), 0);
    let deadline = || Deadline(std::time::Instant::now() + std::time::Duration::from_millis(100));
    assert!(matches!(
        session.wait_maintenance(&ticket, deadline()),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(calls.get(), 1);
    assert_eq!(
        store.inner.coordinator.snapshot().unwrap().phase,
        Phase::Prepare
    );
    assert!(matches!(
        request_ticket.try_take(),
        Ok(TicketState::Ready(Ok(Outcome::Success(1))))
    ));
    idle.call(|session| session.close(wait_deadline()).unwrap());
    assert!(matches!(
        session.wait_maintenance(&ticket, deadline()),
        Err(Error::DeadlineExceeded)
    ));
    assert_eq!(
        store.inner.coordinator.snapshot().unwrap().phase,
        Phase::WaitFlush
    );
    assert_eq!(session.runtime.current.version, CheckpointVersion(1));
    assert!(ticket.try_report().unwrap().is_none());
    let progress = store.maintenance().poll(PollBudget::default()).unwrap();
    assert_eq!(progress.remaining, 1);
    assert_eq!(progress.completed, 0);
    assert!(!progress.phase_advanced);
    let cuts = store.inner.coordinator.cuts(id).unwrap();
    assert!(
        cuts.iter()
            .any(|cut| cut.session == session.id() && cut.last_accepted == Some(Serial(7)))
    );
    assert!(
        cuts.iter()
            .any(|cut| cut.session == idle_id && cut.last_accepted.is_none())
    );
    // Component testing ends with failure report;Don't fake the success checkpoint without writing material.
    store
        .inner
        .coordinator
        .fail_action(id, Error::Codec("Component testing terminated"))
        .unwrap();
    let report = complete
        .finish(Err(Error::Codec("Component testing terminated")))
        .unwrap();
    let received = session
        .wait_maintenance(&ticket, Deadline(std::time::Instant::now()))
        .unwrap();
    assert!(Arc::ptr_eq(&report, &received));
    assert!(matches!(
        &*received,
        Err(Error::Codec("Component testing terminated"))
    ));
    assert!(complete.finish(Ok(())).is_err());
    assert!(Arc::ptr_eq(
        &received,
        &session.wait_maintenance(&ticket, wait_deadline()).unwrap()
    ));
    session.close(wait_deadline()).unwrap();
    store.shutdown(wait_deadline()).unwrap();
}
#[test]
fn wrong_storage_maintenance_ticket_cannot_drive_this_session_even_if_there_is_a_report() {
    use crate::{api::maintenance::MaintenanceTicket, coordination::Action};
    let first = setup();
    let second = setup();
    let first_id = first
        .inner
        .coordinator
        .start_action(Action::CheckpointLog)
        .unwrap();
    let (ticket, complete) = MaintenanceTicket::<()>::pair(first.inner.id, first_id);
    complete
        .finish(Err(Error::Codec("component report")))
        .unwrap();
    let mut session = second.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(_request) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("should_pending")
    };
    assert!(matches!(
        session.wait_maintenance(&ticket, wait_deadline()),
        Err(Error::InvalidState(_))
    ));
    assert_eq!(calls.get(), 0);
}
#[test]
fn the_maintenance_driver_stops_at_a_stage_that_requires_actual_materials_and_the_completion_end_gives_up_with_a_failure_report()
 {
    use crate::{
        api::maintenance::MaintenanceTicket,
        coordination::{Action, Phase},
    };
    for (action, expected) in [
        (Action::CheckpointFull, Phase::IndexSnapshot),
        (Action::CheckpointIndex, Phase::IndexSnapshot),
        (Action::CheckpointLog, Phase::WaitFlush),
        (Action::Gc, Phase::GcIo),
        (Action::GrowIndex, Phase::GrowCopy),
    ] {
        let store = setup();
        let id = store.inner.coordinator.start_action(action).unwrap();
        let (ticket, complete) = MaintenanceTicket::<()>::pair(store.inner.id, id);
        store.maintenance().poll(PollBudget::default()).unwrap();
        assert_eq!(store.inner.coordinator.snapshot().unwrap().phase, expected);
        assert_eq!(
            store
                .maintenance()
                .poll(PollBudget::default())
                .unwrap()
                .remaining,
            1
        );
        assert!(ticket.try_report().unwrap().is_none());
        drop(complete);
        assert!(matches!(
            &*ticket.try_report().unwrap().unwrap(),
            Err(Error::InvalidState(_))
        ));
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn native_file_pending_requests_are_completed_in_maintenance_wait_and_old_version_shards_are_retained()
 {
    use crate::{
        api::maintenance::MaintenanceTicket,
        coordination::{Action, Phase},
    };
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-phase-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..60 {
        assert!(matches!(
            session
                .upsert(Serial(key), Put(key))
                .map_err(|r| r.reason)
                .unwrap(),
            Submission::Ready(Ok(_))
        ));
    }
    let deadline = wait_deadline();
    while store.inner.log.frontiers().unwrap().safe_head.0 < 4096 {
        assert!(
            !deadline.expired(),
            "Old pages must be actually written out and retired"
        );
        session.poll(PollBudget::default()).unwrap();
        std::thread::yield_now();
    }
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut request_ticket) = session
        .rmw(
            Serial(60),
            Add {
                key: 0,
                delta: 1,
                copies: calls.clone(),
                initials: Rc::new(Cell::new(0)),
            },
            RmwOptions::default(),
        )
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("Old records should be read in from native files")
    };
    let id = store
        .inner
        .coordinator
        .start_action(Action::CheckpointLog)
        .unwrap();
    let (ticket, complete) = MaintenanceTicket::<()>::pair(store.inner.id, id);
    let overall = wait_deadline();
    loop {
        assert!(
            !overall.expired(),
            "Maintenance waits should advance native file requests"
        );
        let slice = Deadline(
            overall
                .0
                .min(std::time::Instant::now() + std::time::Duration::from_millis(20)),
        );
        assert!(matches!(
            session.wait_maintenance(&ticket, slice),
            Err(Error::DeadlineExceeded)
        ));
        if store.inner.coordinator.snapshot().unwrap().phase == Phase::WaitFlush {
            break;
        }
    }
    assert_eq!(
        store.inner.coordinator.snapshot().unwrap().phase,
        Phase::WaitFlush
    );
    assert_eq!(calls.get(), 1);
    assert!(matches!(
        request_ticket.try_take(),
        Ok(TicketState::Ready(Ok(Outcome::Success(1))))
    ));
    assert_eq!(session.runtime.current.version, CheckpointVersion(1));
    assert!(matches!(
        session
            .upsert(
                Serial(61),
                CountedPut {
                    key: 0,
                    value: 100,
                    calls: Rc::new(Cell::new(0))
                }
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert!(
        matches!(session.read(Serial(62),request(0,&calls),ReadOptions::default()).map_err(|r|r.reason).unwrap(),Submission::Ready(Ok(Outcome::Success(value))) if *value==100)
    );
    assert!(
        store
            .inner
            .coordinator
            .cuts(id)
            .unwrap()
            .iter()
            .any(|cut| cut.session == session.id()
                && cut.last_accepted == Some(Serial(60))
                && cut.old_pending == 0)
    );
    assert!(ticket.try_report().unwrap().is_none());
    store
        .inner
        .coordinator
        .fail_action(id, Error::Codec("Component acceptance ends"))
        .unwrap();
    complete
        .finish(Err(Error::Codec("Component acceptance ends")))
        .unwrap();
    session.close(wait_deadline()).unwrap();
    store.shutdown(wait_deadline()).unwrap();
}
