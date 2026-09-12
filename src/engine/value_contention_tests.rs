//! Creating page flush contention with real value permissions;by public Session/Ticket Validation wait and user error boundaries.
use crate::{
    RasterKV, Submission,
    api::{Outcome, completion::TicketState, operation::*},
    config::Config,
    schema::{
        KeyCodec, ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{atomic::Ordering, mpsc},
    time::{Duration, Instant},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Request {
    value: u64,
    calls: Rc<Cell<usize>>,
    busy: bool,
}
impl Request {
    fn called(&self) -> Result<(), Error> {
        self.calls.set(self.calls.get() + 1);
        if self.busy { Err(Error::Busy) } else { Ok(()) }
    }
}
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &1
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        self.called()?;
        Ok(*v.view())
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        self.called()?;
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.called()?;
        v.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(self.value))
    }
}
impl RmwOperation<Schema> for Request {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        self.called()?;
        Ok((self.value, self.value))
    }
    fn copy_update(&mut self, v: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.called()?;
        let next = v.view().wrapping_add(self.value);
        Ok((next, next))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        self.called()?;
        Ok(UpdateDecision::Updated(
            v.view_mut()
                .fetch_add(self.value, Ordering::SeqCst)
                .wrapping_add(self.value),
        ))
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Read,
    Upsert,
    Rmw,
    RmwCopy,
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(5))
}
fn setup() -> (RasterKV<Schema>, crate::Session<Schema>) {
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 8;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let put = Request {
        value: 10,
        calls: Rc::new(Cell::new(0)),
        busy: false,
    };
    assert!(matches!(
        session
            .upsert(Serial(0), put)
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(10)))
    ));
    (store, session)
}
fn submit(
    mode: Mode,
    session: &mut crate::Session<Schema>,
    request: Request,
) -> Result<Submission<u64>, Rejected<Request>> {
    match mode {
        Mode::Read => session.read(Serial(1), request, ReadOptions::default()),
        Mode::Upsert => session.upsert(Serial(1), request),
        Mode::Rmw | Mode::RmwCopy => session.rmw(Serial(1), request, RmwOptions::default()),
    }
}
fn contention(mode: Mode) {
    let (store, mut session) = setup();
    if matches!(mode, Mode::RmwCopy) {
        let end = store.inner.log.pad_tail().unwrap();
        store.inner.log.advance_read_only(end).unwrap();
    }
    let entry = store.inner.index.prepare(U64Key.hash(&1)).unwrap();
    let crate::index::IndexHead::Log(address) = entry.head else {
        panic!("Already logged")
    };
    let calls = Rc::new(Cell::new(0));
    let (submission, pending, before, failed) = std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let holder = scope.spawn(move || {
            // Same as the stable encoding of brush page,Exclusive value license does not occupy business stripe lock.
            let lease = owner.inner.log.lease(address).unwrap();
            lease
                .read(|_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                })
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut submission = submit(
            mode,
            &mut session,
            Request {
                value: 20,
                calls: calls.clone(),
                busy: false,
            },
        );
        let pending = match &mut submission {
            Ok(Submission::Pending(ticket)) => {
                matches!(ticket.try_take(), Ok(TicketState::Pending))
            }
            _ => false,
        };
        let before = calls.get();
        let failed = store.diagnostics().unwrap().failed;
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        (submission, pending, before, failed)
    });
    assert!(
        pending,
        "Internal license contention must keep original request pending,Cannot be used as a business Busy end"
    );
    assert_eq!(before, 0);
    assert!(
        !failed,
        "Contention that does not call user code cannot fail to close"
    );
    let Submission::Pending(mut ticket) = submission.map_err(|r| r.reason).unwrap() else {
        unreachable!()
    };
    let result = session.wait(&mut ticket, deadline()).unwrap().unwrap();
    let expected = match mode {
        Mode::Read => 10,
        Mode::Upsert => 20,
        Mode::Rmw | Mode::RmwCopy => 30,
    };
    assert!(matches!(result,Outcome::Success(n) if n==expected));
    assert_eq!(calls.get(), 1);
    assert!(ticket.try_take().is_err());
    session.poll(PollBudget::default()).unwrap();
    assert_eq!(calls.get(), 1);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn freeze_source_read_contention_makes_reads_modifications_and_writes_wait_and_only_count_once_after_release()
 {
    contention(Mode::RmwCopy)
}
#[test]
fn resident_read_contention_does_not_return_business_busy_error() {
    contention(Mode::Read)
}
#[test]
fn in_place_write_permission_contention_fails_to_close_without_error() {
    contention(Mode::Upsert)
}
#[test]
fn in_place_read_modify_write_permission_contention_without_error_failed_to_close() {
    contention(Mode::Rmw)
}
#[test]
fn busy_errors_for_user_read_and_copy_callbacks_terminate_once_and_not_replayed() {
    for mode in [Mode::Read, Mode::RmwCopy] {
        let (store, mut session) = setup();
        if matches!(mode, Mode::RmwCopy) {
            let end = store.inner.log.pad_tail().unwrap();
            store.inner.log.advance_read_only(end).unwrap();
        }
        let calls = Rc::new(Cell::new(0));
        let submission = submit(
            mode,
            &mut session,
            Request {
                value: 20,
                calls: calls.clone(),
                busy: true,
            },
        )
        .map_err(|r| r.reason)
        .unwrap();
        assert!(matches!(
            submission,
            Submission::Ready(Err(OperationError {
                cause: Error::Busy,
                effect: Effect::NotApplied
            }))
        ));
        session.poll(PollBudget::default()).unwrap();
        assert_eq!(calls.get(), 1);
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}

#[derive(Default)]
struct DropCounts {
    calls: Cell<usize>,
    requests: Cell<usize>,
    outputs: Cell<usize>,
}
struct OwnedOutput {
    value: u64,
    counts: Rc<DropCounts>,
}
impl Drop for OwnedOutput {
    fn drop(&mut self) {
        self.counts.outputs.set(self.counts.outputs.get() + 1);
    }
}
struct OwnedPut {
    value: u64,
    counts: Rc<DropCounts>,
}
impl Drop for OwnedPut {
    fn drop(&mut self) {
        self.counts.requests.set(self.counts.requests.get() + 1);
    }
}
impl Keyed<Schema> for OwnedPut {
    fn key(&self) -> &u64 {
        &1
    }
}
impl UpsertOperation<Schema> for OwnedPut {
    type Output = OwnedOutput;
    fn replacement(&mut self) -> Result<(u64, OwnedOutput), Error> {
        self.counts.calls.set(self.counts.calls.get() + 1);
        Ok((
            self.value,
            OwnedOutput {
                value: self.value,
                counts: self.counts.clone(),
            },
        ))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<OwnedOutput>, Error> {
        self.counts.calls.set(self.counts.calls.get() + 1);
        value.view_mut().store(self.value, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(OwnedOutput {
            value: self.value,
            counts: self.counts.clone(),
        }))
    }
}
fn owned_result(discard: bool) {
    let mut config = Config::default();
    config.session.max_pending = 1;
    config.session.max_results = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session
            .upsert(
                Serial(0),
                Request {
                    value: 10,
                    calls: Rc::new(Cell::new(0)),
                    busy: false
                }
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(10)))
    ));
    let entry = store.inner.index.prepare(U64Key.hash(&1)).unwrap();
    let crate::index::IndexHead::Log(address) = entry.head else {
        panic!("should have a dwell value")
    };
    let counts = Rc::new(DropCounts::default());
    let ticket = std::thread::scope(|scope| {
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
        let submitted = session
            .upsert(
                Serial(1),
                OwnedPut {
                    value: 20,
                    counts: counts.clone(),
                },
            )
            .map_err(|r| r.reason)
            .unwrap();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let Submission::Pending(ticket) = submitted else {
            panic!("A real license contention must establish a pending ticket")
        };
        ticket
    });
    assert_eq!(ticket.id().session, session.id());
    assert_eq!(ticket.id().store, store.id());
    assert_eq!(counts.calls.get(), 0);
    let mut ticket = if discard {
        drop(ticket);
        None
    } else {
        Some(ticket)
    };
    for _ in 0..8 {
        session.poll(PollBudget::default()).unwrap();
    }
    assert_eq!(counts.calls.get(), 1);
    assert_eq!(counts.requests.get(), 1);
    assert_eq!(counts.outputs.get(), usize::from(discard));
    assert_eq!(session.last_accepted(), Some(Serial(1)));
    let next_counts = Rc::new(DropCounts::default());
    let next = OwnedPut {
        value: 30,
        counts: next_counts.clone(),
    };
    let mut kept = None;
    let next = if let Some(ticket) = ticket.as_mut() {
        let rejected = match session.upsert(Serial(2), next) {
            Err(rejected) => rejected,
            Ok(_) => panic!("Unreceived results must reserve a spot"),
        };
        assert!(matches!(rejected.reason, Error::Busy));
        assert_eq!(session.last_accepted(), Some(Serial(1)));
        assert!(Rc::ptr_eq(&rejected.request.counts, &next_counts));
        assert_eq!(next_counts.calls.get(), 0);
        assert_eq!(next_counts.requests.get(), 0);
        let TicketState::Ready(Ok(Outcome::Success(output))) = ticket.try_take().unwrap() else {
            panic!("should obtain unique owned results")
        };
        assert_eq!(output.value, 20);
        assert!(ticket.try_take().is_err());
        kept = Some(output);
        rejected.request
    } else {
        next
    };
    let Submission::Ready(Ok(Outcome::Success(output))) = session
        .upsert(Serial(2), next)
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("Resident writes should return synchronously after budget is released")
    };
    assert_eq!(output.value, 30);
    assert_eq!(next_counts.calls.get(), 1);
    assert_eq!(next_counts.requests.get(), 1);
    assert_eq!(next_counts.outputs.get(), 0);
    assert_eq!(session.last_accepted(), Some(Serial(2)));
    let last_counts = Rc::new(DropCounts::default());
    let Submission::Ready(Ok(Outcome::Success(last_output))) = session
        .upsert(
            Serial(3),
            OwnedPut {
                value: 40,
                counts: last_counts.clone(),
            },
        )
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("Still holding synchronized outputs should not occupy the result budget")
    };
    assert_eq!(last_output.value, 40);
    assert_eq!(last_counts.calls.get(), 1);
    assert_eq!(last_counts.requests.get(), 1);
    assert!(matches!(
        session
            .read(
                Serial(4),
                Request {
                    value: 0,
                    calls: Rc::new(Cell::new(0)),
                    busy: false
                },
                ReadOptions::default()
            )
            .map_err(|r| r.reason)
            .unwrap(),
        Submission::Ready(Ok(Outcome::Success(40)))
    ));
    assert!(!store.diagnostics().unwrap().failed);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    assert_eq!(counts.outputs.get(), usize::from(discard));
    assert_eq!(next_counts.outputs.get(), 0);
    assert_eq!(last_counts.outputs.get(), 0);
    drop(kept);
    drop(output);
    drop(last_output);
    assert_eq!(counts.outputs.get(), 1);
    assert_eq!(next_counts.outputs.get(), 1);
    assert_eq!(last_counts.outputs.get(), 1);
}
#[test]
fn owning_results_of_pending_writes_are_charged_to_free_up_budget_and_survive_shutdown() {
    owned_result(false)
}
#[test]
fn abandoning_the_pending_write_ticket_does_not_take_effect_and_the_result_is_only_destroyed_once()
{
    owned_result(true)
}
