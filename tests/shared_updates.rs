use raster::{
    RasterKV, Session, Submission,
    api::{completion::Outcome, operation::*},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    sync::{atomic::Ordering, mpsc},
    time::{Duration, Instant},
};

type Schema = SchemaPair<U64Key, AtomicU64Value>;

struct Increment {
    entered: Option<mpsc::SyncSender<()>>,
    release: Option<mpsc::Receiver<()>>,
}
impl Increment {
    fn plain() -> Self {
        Self {
            entered: None,
            release: None,
        }
    }
    fn update(&mut self, mut value: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        value.view_mut().fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
            self.release
                .take()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        Ok(UpdateDecision::Updated(()))
    }
}
impl Keyed<Schema> for Increment {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for Increment {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((0, ()))
    }
    fn update_in_place(
        &mut self,
        value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        self.update(value)
    }
}
impl RmwOperation<Schema> for Increment {
    type Output = ();
    fn initial(&mut self) -> Result<(u64, ()), Error> {
        Ok((1, ()))
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, ()), Error> {
        Ok((*value.view() + 1, ()))
    }
    fn update_in_place(
        &mut self,
        value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        self.update(value)
    }
}
struct Read;
impl Keyed<Schema> for Read {
    fn key(&self) -> &u64 {
        &7
    }
}
impl ReadOperation<Schema> for Read {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
fn finish<T: 'static>(session: &mut Session<Schema>, submission: Submission<T>) -> Outcome<T> {
    match submission {
        Submission::Ready(result) => result.unwrap(),
        Submission::Pending(mut ticket) => session
            .wait(
                &mut ticket,
                Deadline(Instant::now() + Duration::from_secs(5)),
            )
            .unwrap()
            .unwrap(),
    }
}
fn update(store: &RasterKV<Schema>, mut request: Increment, rmw: bool) {
    let mut session = store.start_session(Default::default()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = if rmw {
            session.rmw(Serial(0), request, Default::default())
        } else {
            session.upsert(Serial(0), request)
        };
        match result {
            Ok(submission) => {
                assert!(matches!(
                    finish(&mut session, submission),
                    Outcome::Success(())
                ));
                return;
            }
            Err(rejected) => {
                assert!(matches!(rejected.reason, Error::Busy));
                assert!(Instant::now() < deadline);
                request = rejected.request;
                std::thread::yield_now();
            }
        }
    }
}
fn store() -> RasterKV<Schema> {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    update(&store, Increment::plain(), false);
    store
}

#[test]
fn atomic_upserts_and_rmw_overlap_on_the_same_record() {
    for rmw in [false, true] {
        let store = store();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let overlapped = std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                update(
                    &store,
                    Increment {
                        entered: Some(entered_tx),
                        release: Some(release_rx),
                    },
                    rmw,
                )
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let second = scope.spawn(|| {
                update(&store, Increment::plain(), rmw);
                done_tx.send(()).unwrap();
            });
            let overlapped = done_rx.recv_timeout(Duration::from_millis(200)).is_ok();
            release_tx.send(()).unwrap();
            first.join().unwrap();
            second.join().unwrap();
            overlapped
        });
        assert!(
            overlapped,
            "compatible atomic update was blocked by another update"
        );
        let mut session = store.start_session(Default::default()).unwrap();
        let result = session
            .read(Serial(0), Read, Default::default())
            .map_err(|r| r.reason)
            .unwrap();
        assert!(matches!(finish(&mut session, result), Outcome::Success(2)));
    }
}

struct Append {
    updates: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    copies: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl Keyed<Schema> for Append {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for Append {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        self.copies.fetch_add(1, Ordering::SeqCst);
        Ok((100, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        self.updates.fetch_add(1, Ordering::SeqCst);
        Ok(UpdateDecision::Append)
    }
}
impl RmwOperation<Schema> for Append {
    type Output = ();
    fn initial(&mut self) -> Result<(u64, ()), Error> {
        panic!("source must exist")
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, ()), Error> {
        self.copies.fetch_add(1, Ordering::SeqCst);
        Ok((*value.view() + 10, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        self.updates.fetch_add(1, Ordering::SeqCst);
        Ok(UpdateDecision::Append)
    }
}

#[test]
fn append_waits_for_shared_updates_and_does_not_replay_the_in_place_callback() {
    use std::sync::{Arc, atomic::AtomicUsize};
    for rmw in [false, true] {
        let store = store();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let updates = Arc::new(AtomicUsize::new(0));
        let copies = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            let holder = scope.spawn(|| {
                update(
                    &store,
                    Increment {
                        entered: Some(entered_tx),
                        release: Some(release_rx),
                    },
                    true,
                )
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut session = store.start_session(Default::default()).unwrap();
            let request = Append {
                updates: updates.clone(),
                copies: copies.clone(),
            };
            let submission = if rmw {
                session.rmw(Serial(0), request, Default::default())
            } else {
                session.upsert(Serial(0), request)
            };
            let pending = matches!(&submission, Ok(Submission::Pending(_)));
            let copies_before_release = copies.load(Ordering::SeqCst);
            let accepted = session.last_accepted();
            release_tx.send(()).unwrap();
            holder.join().unwrap();
            assert!(
                pending,
                "accepted structural fallback must wait without rejecting or copying"
            );
            assert_eq!(copies_before_release, 0);
            assert_eq!(accepted, Some(Serial(0)));
            let submission = submission.map_err(|r| r.reason).unwrap();
            assert!(matches!(
                finish(&mut session, submission),
                Outcome::Success(())
            ));
            assert_eq!(updates.load(Ordering::SeqCst), 1);
            assert_eq!(copies.load(Ordering::SeqCst), 1);
            let result = session
                .read(Serial(1), Read, Default::default())
                .map_err(|r| r.reason)
                .unwrap();
            let value = match finish(&mut session, result) {
                Outcome::Success(value) => value,
                _ => panic!("record missing"),
            };
            assert_eq!(value, if rmw { 11 } else { 100 });
        });
    }
}

struct Delete;
impl Keyed<Schema> for Delete {
    fn key(&self) -> &u64 {
        &7
    }
}
impl DeleteOperation<Schema> for Delete {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {}
}

#[test]
fn append_and_delete_complete_while_shared_writers_keep_running() {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    let store = store();
    let stop = AtomicBool::new(false);
    let completed = AtomicUsize::new(0);
    let started = std::sync::Barrier::new(5);
    let limit = Instant::now() + Duration::from_secs(5);
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..4 {
            workers.push(scope.spawn(|| {
                let mut session = store.start_session(Default::default()).unwrap();
                let mut serial = 0;
                started.wait();
                while !stop.load(Ordering::SeqCst) && Instant::now() < limit {
                    match session.upsert(Serial(serial), Increment::plain()) {
                        Ok(submission) => {
                            assert!(matches!(
                                finish(&mut session, submission),
                                Outcome::Success(())
                            ));
                            serial += 1;
                            completed.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(rejected) => {
                            assert!(matches!(rejected.reason, Error::Busy));
                            std::thread::yield_now();
                        }
                    }
                }
            }));
        }
        started.wait();
        while completed.load(Ordering::SeqCst) < 32 && Instant::now() < limit {
            std::thread::yield_now();
        }
        let before = completed.load(Ordering::SeqCst);
        let mut session = store.start_session(Default::default()).unwrap();
        for serial in 0..20 {
            loop {
                assert!(Instant::now() < limit, "structural writer did not progress");
                let submission = if serial % 2 == 0 {
                    session
                        .upsert(
                            Serial(serial),
                            Append {
                                updates: Default::default(),
                                copies: Default::default(),
                            },
                        )
                        .map_err(|r| r.reason)
                } else {
                    session
                        .delete(Serial(serial), Delete, Default::default())
                        .map_err(|r| r.reason)
                };
                match submission {
                    Ok(submission) => {
                        assert!(matches!(
                            finish(&mut session, submission),
                            Outcome::Success(())
                        ));
                        break;
                    }
                    Err(Error::Busy) => std::thread::yield_now(),
                    Err(error) => panic!("structural operation failed: {error:?}"),
                }
            }
        }
        let after = completed.load(Ordering::SeqCst);
        stop.store(true, Ordering::SeqCst);
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(
            before >= 32 && after > before,
            "shared writers must make progress during structural operations"
        );
    });
}
