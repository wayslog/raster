//! Record the complete call interval on the native disk,Enumeration legal history after staggered maintenance,And verify again with real recovery.
#[path = "support/history.rs"]
mod history;
use history::{Action, Event, Reply, Request, Schema};
use raster::{
    RasterKV, Session, Submission,
    api::{completion::Outcome, maintenance::*, operation::*, session::SessionOptions},
    config::Config,
    device::thread_pool::ThreadPoolDeviceFactory,
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(60))
}
#[derive(Debug)]
struct Fill(u64);
impl Keyed<Schema> for Fill {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Fill {
    type Output = Reply;
    fn replacement(&mut self) -> Result<(u64, Reply), Error> {
        Ok((7, Reply::Value(7)))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<Reply>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn take(session: &mut Session<Schema>, submission: Submission<Reply>) -> Reply {
    let result = match submission {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap(),
    };
    match result {
        Ok(Outcome::Success(reply)) => reply,
        Ok(Outcome::NotFound) => Reply::Missing,
        other => panic!("unexpected business results {other:?}"),
    }
}
fn call(
    session: &mut Session<Schema>,
    serial: u64,
    action: Action,
    clock: &AtomicU64,
) -> (u64, Submission<Reply>) {
    let end = deadline();
    loop {
        let request = Request(action);
        // Only the origin of the call that is actually accepted is recorded,Don't put it before Busy The interval is merged into the history.
        let start = clock.fetch_add(1, Ordering::SeqCst);
        let result = match action {
            Action::Read => session.read(Serial(serial), request, ReadOptions::default()),
            Action::Put(..) => session.upsert(Serial(serial), request),
            Action::Add(..) => session.rmw(Serial(serial), request, RmwOptions::default()),
            Action::Delete => session.delete(Serial(serial), request, DeleteOptions::default()),
        };
        match result {
            Ok(result) => return (start, result),
            Err(rejected) => {
                assert!(
                    matches!(rejected.reason, Error::Busy),
                    "Reason for rejection {}",
                    rejected.reason
                );
                assert!(
                    !end.expired(),
                    "Backpressure timeout for unaccepted requests"
                );
                assert!(session.last_accepted().is_none_or(|last| last.0 < serial));
                session.poll(PollBudget::default()).unwrap();
                std::thread::yield_now();
            }
        }
    }
}
fn drive<R: Clone + std::fmt::Debug>(store: &RasterKV<Schema>, ticket: MaintenanceTicket<R>) -> R {
    let end = deadline();
    loop {
        store.maintenance().poll(PollBudget::default()).unwrap();
        if let Some(report) = ticket.try_report().unwrap() {
            return report.as_ref().as_ref().unwrap().clone();
        }
        assert!(!end.expired(), "Maintenance task not completed");
        std::thread::yield_now();
    }
}
fn builder(config: Config) -> raster::Builder<Schema> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 128,
        }))
}
#[derive(Clone, Copy, Debug)]
enum MaintenanceMode {
    Grow,
    Checkpoint,
    ScanDedup,
    Lookup,
}
struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn scenario(mode: MaintenanceMode, cache: bool, round: u64) -> String {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-p9-history-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 16384;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    config.index.buckets = 32;
    config.cache.enabled = cache;
    config.cache.capacity_bytes = 8192;
    let store = builder(config.clone()).create().unwrap();
    let mut warm = store.start_session(SessionOptions::default()).unwrap();
    for key in 1..=800 {
        let result = warm.upsert(Serial(key), Fill(key)).unwrap();
        assert_eq!(take(&mut warm, result), Reply::Value(7));
    }
    warm.close(deadline()).unwrap();
    drop(warm);
    let before = store.diagnostics().unwrap();
    assert!(before.log_span_bytes > config.log.page_bytes as u64 * config.log.memory_pages as u64);
    // The initial key already exists on disk;Two workers hand over first Pending,Accept only after maintenance,Then work together to advance.
    let (submitted_tx, submitted_rx) = mpsc::channel();
    let clock = AtomicU64::new(0);
    let events = Mutex::new(Vec::new());
    let streams = [
        [Action::Add(round + 1, true), Action::Delete, Action::Read],
        [
            Action::Put(round + 9, true),
            Action::Add(3, false),
            Action::Read,
        ],
    ];
    std::thread::scope(|scope| {
        let mut starters = Vec::new();
        for stream in streams {
            let (start_tx, start_rx) = mpsc::sync_channel(1);
            starters.push(start_tx);
            let store = &store;
            let submitted = submitted_tx.clone();
            let clock = &clock;
            let events = &events;
            scope.spawn(move || {
                let mut session = store.start_session(SessionOptions::default()).unwrap();
                let (start, initial) = call(&mut session, 1, Action::Read, clock);
                assert!(
                    matches!(initial, Submission::Pending(_)),
                    "The historical starting point must be a real disk Pending"
                );
                submitted.send(()).unwrap();
                start_rx
                    .recv_timeout(Duration::from_secs(60))
                    .expect("Maintenance not accepted or main thread failed");
                let reply = take(&mut session, initial);
                let end = clock.fetch_add(1, Ordering::SeqCst);
                events.lock().unwrap().push(Event {
                    start,
                    end,
                    action: Action::Read,
                    reply,
                });
                for (i, action) in stream.into_iter().enumerate() {
                    let (start, result) = call(&mut session, 3 + 2 * i as u64, action, clock);
                    let reply = take(&mut session, result);
                    let end = clock.fetch_add(1, Ordering::SeqCst);
                    events.lock().unwrap().push(Event {
                        start,
                        end,
                        action,
                        reply,
                    });
                }
                session.close(deadline()).unwrap();
            });
        }
        for _ in 0..2 {
            submitted_rx
                .recv_timeout(Duration::from_secs(60))
                .expect("Worker not submitted Pending");
        }
        let start = || {
            for starter in &starters {
                starter.send(()).unwrap();
            }
        };
        match mode {
            MaintenanceMode::Grow => {
                let ticket = store.maintenance().grow_index().unwrap();
                start();
                assert_eq!(drive(&store, ticket).new_buckets, 64);
            }
            MaintenanceMode::Checkpoint => {
                let ticket = store
                    .maintenance()
                    .checkpoint(CheckpointKind::Full)
                    .unwrap();
                start();
                drive(&store, ticket);
            }
            MaintenanceMode::ScanDedup | MaintenanceMode::Lookup => {
                let ticket = store
                    .maintenance()
                    .compact(CompactionOptions {
                        algorithm: if matches!(mode, MaintenanceMode::ScanDedup) {
                            CompactionAlgorithm::ScanDedup
                        } else {
                            CompactionAlgorithm::Lookup
                        },
                        until: LogAddress(4096),
                        workers: 2,
                        shift_begin: false,
                        checkpoint: false,
                    })
                    .unwrap();
                start();
                let report = drive(&store, ticket);
                assert_eq!(report.until, LogAddress(4096));
            }
        }
    });
    let mut observer = store.start_session(SessionOptions::default()).unwrap();
    let (start, result) = call(&mut observer, 1, Action::Read, &clock);
    let final_reply = take(&mut observer, result);
    let end = clock.fetch_add(1, Ordering::SeqCst);
    let mut events = events.into_inner().unwrap();
    events.push(Event {
        start,
        end,
        action: Action::Read,
        reply: final_reply,
    });
    assert_eq!(events.len(), 9);
    assert!(
        history::linearizable_from(&events, Some(7)),
        "mode {mode:?} cache {cache} round {round}:{events:?}"
    );
    let token = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let checkpoint = observer.wait_maintenance(&token, deadline()).unwrap();
    let checkpoint = checkpoint.as_ref().as_ref().unwrap().clone();
    let id = observer.id();
    assert!(
        checkpoint
            .sessions
            .iter()
            .any(|progress| progress.session == id && progress.serial == Serial(1))
    );
    config.index.buckets = store.diagnostics().unwrap().bucket_distribution.len();
    let set = RecoverySet {
        store: store.id(),
        index: checkpoint.token,
        log: checkpoint.token,
    };
    observer.close(deadline()).unwrap();
    drop(observer);
    assert!(store.shutdown(deadline()).unwrap().device_drained);
    drop(store);
    let (recovered, _) = builder(config).recover(set).unwrap();
    let mut observer = recovered.continue_session(id).unwrap().session;
    let (_, result) = call(&mut observer, 3, Action::Read, &clock);
    assert_eq!(
        take(&mut observer, result),
        final_reply,
        "The end state of history changes after the restoration of reality"
    );
    observer.close(deadline()).unwrap();
    drop(observer);
    recovered.shutdown(deadline()).unwrap();
    format!("mode {mode:?} cache {cache} round {round}:{events:?}")
}
#[test]
fn complete_history_of_native_disk_double_suspend_and_maintenance_interleaving_is_linearizable_and_recovery_consistent()
 {
    let mut histories = Vec::new();
    for mode in [
        MaintenanceMode::Grow,
        MaintenanceMode::Checkpoint,
        MaintenanceMode::ScanDedup,
        MaintenanceMode::Lookup,
    ] {
        for cache in [false, true] {
            for round in 0..4 {
                let record = scenario(mode, cache, round);
                println!("{record}");
                histories.push(record);
            }
        }
    }
    assert_eq!(histories.len(), 32);
    if let Some(path) = std::env::var_os("RASTER_HISTORY_OUTPUT") {
        std::fs::write(path, histories.join("\n") + "\n").unwrap();
    }
}
