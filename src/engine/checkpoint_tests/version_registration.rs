use super::*;
use crate::{api::Outcome, coordination::Phase};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

enum Command {
    Refresh,
    Submit,
    Stop,
}
enum Reply {
    Refreshed,
    Completed(bool, Option<Serial>),
}
struct PausedPut {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
    calls: Arc<AtomicUsize>,
}
impl Keyed<Schema> for PausedPut {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for PausedPut {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(10)).unwrap();
        Ok((42, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        panic!("the old-version operation targets a missing key")
    }
}

#[test]
fn a_synchronous_old_version_blocks_the_same_key_until_its_checkpoint_cut_is_safe() {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut main = store.start_session(Default::default()).unwrap();
    put(&mut main, 0, 1);
    let main_id = main.id();
    let calls = Arc::new(AtomicUsize::new(0));
    let (report, worker_id) = std::thread::scope(|scope| {
        let (command_tx, command_rx) = mpsc::sync_channel(1);
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let calls = calls.clone();
        let worker = scope.spawn(move || {
            let mut session = owner.start_session(Default::default()).unwrap();
            ready_tx.send(session.id()).unwrap();
            let mut request = Some(PausedPut {
                entered: entered_tx,
                release: release_rx,
                calls,
            });
            while let Ok(command) = command_rx.recv_timeout(Duration::from_secs(10)) {
                match command {
                    Command::Refresh => {
                        session.refresh().unwrap();
                        reply_tx.send(Reply::Refreshed).unwrap();
                    }
                    Command::Submit => {
                        let result = session.upsert(Serial(1), request.take().unwrap());
                        reply_tx
                            .send(Reply::Completed(
                                matches!(result, Ok(Submission::Ready(Ok(Outcome::Success(()))))),
                                session.last_accepted(),
                            ))
                            .unwrap();
                    }
                    Command::Stop => {
                        session.close(deadline()).unwrap();
                        break;
                    }
                }
            }
        });
        let worker_id = ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let ticket = store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap();
        let end = deadline();
        while store.inner.coordinator.snapshot().unwrap().phase != Phase::Prepare {
            assert!(!end.expired(), "checkpoint must reach Prepare");
            command_tx.send(Command::Refresh).unwrap();
            assert!(matches!(
                reply_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
                Reply::Refreshed
            ));
            main.refresh().unwrap();
            store.maintenance().poll(PollBudget::default()).unwrap();
        }
        // The call acknowledges Prepare and is admitted in the old version,
        // then pauses before allocating or holding any record value permission.
        command_tx.send(Command::Submit).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let end = deadline();
        while store.inner.coordinator.snapshot().unwrap().phase != Phase::InProgress {
            assert!(!end.expired(), "checkpoint must publish the new version");
            main.refresh().unwrap();
            store.maintenance().poll(PollBudget::default()).unwrap();
        }
        main.refresh().unwrap();
        assert_eq!(main.runtime.current.version, CheckpointVersion(1));
        // A new-version operation cannot overtake the admitted old callback.
        // Compaction cannot create a newer copy through an active checkpoint.
        let newer = main.upsert(Serial(1), Put(7));
        let copy = store.inner.new_conditional_copy(
            ticket.id(),
            LogAddress(0),
            7u64.to_le_bytes().to_vec(),
        );
        release_tx.send(()).unwrap();
        assert!(matches!(
            newer,
            Err(Rejected {
                reason: Error::Busy,
                ..
            })
        ));
        assert_eq!(main.last_accepted(), Some(Serial(0)));
        assert!(matches!(copy, Err(Error::InvalidState(_))));
        assert!(matches!(
            reply_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            Reply::Completed(true, Some(Serial(1)))
        ));
        // The old value is 42. A newer-version overwrite stores 7, which must
        // remain outside the old checkpoint even though both target one key.
        put(&mut main, 1, 7);
        let end = deadline();
        let report = loop {
            if let Some(report) = ticket.try_report().unwrap() {
                break report.as_ref().as_ref().unwrap().clone();
            }
            assert!(
                !end.expired(),
                "checkpoint must finish after the old callback returns"
            );
            command_tx.send(Command::Refresh).unwrap();
            assert!(matches!(
                reply_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
                Reply::Refreshed
            ));
            main.refresh().unwrap();
            store.maintenance().poll(PollBudget::default()).unwrap();
        };
        command_tx.send(Command::Stop).unwrap();
        worker.join().unwrap();
        (report, worker_id)
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.version, CheckpointVersion(0));
    assert_eq!(
        report
            .sessions
            .iter()
            .find(|p| p.session == worker_id)
            .unwrap()
            .serial,
        Serial(1)
    );
    assert_eq!(
        report
            .sessions
            .iter()
            .find(|p| p.session == main_id)
            .unwrap()
            .serial,
        Serial(0)
    );
    assert_eq!(read_value(&mut main, 2, 7), Some(7));
    main.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    };
    let (recovered, _) = recover_store(config, set).unwrap();
    let mut reader = recovered.start_session(Default::default()).unwrap();
    assert_eq!(read_value(&mut reader, 0, 7), Some(42));
    reader.close(deadline()).unwrap();
    recovered.shutdown(deadline()).unwrap();
}
