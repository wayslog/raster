//! 公开扩容、在途请求和业务回调的实际引擎验证。
use super::*;
use crate::{
    api::{maintenance::IndexGrowthReport, operation::RmwOperation},
    schema::ValueRead,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn finish_growth(
    store: &RasterKV<Schema>,
    ticket: &MaintenanceTicket<IndexGrowthReport>,
) -> Arc<Result<IndexGrowthReport, Error>> {
    let end = deadline();
    loop {
        if let Some(report) = ticket.try_report().unwrap() {
            return report;
        }
        assert!(!end.expired(), "扩容等待超时");
        store.maintenance().poll(PollBudget::default()).unwrap();
    }
}
#[test]
fn 公开扩容等待旧表_epoch_释放且容量报告与再次扩容真实() {
    let (_root, store) = setup(Some(Box::new(device::memory::MemoryDeviceFactory)));
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..50 {
        put(&mut session, key, key);
    }
    let participant = store.inner.epoch.register().unwrap();
    let guard = store.inner.epoch.enter(participant).unwrap();
    let ticket = store.maintenance().grow_index().unwrap();
    assert!(matches!(store.maintenance().grow_index(), Err(Error::Busy)));
    assert!(
        store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .is_err()
    );
    assert!(matches!(store.shutdown(deadline()), Err(Error::Busy)));
    session.refresh().unwrap();
    let end = deadline();
    while store.inner.coordinator.snapshot().unwrap().phase != crate::coordination::Phase::Publish {
        assert!(!end.expired());
        store.maintenance().poll(PollBudget::default()).unwrap();
    }
    assert!(ticket.try_report().unwrap().is_none());
    drop(guard);
    store.inner.epoch.unregister(participant).unwrap();
    let report = finish_growth(&store, &ticket);
    let report = report.as_ref().as_ref().unwrap();
    assert_eq!(
        (report.old_buckets, report.new_buckets, report.generation),
        (1024, 2048, Generation(1))
    );
    assert_eq!(store.inner.index.snapshot().unwrap().buckets, 2048);
    assert_eq!(
        store.inner.coordinator.snapshot().unwrap().version,
        CheckpointVersion(0)
    );
    for key in 0..50 {
        assert_eq!(read_value(&mut session, 50 + key, key), Some(key));
    }
    let next = store.maintenance().grow_index().unwrap();
    let next = session.wait_maintenance(&next, deadline()).unwrap();
    assert_eq!(next.as_ref().as_ref().unwrap().new_buckets, 4096);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[derive(Debug)]
struct Add(Arc<AtomicUsize>);
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &0
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        panic!("原键必须存在")
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let value = *value.view() + 5;
        Ok((value, value))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn 磁盘读改写读取与删除挂起跨越扩容且回调只执行一次() {
    let (_root, store) = setup(None);
    let mut config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let Submission::Pending(mut rmw) = session
        .rmw(Serial(400), Add(calls.clone()), Default::default())
        .unwrap()
    else {
        panic!("旧页必须挂起")
    };
    let worker_store = store.clone();
    let deleter = crate::engine::session_actor::Actor::new(move || {
        let mut session = worker_store.start_session(Default::default()).unwrap();
        let Submission::Pending(ticket) = session
            .delete(Serial(10), Delete(1), Default::default())
            .unwrap()
        else {
            panic!("冷页删除必须挂起");
        };
        (session, ticket)
    });
    let growth = store.maintenance().grow_index().unwrap();
    assert!(matches!(
        store.maintenance().checkpoint(CheckpointKind::Full),
        Err(Error::Busy)
    ));
    session.refresh().unwrap();
    deleter.call(|(session, _)| session.refresh().unwrap());
    finish_growth(&store, &growth).as_ref().as_ref().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0, "维护轮询不能执行用户回调");
    assert!(matches!(
        session.wait(&mut rmw, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(5)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    deleter.call(|(session, ticket)| {
        assert!(matches!(
            session.wait(ticket, deadline()).unwrap().unwrap(),
            crate::api::Outcome::Success(())
        ));
        session.close(deadline()).unwrap();
    });
    let Submission::Pending(mut read) = session
        .read(Serial(401), Read(2), Default::default())
        .unwrap()
    else {
        panic!("旧页必须挂起")
    };
    let growth = store.maintenance().grow_index().unwrap();
    session.refresh().unwrap();
    let growth = finish_growth(&store, &growth);
    config.index.buckets = growth.as_ref().as_ref().unwrap().new_buckets;
    assert!(matches!(
        session.wait(&mut read, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(2)
    ));
    let checkpoint = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: checkpoint.token,
        log: checkpoint.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = recover_store(config, set).unwrap();
    let mut session = store.continue_session(id).unwrap().session;
    assert_eq!(read_value(&mut session, 402, 0), Some(5));
    assert_eq!(read_value(&mut session, 403, 1), None);
    assert_eq!(read_value(&mut session, 404, 2), Some(2));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[derive(Debug)]
struct PausedPut {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
impl Keyed<Schema> for PausedPut {
    fn key(&self) -> &u64 {
        &7
    }
}
impl UpsertOperation<Schema> for PausedPut {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(10)).unwrap();
        Ok((77, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn 扩容不会使暂停回调中已准备的写入因迁移而失败() {
    let (_root, store) = setup(Some(Box::new(device::memory::MemoryDeviceFactory)));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let worker_store = store.clone();
        let worker = scope.spawn(move || {
            let mut session = worker_store
                .start_session(SessionOptions::default())
                .unwrap();
            ready_tx.send(()).unwrap();
            start_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            session.refresh().unwrap();
            let result = session
                .upsert(
                    Serial(0),
                    PausedPut {
                        entered: entered_tx,
                        release: release_rx,
                    },
                )
                .unwrap();
            match result {
                Submission::Ready(result) => {
                    result.unwrap();
                }
                Submission::Pending(mut ticket) => {
                    session.wait(&mut ticket, deadline()).unwrap().unwrap();
                }
            }
            session.close(deadline()).unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let growth = store.maintenance().grow_index().unwrap();
        start_tx.send(()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        for _ in 0..20 {
            store.maintenance().poll(PollBudget::default()).unwrap();
        }
        assert!(growth.try_report().unwrap().is_none());
        assert!(!store.inner.failed.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        finish_growth(&store, &growth).as_ref().as_ref().unwrap();
    });
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    assert_eq!(read_value(&mut session, 0, 7), Some(77));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 放弃扩容参与会话使票据稳定失败且禁止新的动作() {
    let (_root, store) = setup(Some(Box::new(device::memory::MemoryDeviceFactory)));
    let session = store.start_session(SessionOptions::default()).unwrap();
    let ticket = store.maintenance().grow_index().unwrap();
    drop(session);
    assert!(store.maintenance().poll(PollBudget::default()).is_err());
    let first = ticket.try_report().unwrap().unwrap();
    assert!(first.is_err());
    assert!(Arc::ptr_eq(&first, &ticket.try_report().unwrap().unwrap()));
    assert!(store.maintenance().grow_index().is_err());
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 多会话磁盘写入与协作扩容并发且完成只计数一次() {
    let (_root, store) = setup(None);
    let barrier = std::sync::Barrier::new(5);
    let completed = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for worker in 0..4 {
            let store = &store;
            let barrier = &barrier;
            workers.push(scope.spawn(move || {
                let mut session = store.start_session(SessionOptions::default()).unwrap();
                barrier.wait();
                barrier.wait();
                for step in 0..100 {
                    let mut request = Put(worker * 100 + step);
                    let end = deadline();
                    loop {
                        assert!(!end.expired());
                        match session.upsert(Serial(step), request) {
                            Ok(Submission::Ready(result)) => {
                                result.unwrap();
                                break;
                            }
                            Ok(Submission::Pending(mut ticket)) => {
                                session.wait(&mut ticket, deadline()).unwrap().unwrap();
                                break;
                            }
                            Err(rejected) => {
                                assert!(matches!(rejected.reason, Error::Busy));
                                request = rejected.request;
                                session.poll(PollBudget::default()).unwrap();
                                std::thread::yield_now();
                            }
                        }
                    }
                }
                session.close(deadline()).unwrap();
            }));
        }
        barrier.wait();
        let ticket = store.maintenance().grow_index().unwrap();
        barrier.wait();
        let drive = || {
            let end = deadline();
            while ticket.try_report().unwrap().is_none() {
                assert!(!end.expired());
                let progress = store.maintenance().poll(PollBudget::default()).unwrap();
                completed.fetch_add(progress.completed, Ordering::SeqCst);
                std::thread::yield_now();
            }
        };
        std::thread::scope(|drivers| {
            let helper = drivers.spawn(drive);
            drive();
            helper.join().unwrap();
        });
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(
            ticket
                .try_report()
                .unwrap()
                .unwrap()
                .as_ref()
                .as_ref()
                .unwrap()
                .new_buckets,
            2048
        );
    });
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        assert_eq!(read_value(&mut session, key, key), Some(key));
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 空索引扩容丢弃票据仍能完成且动作期间拒绝关闭() {
    let (_root, store) = setup(Some(Box::new(device::memory::MemoryDeviceFactory)));
    let ticket = store.maintenance().grow_index().unwrap();
    assert!(matches!(store.shutdown(deadline()), Err(Error::Busy)));
    assert!(matches!(
        store.start_session(SessionOptions::default()),
        Err(Error::Busy)
    ));
    drop(ticket);
    let end = deadline();
    let mut completed = 0;
    while completed == 0 {
        assert!(!end.expired());
        completed += store
            .maintenance()
            .poll(PollBudget::default())
            .unwrap()
            .completed;
    }
    assert_eq!(completed, 1);
    assert_eq!(store.inner.index.snapshot().unwrap().buckets, 2048);
    store.shutdown(deadline()).unwrap();
}
