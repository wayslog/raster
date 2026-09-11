//! P6.4：在同一实例交错缓存、Pending 写入、扩容、扫描和 Full 检查点，再核对恢复结果。
use super::*;
use crate::{
    api::{
        completion::Outcome,
        operation::RmwOperation,
        scan::{Buffering, RecordScanner, ScanOptions, ScannedRecord},
    },
    schema::{KeyCodec, ValueRead},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Add(Arc<AtomicUsize>);
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &1
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        panic!("键 1 必须存在")
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let next = value.view().wrapping_add(5);
        Ok((next, next))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn collect_one(scanner: &mut RecordScanner<Schema>, rows: &mut Vec<ScannedRecord<Schema>>) {
    rows.push(scanner.next_record().unwrap().expect("原物理范围仍有记录"));
}
fn scenario(cache: bool) -> (Vec<Option<u64>>, Vec<Option<u64>>) {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-online-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 8192;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.index.buckets = 16;
    config.session.max_pending = 4;
    config.cache.enabled = cache;
    config.cache.capacity_bytes = 8192;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config.clone())
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let session_id = session.id();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    let repeated = session
        .read(Serial(401), Read(0), Default::default())
        .unwrap();
    assert_eq!(matches!(&repeated, Submission::Ready(_)), cache);
    let repeated = match repeated {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap(),
    };
    assert!(matches!(repeated.unwrap(), Outcome::Success(0)));
    if cache {
        assert!(matches!(
            store.inner.index.prepare(U64Key.hash(&0)).unwrap().head,
            crate::index::IndexHead::Cache(_)
        ));
    }
    let frontiers = store.inner.log.frontiers().unwrap();
    let mut scanner = store
        .scan(ScanOptions {
            begin: frontiers.begin,
            end: frontiers.tail,
            buffering: Buffering::DoublePage,
        })
        .unwrap();
    let mut rows = Vec::new();
    collect_one(&mut scanner, &mut rows);
    let calls = Arc::new(AtomicUsize::new(0));
    let Submission::Pending(mut changed) = session
        .rmw(Serial(402), Add(calls.clone()), Default::default())
        .unwrap()
    else {
        panic!("冷页读改写必须挂起");
    };
    let worker_store = store.clone();
    let deleter = crate::engine::session_actor::Actor::new(move || {
        let mut session = worker_store.start_session(Default::default()).unwrap();
        let Submission::Pending(ticket) = session
            .delete(Serial(0), Delete(2), Default::default())
            .unwrap()
        else {
            panic!("冷页删除必须挂起");
        };
        (session, ticket)
    });
    let growth = store.maintenance().grow_index().unwrap();
    assert!(matches!(store.maintenance().grow_index(), Err(Error::Busy)));
    assert!(matches!(
        store.maintenance().checkpoint(CheckpointKind::Full),
        Err(Error::Busy)
    ));
    session.refresh().unwrap();
    deleter.call(|(session, _)| session.refresh().unwrap());
    collect_one(&mut scanner, &mut rows);
    let end = deadline();
    let report = loop {
        if let Some(report) = growth.try_report().unwrap() {
            break report;
        }
        assert!(!end.expired());
        store
            .maintenance()
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
    };
    let report = report.as_ref().as_ref().unwrap();
    assert_eq!((report.old_buckets, report.new_buckets), (16, 32));
    config.index.buckets = report.new_buckets;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "维护和扫描不能执行 RMW 用户上下文"
    );
    assert_eq!(
        store.inner.cache.allocated_bytes(),
        0,
        "扩容已经规范化缓存索引头"
    );
    assert!(matches!(
        session.wait(&mut changed, deadline()).unwrap().unwrap(),
        Outcome::Success(6)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    deleter.call(|(session, ticket)| {
        assert!(matches!(
            session.wait(ticket, deadline()).unwrap().unwrap(),
            crate::api::Outcome::Success(())
        ));
        session.close(deadline()).unwrap();
    });
    let Submission::Pending(mut reading) = session
        .read(Serial(403), Read(3), Default::default())
        .unwrap()
    else {
        panic!("未缓存的冷键必须挂起");
    };
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    assert!(matches!(store.maintenance().grow_index(), Err(Error::Busy)));
    collect_one(&mut scanner, &mut rows);
    // 扫描器保持打开但不加入会话切分屏障；检查点等待真正的旧版本会话请求。
    let checkpoint = wait(&mut session, &checkpoint);
    assert_eq!(
        checkpoint
            .sessions
            .iter()
            .find(|cut| cut.session == session_id)
            .unwrap()
            .serial,
        Serial(403)
    );
    assert!(matches!(
        session.wait(&mut reading, deadline()).unwrap().unwrap(),
        Outcome::Success(3)
    ));
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: checkpoint.token,
        log: checkpoint.token,
    };
    // 检查点之后继续写入，迫使扫描原范围的驻留页全部淘汰。扫描固定 end 不扩大。
    for key in 400..800 {
        put(&mut session, key + 4, key);
    }
    assert!(store.inner.log.frontiers().unwrap().head > frontiers.tail);
    while let Some(record) = scanner.next_record().unwrap() {
        rows.push(record);
    }
    assert_eq!(rows.len(), 400);
    for (key, row) in rows.iter().enumerate() {
        assert_eq!(row.key, key as u64);
        assert_eq!(row.value, Some(key as u64));
        assert!(!row.tombstone && !row.invalid);
        assert!(row.address < frontiers.tail);
        if key > 0 {
            assert!(row.address > rows[key - 1].address);
        }
    }
    let live: Vec<_> = (0..800)
        .map(|key| read_value(&mut session, 804 + key, key))
        .collect();
    for (key, actual) in live.iter().enumerate() {
        let expected = match key {
            1 => Some(6),
            2 => None,
            _ => Some(key as u64),
        };
        assert_eq!(*actual, expected);
    }
    scanner.close().unwrap();
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = recover_store(config, set).unwrap();
    assert_eq!(store.inner.index.snapshot().unwrap().buckets, 32);
    assert_eq!(
        store.inner.cache.allocated_bytes(),
        0,
        "恢复后尚未访问前缓存必须为空"
    );
    let resumed = store.continue_session(session_id).unwrap();
    assert_eq!(resumed.progress.serial, Serial(403));
    let mut session = resumed.session;
    let restored: Vec<_> = (0..400)
        .map(|key| read_value(&mut session, 804 + key, key))
        .collect();
    assert_eq!(restored, live[..400]);
    assert_eq!(read_value(&mut session, 1204, 400), None);
    assert_eq!(read_value(&mut session, 1205, 799), None);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    (live, restored)
}
#[test]
fn 缓存开关下挂起写入扩容扫描检查点及恢复交错结果一致() {
    assert_eq!(scenario(false), scenario(true));
}

mod paused_scan {
    use super::{Directory, deadline};
    use crate::{
        RasterKV, Submission,
        api::{
            completion::Outcome,
            maintenance::{CheckpointKind, RecoverySet},
            operation::*,
            scan::{Buffering, ScanOptions},
            session::SessionOptions,
        },
        config::Config,
        device::thread_pool::ThreadPoolDeviceFactory,
        schema::{
            ValueRead, ValueUpdate,
            builtin::{SchemaPair, SerializedValue, U64Key, U64ValueCodec},
            value::ValueCodec,
        },
        types::*,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::time::Duration;
    struct Control {
        armed: AtomicBool,
        entered: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    struct Codec(Arc<Control>);
    impl ValueCodec for Codec {
        type Value = u64;
        fn format_id(&self) -> FormatId {
            U64ValueCodec.format_id()
        }
        fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
            U64ValueCodec.encode(value)
        }
        fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
            if self.0.armed.swap(false, Ordering::SeqCst) {
                self.0.entered.send(()).unwrap();
                self.0
                    .resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15))
                    .unwrap();
            }
            U64ValueCodec.decode(bytes)
        }
    }
    type Schema = SchemaPair<U64Key, SerializedValue<Codec>>;
    #[derive(Debug)]
    struct Put(u64);
    impl Keyed<Schema> for Put {
        fn key(&self) -> &u64 {
            &0
        }
    }
    impl UpsertOperation<Schema> for Put {
        type Output = ();
        fn replacement(&mut self) -> Result<(u64, ()), Error> {
            Ok((self.0, ()))
        }
        fn update_in_place(
            &mut self,
            _: ValueUpdate<'_, Schema>,
        ) -> Result<UpdateDecision<()>, Error> {
            Ok(UpdateDecision::Append)
        }
    }
    #[derive(Debug)]
    struct Read;
    impl Keyed<Schema> for Read {
        fn key(&self) -> &u64 {
            &0
        }
    }
    impl ReadOperation<Schema> for Read {
        type Output = u64;
        fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<u64, Error> {
            Ok(*value.view())
        }
    }
    fn builder(config: Config, control: Arc<Control>) -> crate::Builder<Schema> {
        RasterKV::builder(SchemaPair::new(
            U64Key,
            SerializedValue::new(Codec(control)),
        ))
        .config(config)
        .device(Box::new(ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
    }
    #[test]
    fn 扫描拥有值解码暂停期间检查点仍完成且新写不会混入恢复切分() {
        for cache in [false, true] {
            let root = Directory(std::env::temp_dir().join(format!(
                "raster-online-pause-{:x?}",
                StoreId::generate().unwrap().0
            )));
            let (entered, seen) = mpsc::channel();
            let (release, resume) = mpsc::channel();
            let control = Arc::new(Control {
                armed: false.into(),
                entered,
                resume: Mutex::new(resume),
            });
            let mut config = Config::default();
            config.storage.root = root.0.clone();
            config.log.page_bytes = 4096;
            config.log.memory_pages = 4;
            config.cache.enabled = cache;
            config.cache.capacity_bytes = 8192;
            let store = builder(config.clone(), control.clone()).create().unwrap();
            let mut session = store.start_session(SessionOptions::default()).unwrap();
            let id = session.id();
            assert!(matches!(
                session.upsert(Serial(0), Put(7)).unwrap(),
                Submission::Ready(Ok(_))
            ));
            let end = store.inner.log.frontiers().unwrap().tail;
            let mut scan = store
                .scan(ScanOptions {
                    begin: LogAddress(0),
                    end,
                    buffering: Buffering::DoublePage,
                })
                .unwrap();
            control.armed.store(true, Ordering::SeqCst);
            let worker = std::thread::spawn(move || {
                let row = scan.next_record().unwrap().unwrap();
                scan.close().unwrap();
                row
            });
            seen.recv_timeout(Duration::from_secs(10)).unwrap();
            let checkpoint = store
                .maintenance()
                .checkpoint(CheckpointKind::Full)
                .unwrap();
            let checkpoint = session.wait_maintenance(&checkpoint, deadline()).unwrap();
            let checkpoint = checkpoint.as_ref().as_ref().unwrap();
            assert_eq!(
                checkpoint
                    .sessions
                    .iter()
                    .find(|cut| cut.session == id)
                    .unwrap()
                    .serial,
                Serial(0)
            );
            // 扫描只占有已经复制的字节，不能让日志冻结等待其用户解码。
            assert!(matches!(
                session.upsert(Serial(1), Put(77)).unwrap(),
                Submission::Ready(Ok(_))
            ));
            let live = match session.read(Serial(2), Read, Default::default()).unwrap() {
                Submission::Ready(result) => result,
                Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap(),
            };
            assert!(matches!(live.unwrap(), Outcome::Success(77)));
            release.send(()).unwrap();
            let row = worker.join().unwrap();
            assert_eq!(row.value, Some(7));
            assert_eq!(row.version, CheckpointVersion(0));
            let set = RecoverySet {
                store: store.id(),
                index: checkpoint.token,
                log: checkpoint.token,
            };
            session.close(deadline()).unwrap();
            drop(session);
            store.shutdown(deadline()).unwrap();
            drop(store);
            let (store, _) = builder(config, control).recover(set).unwrap();
            assert_eq!(store.inner.cache.allocated_bytes(), 0);
            let resumed = store.continue_session(id).unwrap();
            assert_eq!(resumed.progress.serial, Serial(0));
            let mut session = resumed.session;
            let restored = match session.read(Serial(1), Read, Default::default()).unwrap() {
                Submission::Ready(result) => result,
                Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap(),
            };
            assert!(matches!(restored.unwrap(), Outcome::Success(7)));
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
        }
    }
}
