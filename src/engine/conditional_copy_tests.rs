//! 条件复制使用真实索引、日志与设备；测试暂停点只控制交错，不替代发布协议。
use super::*;
use crate::{
    api::operation::RmwOperation,
    coordination::{Action, Phase},
    engine::{
        Engine,
        conditional_copy::{ConditionalCopy, CopyResult},
    },
    schema::{KeyCodec, ValueRead},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn budget() -> PollBudget {
    PollBudget(std::num::NonZeroUsize::new(1).unwrap())
}
fn start<S: crate::schema::Schema>(engine: &Engine<S>) -> MaintenanceId {
    engine.coordinator.start_action(Action::Compact).unwrap()
}
fn finish<S: crate::schema::Schema>(engine: &Engine<S>, id: MaintenanceId) {
    engine.coordinator.advance(id, Phase::Compacting).unwrap();
    engine.coordinator.finish_action(id).unwrap();
}
fn source<S: crate::schema::Schema<Key = U64Key>>(engine: &Engine<S>, key: u64) -> LogAddress {
    engine
        .resolve_index(U64Key.hash(&key), &key.to_le_bytes())
        .unwrap()
        .head
        .unwrap()
}
fn drive<S: crate::schema::Schema>(engine: &Engine<S>, task: &mut ConditionalCopy) -> CopyResult {
    let end = deadline();
    loop {
        assert!(!end.expired(), "条件复制没有在期限内结束");
        match engine.conditional_copy(task, budget()).unwrap() {
            CopyResult::Retry => {
                engine.poll_maintenance(budget()).unwrap();
                std::thread::yield_now();
            }
            result => return result,
        }
    }
}
#[derive(Debug)]
struct Add(u64, u64);
impl Keyed<Schema> for Add {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl RmwOperation<Schema> for Add {
    type Output = u64;
    fn initial(&mut self) -> Result<(u64, u64), Error> {
        panic!("源必须存在")
    }
    fn copy_update(&mut self, old: ValueRead<'_, Schema>) -> Result<(u64, u64), Error> {
        let new = old.view().wrapping_add(self.1);
        Ok((new, new))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Updated(
            value
                .view_mut()
                .fetch_add(self.1, Ordering::SeqCst)
                .wrapping_add(self.1),
        ))
    }
}
#[test]
fn 候选捕获后原地更新保持源地址但复制当前值且只终结一次() {
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let old = source(&store.inner, 7);
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 7u64.to_le_bytes().to_vec())
        .unwrap();
    session
        .rmw(Serial(1), Add(7, 5), Default::default())
        .unwrap();
    assert_eq!(source(&store.inner, 7), old);
    let before = store.inner.log.frontiers().unwrap();
    let CopyResult::Copied(new) = drive(&store.inner, &mut task) else {
        panic!("应复制活源")
    };
    assert!(new > old);
    assert_eq!(task.published_address(), Some(new));
    assert_eq!(store.inner.log.frontiers().unwrap().begin, before.begin);
    assert!(matches!(
        store.inner.conditional_copy(&mut task, budget()),
        Err(Error::InvalidState(_))
    ));
    assert!(task.drain(&store.inner.storage).unwrap());
    let stats = store.statistics().conditional_copies;
    assert_eq!((stats.accepted, stats.completed, stats.success), (1, 1, 1));
    assert_eq!(stats.io_per_request[0], 1);
    assert_eq!(store.diagnostics().unwrap().active_requests, 0);
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 2, 7), Some(12));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 追加使旧源失效且原地删除后只复制当前墓碑() {
    let (_root, store) = setup(None);
    store.enable_stats_collection();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 9);
    let old = source(&store.inner, 9);
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 9u64.to_le_bytes().to_vec())
        .unwrap();
    put(&mut session, 1, 9);
    let tail = store.inner.log.frontiers().unwrap().tail;
    assert_eq!(drive(&store.inner, &mut task), CopyResult::Obsolete);
    assert_eq!(store.statistics().conditional_copies.not_found, 1);
    assert_eq!(store.inner.log.frontiers().unwrap().tail, tail);
    let current = source(&store.inner, 9);
    let mut task = store
        .inner
        .new_conditional_copy(id, current, 9u64.to_le_bytes().to_vec())
        .unwrap();
    session
        .delete(Serial(2), Delete(9), Default::default())
        .unwrap();
    let CopyResult::Copied(deleted) = drive(&store.inner, &mut task) else {
        panic!("原地删除保留源地址，复制必须取得当前墓碑");
    };
    assert!(store.inner.log.lease(deleted).unwrap().is_tombstone());
    let tombstone = source(&store.inner, 9);
    let mut task = store
        .inner
        .new_conditional_copy(id, tombstone, 9u64.to_le_bytes().to_vec())
        .unwrap();
    let CopyResult::Copied(new) = drive(&store.inner, &mut task) else {
        panic!("应复制墓碑")
    };
    assert!(store.inner.log.lease(new).unwrap().is_tombstone());
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 3, 9), None);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 同桶同标签不同完整键可复制链内源且保留另一键() {
    let mut config = Config::default();
    config.index.buckets = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 8969);
    let old = source(&store.inner, 8969);
    put(&mut session, 1, 9239);
    assert_ne!(U64Key.hash(&8969).0 % 64, U64Key.hash(&9239).0 % 64);
    assert_eq!(source(&store.inner, 8969), source(&store.inner, 9239));
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 8969u64.to_le_bytes().to_vec())
        .unwrap();
    assert!(matches!(
        drive(&store.inner, &mut task),
        CopyResult::Copied(_)
    ));
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 2, 8969), Some(8969));
    assert_eq!(read_value(&mut session, 3, 9239), Some(9239));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 冷源读取挂起期间追加可完成而过时源不能重新发布() {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 0);
    let old = source(&store.inner, 0);
    for key in 1..400 {
        put(&mut session, key, key);
    }
    assert!(old < store.inner.log.frontiers().unwrap().head);
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 0u64.to_le_bytes().to_vec())
        .unwrap();
    while !task.has_inflight() {
        assert_eq!(
            store.inner.conditional_copy(&mut task, budget()).unwrap(),
            CopyResult::Retry
        );
    }
    // 查索引式 upsert 不需要读取旧值；挂起的复制没有持有业务或源记录许可。
    put(&mut session, 400, 0);
    let latest = source(&store.inner, 0);
    assert_eq!(drive(&store.inner, &mut task), CopyResult::Obsolete);
    assert_eq!(source(&store.inner, 0), latest);
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 401, 0), Some(0));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 外来实例拒绝复制而所属实例仍可完成() {
    let (_one, first) = setup(None);
    let (_two, second) = setup(None);
    let mut session = first.start_session(Default::default()).unwrap();
    put(&mut session, 0, 1);
    let id = start(&first.inner);
    let mut task = first
        .inner
        .new_conditional_copy(id, source(&first.inner, 1), 1u64.to_le_bytes().to_vec())
        .unwrap();
    assert!(matches!(
        second.inner.conditional_copy(&mut task, budget()),
        Err(Error::InvalidState(_))
    ));
    assert!(!second.inner.failed.load(Ordering::SeqCst));
    assert!(matches!(
        drive(&first.inner, &mut task),
        CopyResult::Copied(_)
    ));
    finish(&first.inner, id);
    session.close(deadline()).unwrap();
    first.shutdown(deadline()).unwrap();
    second.shutdown(deadline()).unwrap();
}

mod ordinary {
    use super::*;
    use crate::schema::{
        builtin::{SerializedValue, U64ValueCodec},
        value::ValueCodec,
    };
    use std::sync::{Mutex, mpsc};
    type Ordinary = SchemaPair<U64Key, SerializedValue<Codec>>;
    struct Fault {
        encode_countdown: AtomicUsize,
        encodes: AtomicUsize,
        mode: AtomicUsize,
        reached: mpsc::Sender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    struct Codec(Arc<Fault>);
    impl ValueCodec for Codec {
        type Value = u64;
        fn format_id(&self) -> FormatId {
            U64ValueCodec.format_id()
        }
        fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
            self.0.encodes.fetch_add(1, Ordering::SeqCst);
            if self
                .0
                .encode_countdown
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                == Ok(1)
            {
                match self.0.mode.load(Ordering::SeqCst) {
                    1 => return Err(Error::Codec("注入目标初始化编码失败")),
                    2 => panic!("注入目标初始化恐慌"),
                    3 => return Err(Error::Busy),
                    _ => {
                        self.0.reached.send(()).unwrap();
                        self.0
                            .resume
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(10))
                            .unwrap();
                    }
                }
            }
            U64ValueCodec.encode(value)
        }
        fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
            U64ValueCodec.decode(bytes)
        }
    }
    #[derive(Debug)]
    struct Write(u64, u64);
    impl Keyed<Ordinary> for Write {
        fn key(&self) -> &u64 {
            &self.0
        }
    }
    impl UpsertOperation<Ordinary> for Write {
        type Output = ();
        fn replacement(&mut self) -> Result<(u64, ()), Error> {
            Ok((self.1, ()))
        }
        fn update_in_place(
            &mut self,
            _: ValueUpdate<'_, Ordinary>,
        ) -> Result<UpdateDecision<()>, Error> {
            Ok(UpdateDecision::Append)
        }
    }
    fn setup() -> (
        RasterKV<Ordinary>,
        Arc<Fault>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let (reached, receiver) = mpsc::channel();
        let (sender, resume) = mpsc::channel();
        let fault = Arc::new(Fault {
            encode_countdown: 0.into(),
            encodes: 0.into(),
            mode: 0.into(),
            reached,
            resume: Mutex::new(resume),
        });
        let mut config = Config::default();
        config.index.buckets = 1;
        let store = RasterKV::builder(SchemaPair::new(
            U64Key,
            SerializedValue::new(Codec(fault.clone())),
        ))
        .config(config)
        .device(Box::new(device::null::NullDeviceFactory))
        .create()
        .unwrap();
        (store, fault, receiver, sender)
    }
    fn write(session: &mut Session<Ordinary>, serial: u64, key: u64, value: u64) {
        let Submission::Ready(result) = session.upsert(Serial(serial), Write(key, value)).unwrap()
        else {
            panic!("热记录应同步完成")
        };
        result.unwrap();
    }
    fn value(store: &RasterKV<Ordinary>, key: u64) -> u64 {
        let mut at = Some(source(&store.inner, key));
        while let Some(address) = at {
            let lease = store.inner.log.lease(address).unwrap();
            if lease.key() == key.to_le_bytes() {
                return lease.read(|value| value).unwrap();
            }
            at = lease.previous();
        }
        panic!("键必须存在")
    }
    #[test]
    fn 源许可覆盖目标初始化且碰撞发布冲突清理目标后重试保留两键() {
        let (store, fault, reached, resume) = setup();
        let mut session = store.start_session(Default::default()).unwrap();
        write(&mut session, 0, 8969, 11);
        let old = source(&store.inner, 8969);
        write(&mut session, 1, 9239, 22);
        let id = start(&store.inner);
        let mut task = store
            .inner
            .new_conditional_copy(id, old, 8969u64.to_le_bytes().to_vec())
            .unwrap();
        // 第二次编码是目标初始化，分配已经占槽但尚未进入地址表。
        fault.encode_countdown.store(2, Ordering::SeqCst);
        let end_before = store.inner.log.frontiers().unwrap().tail;
        std::thread::scope(|scope| {
            let copying = scope.spawn(|| {
                loop {
                    let result = store.inner.conditional_copy(&mut task, budget()).unwrap();
                    if fault.encode_countdown.load(Ordering::SeqCst) == 0 {
                        return result;
                    }
                    assert_eq!(result, CopyResult::Retry);
                }
            });
            reached.recv_timeout(Duration::from_secs(10)).unwrap();
            let source_lease = store.inner.log.lease(old).unwrap();
            assert!(matches!(
                source_lease.update_if_mutable(|mut value| value.replace(&99)),
                Err(Error::Busy)
            ));
            assert!(matches!(
                store.inner.log.snapshot_next(old, end_before),
                Err(Error::Busy)
            ));
            drop(source_lease);
            write(&mut session, 2, 9239, 33);
            let collided_head = source(&store.inner, 9239);
            resume.send(()).unwrap();
            assert_eq!(copying.join().unwrap(), CopyResult::Retry);
            assert_eq!(source(&store.inner, 9239), collided_head);
        });
        // 失败目标的地址槽可能有对齐填充，但不出现在物理记录扫描中。
        let mut count = 0;
        let end = store.inner.log.frontiers().unwrap().tail;
        let mut at = LogAddress(0);
        while let Some((address, bytes)) = store.inner.log.snapshot_next(at, end).unwrap() {
            count += 1;
            at = address
                .checked_add(
                    crate::format::Record::decode(&bytes)
                        .unwrap()
                        .header
                        .encoded_len()
                        .unwrap() as u64,
                )
                .unwrap();
        }
        assert_eq!(count, 3);
        assert!(matches!(
            drive(&store.inner, &mut task),
            CopyResult::Copied(_)
        ));
        assert_eq!((value(&store, 8969), value(&store, 9239)), (11, 33));
        finish(&store.inner, id);
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
    #[test]
    fn 目标初始化失败保持源和索引且不能再次执行编码() {
        for mode in [1, 3] {
            let (store, fault, _, _) = setup();
            let mut session = store.start_session(Default::default()).unwrap();
            write(&mut session, 0, 7, 42);
            let old = source(&store.inner, 7);
            let id = start(&store.inner);
            let mut task = store
                .inner
                .new_conditional_copy(id, old, 7u64.to_le_bytes().to_vec())
                .unwrap();
            fault.mode.store(mode, Ordering::SeqCst);
            fault.encode_countdown.store(2, Ordering::SeqCst);
            assert!(matches!(
                store.inner.conditional_copy(&mut task, budget()),
                Err(Error::Codec(_) | Error::Busy)
            ));
            let calls = fault.encodes.load(Ordering::SeqCst);
            assert!(matches!(
                store.inner.conditional_copy(&mut task, budget()),
                Err(Error::InvalidState(_))
            ));
            assert_eq!(fault.encodes.load(Ordering::SeqCst), calls);
            assert_eq!(source(&store.inner, 7), old);
            assert_eq!(value(&store, 7), 42);
            assert!(task.drain(&store.inner.storage).unwrap());
            assert!(!store.inner.failed.load(Ordering::SeqCst));
            finish(&store.inner, id);
            session.close(deadline()).unwrap();
            store.shutdown(deadline()).unwrap();
        }
    }

    #[test]
    fn 目标初始化恐慌失败关闭且没有发布目标() {
        let (store, fault, _, _) = setup();
        let mut session = store.start_session(Default::default()).unwrap();
        write(&mut session, 0, 7, 42);
        let old = source(&store.inner, 7);
        let id = start(&store.inner);
        let mut task = store
            .inner
            .new_conditional_copy(id, old, 7u64.to_le_bytes().to_vec())
            .unwrap();
        fault.mode.store(2, Ordering::SeqCst);
        fault.encode_countdown.store(2, Ordering::SeqCst);
        assert!(matches!(
            store.inner.conditional_copy(&mut task, budget()),
            Err(Error::InvalidState(_))
        ));
        assert!(store.inner.failed.load(Ordering::SeqCst));
        assert_eq!(task.published_address(), None);
        assert_eq!(source(&store.inner, 7), old);
        assert!(task.drain(&store.inner.storage).unwrap());
        finish(&store.inner, id);
        let _ = session.close(deadline());
        let _ = store.shutdown(deadline());
    }
}

#[test]
fn 冷源完整复制和在途排空不保留路由或阻止后续动作() {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 0);
    let old = source(&store.inner, 0);
    for key in 1..400 {
        put(&mut session, key, key);
    }
    let id = start(&store.inner);
    let mut task = store
        .inner
        .new_conditional_copy(id, old, 0u64.to_le_bytes().to_vec())
        .unwrap();
    let CopyResult::Copied(new) = drive(&store.inner, &mut task) else {
        panic!("冷源应完整迁移")
    };
    assert!(new > old);
    assert_eq!(source(&store.inner, 0), new);
    let begin = store.inner.log.frontiers().unwrap().begin;
    assert_eq!(begin, LogAddress(0));
    let old_one = store
        .inner
        .resolve_index(U64Key.hash(&1), &1u64.to_le_bytes())
        .unwrap()
        .head
        .unwrap();
    let mut draining = store
        .inner
        .new_conditional_copy(id, old_one, 1u64.to_le_bytes().to_vec())
        .unwrap();
    while !draining.has_inflight() {
        assert_eq!(
            store
                .inner
                .conditional_copy(&mut draining, budget())
                .unwrap(),
            CopyResult::Retry
        );
    }
    assert!(!draining.drain(&store.inner.storage).unwrap());
    let end = deadline();
    while !draining.drain(&store.inner.storage).unwrap() {
        assert!(!end.expired());
        store
            .inner
            .io
            .poll(&*store.inner.storage.device, budget())
            .unwrap();
    }
    assert_eq!(draining.published_address(), None);
    assert!(matches!(
        store.inner.conditional_copy(&mut draining, budget()),
        Err(Error::InvalidState(_))
    ));
    finish(&store.inner, id);
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    let checkpoint = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    wait(&mut session, &checkpoint);
    // 更旧版本的许可阻止复制；释放之后相同请求可以继续，压缩自身不递增版本。
    let id = start(&store.inner);
    let permit = store
        .inner
        .version_permits
        .reserve(U64Key.hash(&0), CheckpointVersion(0))
        .unwrap();
    let mut task = store
        .inner
        .new_conditional_copy(id, new, 0u64.to_le_bytes().to_vec())
        .unwrap();
    assert_eq!(
        store.inner.conditional_copy(&mut task, budget()).unwrap(),
        CopyResult::Retry
    );
    drop(permit);
    assert!(matches!(
        drive(&store.inner, &mut task),
        CopyResult::Copied(_)
    ));
    finish(&store.inner, id);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
