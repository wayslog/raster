//! 公开扫描的有界预读、失败、关闭与 Drop 生命周期；可控设备仍执行真实文件字节操作。
use super::*;
use crate::{
    RasterKV, Submission,
    api::{operation::*, session::SessionOptions},
    config::Config,
    device::{
        memory::{MemoryDevice, MemoryFault},
        *,
    },
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
};
use std::sync::atomic::AtomicUsize;
type TestSchema = SchemaPair<U64Key, AtomicU64Value>;
struct Control {
    device: MemoryDevice,
    paused: AtomicBool,
    reads: AtomicUsize,
    short: AtomicBool,
    fail: AtomicBool,
}
struct Factory(Arc<Control>);
struct Controlled(Arc<Control>);
impl DeviceFactory for Factory {
    fn open(&self, _: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(Controlled(self.0.clone())))
    }
}
impl Device for Controlled {
    fn capabilities(&self) -> DeviceCapabilities {
        self.0.device.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let read = matches!(request.operation, IoOperation::Read { .. });
        if read && self.0.short.swap(false, Ordering::SeqCst) {
            self.0.device.inject_next(MemoryFault::Short(1)).unwrap();
        }
        if read && self.0.fail.swap(false, Ordering::SeqCst) {
            self.0
                .device
                .inject_next(MemoryFault::Fail(std::io::ErrorKind::Other))
                .unwrap();
        }
        let result = self.0.device.submit(request);
        if read && result.is_ok() {
            self.0.reads.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        if self.0.paused.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.0.device.poll(budget, output)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.0.device.shutdown(deadline)
    }
}
#[derive(Debug)]
struct Put(u64);
impl Keyed<TestSchema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<TestSchema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, TestSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + std::time::Duration::from_secs(5))
}
fn setup() -> (RasterKV<TestSchema>, Arc<Control>) {
    let control = Arc::new(Control {
        device: MemoryDevice::new(512, 8 << 20).unwrap(),
        paused: false.into(),
        reads: 0.into(),
        short: false.into(),
        fail: false.into(),
    });
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    // 页帧跨段，恢复短读必须继续处理帧尾和下一段。
    config.storage.segment_bytes = 4096;
    config.scan.max_scanners = 1;
    config.scan.timeout = std::time::Duration::from_secs(1);
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(Factory(control.clone())))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        let submission = session.upsert(Serial(key), Put(key)).unwrap();
        match submission {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
    }
    session.close(deadline()).unwrap();
    assert!(store.inner.log.frontiers().unwrap().head.0 >= 3 * 4096);
    (store, control)
}
fn options(store: &RasterKV<TestSchema>, mode: Buffering) -> ScanOptions {
    ScanOptions {
        begin: LogAddress(0),
        end: store.inner.log.frontiers().unwrap().tail,
        buffering: mode,
    }
}
#[test]
fn 三种预读分别接受一二三页读取且超时恢复不遗漏() {
    let (store, control) = setup();
    for (mode, frames) in [
        (Buffering::Unbuffered, 1),
        (Buffering::SinglePage, 2),
        (Buffering::DoublePage, 3),
    ] {
        control.reads.store(0, Ordering::SeqCst);
        let mut scan = store.scan(options(&store, mode)).unwrap();
        control.paused.store(true, Ordering::SeqCst);
        assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
        assert_eq!(control.reads.load(Ordering::SeqCst), frames);
        control.paused.store(false, Ordering::SeqCst);
        let mut keys = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            assert_eq!(record.value, Some(record.key));
            keys.push(record.key);
        }
        assert_eq!(keys, (0..400).collect::<Vec<_>>());
        scan.close().unwrap();
    }
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 放弃在途扫描仍占名额且归还完成后可重新注册() {
    let (store, control) = setup();
    let range = options(&store, Buffering::DoublePage);
    let mut scan = store.scan(range).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    drop(scan);
    assert!(matches!(store.scan(range), Err(Error::Busy)));
    assert!(matches!(
        store.shutdown(Deadline(
            Instant::now() + std::time::Duration::from_millis(5)
        )),
        Err(Error::DeadlineExceeded)
    ));
    let reads = control.reads.load(Ordering::SeqCst);
    control.paused.store(false, Ordering::SeqCst);
    let mut next = store.scan(range).unwrap();
    // 放弃扫描只回收已接受的完成，不提交帧跨段的剩余读取。
    assert_eq!(control.reads.load(Ordering::SeqCst), reads);
    next.close().unwrap();
    store.shutdown(deadline()).unwrap();
    next.close().unwrap();
}
#[test]
fn 显式关闭超时可继续关闭且不会重新开始读取() {
    let (store, control) = setup();
    let mut scan = store.scan(options(&store, Buffering::SinglePage)).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    assert!(matches!(scan.close(), Err(Error::DeadlineExceeded)));
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    let accepted = control.reads.load(Ordering::SeqCst);
    control.paused.store(false, Ordering::SeqCst);
    scan.close().unwrap();
    scan.close().unwrap();
    assert_eq!(control.reads.load(Ordering::SeqCst), accepted);
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 短读跨段继续而读取失败使扫描一次失败关闭() {
    let (store, control) = setup();
    let range = options(&store, Buffering::Unbuffered);
    let mut scan = store.scan(range).unwrap();
    control.short.store(true, Ordering::SeqCst);
    assert_eq!(scan.next_record().unwrap().unwrap().key, 0);
    assert!(control.reads.load(Ordering::SeqCst) >= 3);
    scan.close().unwrap();
    let mut scan = store.scan(range).unwrap();
    control.fail.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::Io(_))));
    let reads = control.reads.load(Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert_eq!(control.reads.load(Ordering::SeqCst), reads);
    scan.close().unwrap();
    // 一个扫描的文件读取错误不伪装成整个引擎不可用。
    let mut other = store.scan(range).unwrap();
    assert_eq!(other.next_record().unwrap().unwrap().key, 0);
    other.close().unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 开放扫描前拒绝冷热半记录边界以及倒置和越界() {
    let (store, _) = setup();
    let range = options(&store, Buffering::Unbuffered);
    let hot = store.inner.log.frontiers().unwrap().head;
    let (_, bytes) = store
        .inner
        .log
        .snapshot_next(hot, range.end)
        .unwrap()
        .unwrap();
    assert!(!bytes.is_empty());
    // 冷页从第一个记录的第二字节开始或结束，不能先交付其他记录再报边界错误。
    for (begin, end) in [
        (LogAddress(1), range.end),
        (LogAddress(0), LogAddress(1)),
        (range.end, LogAddress(0)),
        (LogAddress(0), range.end.checked_add(1).unwrap()),
    ] {
        assert!(
            store
                .scan(ScanOptions {
                    begin,
                    end,
                    ..range
                })
                .is_err()
        );
    }
    let (address, _) = store
        .inner
        .log
        .snapshot_next(hot, range.end)
        .unwrap()
        .unwrap();
    assert!(
        store
            .scan(ScanOptions {
                begin: address.checked_add(1).unwrap(),
                ..range
            })
            .is_err()
    );
    let mut empty = store
        .scan(ScanOptions {
            begin: LogAddress(1),
            end: LogAddress(1),
            ..range
        })
        .unwrap();
    assert!(empty.next_record().unwrap().is_none());
    assert!(empty.next_record().unwrap().is_none());
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 放弃通知不因收尾线程持锁而丢失() {
    let (store, _) = setup();
    let scan = store.scan(options(&store, Buffering::Unbuffered)).unwrap();
    let shared = scan.state.clone();
    let guard = shared.state.lock().unwrap();
    drop(scan);
    assert!(shared.abandoned.load(Ordering::SeqCst));
    drop(guard);
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 扫描配置拒绝零预算超时和路由容量溢出() {
    let mut config = Config::default();
    config.scan.max_scanners = 0;
    assert!(config.validate().is_err());
    config.scan.max_scanners = usize::MAX;
    assert!(matches!(config.validate(), Err(Error::CapacityExceeded)));
    config.scan.max_scanners = 1;
    config.scan.timeout = std::time::Duration::ZERO;
    assert!(config.validate().is_err());
}

#[test]
fn 扫描返回后允许全部原驻留页淘汰并从磁盘继续固定范围() {
    let (store, _) = setup();
    let range = options(&store, Buffering::DoublePage);
    let mut scan = store.scan(range).unwrap();
    let first = scan.next_record().unwrap().unwrap();
    assert_eq!(first.key, 0);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 400..800 {
        match session.upsert(Serial(key), Put(key)).unwrap() {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
    }
    session.close(deadline()).unwrap();
    assert!(store.inner.log.frontiers().unwrap().head > range.end);
    let mut keys = vec![first.key];
    while let Some(record) = scan.next_record().unwrap() {
        keys.push(record.key);
    }
    assert_eq!(keys, (0..400).collect::<Vec<_>>());
    assert_eq!(first.value, Some(0));
    store.shutdown(deadline()).unwrap();
}

#[derive(Debug)]
struct Replace(u64);
impl Keyed<TestSchema> for Replace {
    fn key(&self) -> &u64 {
        &0
    }
}
impl UpsertOperation<TestSchema> for Replace {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, TestSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        value.view_mut().store(self.0, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(()))
    }
}
#[test]
fn 可变记录扫描不冻结值且旧输出不随原地更新变化() {
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), Replace(7)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let range = options(&store, Buffering::DoublePage);
    let mut old = store.scan(range).unwrap();
    let result = old.next_record().unwrap().unwrap();
    let mut later = store.scan(range).unwrap();
    assert!(matches!(
        session.upsert(Serial(1), Replace(9)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    assert_eq!(later.next_record().unwrap().unwrap().value, Some(9));
    assert_eq!(result.value, Some(7));
    assert_eq!(store.inner.log.frontiers().unwrap().tail, range.end);
    old.close().unwrap();
    later.close().unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 逻辑边界前移时已缓存页也必须报范围截断() {
    let (store, _) = setup();
    let range = options(&store, Buffering::DoublePage);
    let mut scan = store.scan(range).unwrap();
    let first = scan.next_record().unwrap().unwrap();
    // P7 尚未开放 shift_begin；此处只注入合法的逻辑边界发布，验证扫描观察契约。
    store
        .inner
        .log
        .advance_begin_for_scan_test(store.inner.log.frontiers().unwrap().head);
    assert!(matches!(scan.next_record(), Err(Error::RangeTruncated)));
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert!(matches!(store.scan(range), Err(Error::RangeTruncated)));
    assert_eq!(first.value, Some(0));
    scan.close().unwrap();
    store.shutdown(deadline()).unwrap();
}

struct PanicCodec(Arc<AtomicUsize>);
impl crate::schema::value::ValueCodec for PanicCodec {
    type Value = Vec<u8>;
    fn format_id(&self) -> FormatId {
        FormatId(*b"scan-panic-test1")
    }
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(value.clone())
    }
    fn decode(&self, _: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("注入扫描解码恐慌")
    }
}
type PanicSchema = SchemaPair<U64Key, crate::schema::builtin::SerializedValue<PanicCodec>>;
#[derive(Debug)]
struct PanicPut;
impl Keyed<PanicSchema> for PanicPut {
    fn key(&self) -> &u64 {
        &0
    }
}
impl UpsertOperation<PanicSchema> for PanicPut {
    type Output = ();
    fn replacement(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((vec![1], ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, PanicSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn 扫描专家解码恐慌只执行一次并使引擎失败关闭() {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        crate::schema::builtin::SerializedValue::new(PanicCodec(calls.clone())),
    ))
    .device(Box::new(crate::device::null::NullDeviceFactory))
    .create()
    .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), PanicPut).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let mut scan = store
        .scan(ScanOptions {
            begin: LogAddress(0),
            end: store.inner.log.frontiers().unwrap().tail,
            buffering: Buffering::Unbuffered,
        })
        .unwrap();
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(scan.next_record(), Err(Error::InvalidState(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.start_session(SessionOptions::default()),
        Err(Error::InvalidState(_))
    ));
    scan.close().unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 在途与放弃读取保护跨段映射但完成页不长期阻止删除() {
    let (store, control) = setup();
    let range = options(&store, Buffering::Unbuffered);
    let mut scan = store.scan(range).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    assert!(matches!(
        store.inner.storage.invalidate(0, Generation(0)),
        Err(Error::Busy)
    ));
    assert!(matches!(
        store.inner.storage.invalidate(1, Generation(0)),
        Err(Error::Busy)
    ));
    drop(scan);
    assert!(matches!(
        store.inner.storage.invalidate(0, Generation(0)),
        Err(Error::Busy)
    ));
    control.paused.store(false, Ordering::SeqCst);
    let mut next = store.scan(range).unwrap();
    assert_eq!(next.next_record().unwrap().unwrap().key, 0);
    // 当前页成为拥有副本后立即释放读取租约，扫描器仍然活跃。
    store.inner.storage.invalidate(0, Generation(0)).unwrap();
    next.close().unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 会话轮询推进闲置扫描的分段预读并及时释放租约() {
    let (store, control) = setup();
    let mut scan = store.scan(options(&store, Buffering::DoublePage)).unwrap();
    control.paused.store(true, Ordering::SeqCst);
    assert!(matches!(scan.next_record(), Err(Error::DeadlineExceeded)));
    control.paused.store(false, Ordering::SeqCst);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let stop = deadline();
    loop {
        session
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
        let ready = {
            let state = scan.state.state.lock().unwrap();
            state.pages.len() == 3
                && state
                    .pages
                    .iter()
                    .all(|page| page.cursor.is_some() && page.lease.is_none())
        };
        if ready {
            break;
        }
        assert!(!stop.expired());
    }
    store.inner.storage.invalidate(0, Generation(0)).unwrap();
    scan.close().unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

struct BlockingCodec {
    armed: Arc<AtomicBool>,
    entered: std::sync::mpsc::Sender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl crate::schema::value::ValueCodec for BlockingCodec {
    type Value = u64;
    fn format_id(&self) -> FormatId {
        crate::schema::builtin::U64ValueCodec.format_id()
    }
    fn encode(&self, value: &u64) -> Result<Vec<u8>, Error> {
        crate::schema::builtin::U64ValueCodec.encode(value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<u64, Error> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        crate::schema::builtin::U64ValueCodec.decode(bytes)
    }
}
type BlockingSchema = SchemaPair<U64Key, crate::schema::builtin::SerializedValue<BlockingCodec>>;
impl Keyed<BlockingSchema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<BlockingSchema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, BlockingSchema>,
    ) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn 拥有值解码期间发布逻辑截断仍必须拒绝交付失效范围() {
    let armed = Arc::new(AtomicBool::new(false));
    let (entered, seen) = std::sync::mpsc::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let codec = BlockingCodec {
        armed: armed.clone(),
        entered,
        resume: Mutex::new(resume),
    };
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        crate::schema::builtin::SerializedValue::new(codec),
    ))
    .config(config)
    .device(Box::new(crate::device::memory::MemoryDeviceFactory))
    .create()
    .unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        match session.upsert(Serial(key), Put(key)).unwrap() {
            Submission::Ready(result) => {
                result.unwrap();
            }
            Submission::Pending(mut ticket) => {
                session.wait(&mut ticket, deadline()).unwrap().unwrap();
            }
        }
    }
    session.close(deadline()).unwrap();
    let frontiers = store.inner.log.frontiers().unwrap();
    assert!(frontiers.head > LogAddress(0));
    let mut scan = store
        .scan(ScanOptions {
            begin: LogAddress(0),
            end: frontiers.tail,
            buffering: Buffering::DoublePage,
        })
        .unwrap();
    armed.store(true, Ordering::SeqCst);
    let worker = std::thread::spawn(move || scan.next_record());
    seen.recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    store.inner.log.advance_begin_for_scan_test(frontiers.head);
    release.send(()).unwrap();
    assert!(matches!(worker.join().unwrap(), Err(Error::RangeTruncated)));
    store.shutdown(deadline()).unwrap();
}

fn compaction_options(
    algorithm: crate::api::maintenance::CompactionAlgorithm,
    until: LogAddress,
) -> crate::api::maintenance::CompactionOptions {
    crate::api::maintenance::CompactionOptions {
        algorithm,
        until,
        workers: 1,
        shift_begin: false,
        checkpoint: false,
    }
}
// 受控内存设备没有操作系统 I/O 等待；用有限推进步数捕获卡死，避免共享 runner 调度改变功能验收。
fn finish_compaction(
    session: &mut crate::api::session::Session<TestSchema>,
    ticket: &crate::api::maintenance::MaintenanceTicket<crate::api::maintenance::CompactionReport>,
) -> crate::api::maintenance::SharedReport<crate::api::maintenance::CompactionReport> {
    for _ in 0..100_000 {
        if let Some(report) = ticket.try_report().unwrap() {
            return report;
        }
        session
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
    }
    panic!("有限输入的压缩超过推进步数上限");
}
#[derive(Debug)]
struct ReadCompaction(u64);
impl Keyed<TestSchema> for ReadCompaction {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl ReadOperation<TestSchema> for ReadCompaction {
    type Output = u64;
    fn read(&mut self, value: crate::schema::ValueRead<'_, TestSchema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
#[test]
fn 压缩轮询不等待设备且短读跨段与用户完成分流可仅由会话驱动() {
    use crate::api::maintenance::CompactionAlgorithm;
    for algorithm in [CompactionAlgorithm::Lookup, CompactionAlgorithm::ScanDedup] {
        let (store, control) = setup();
        let mut session = store.start_session(Default::default()).unwrap();
        control.reads.store(0, Ordering::SeqCst);
        control.paused.store(true, Ordering::SeqCst);
        control.short.store(true, Ordering::SeqCst);
        let until = store.inner.log.frontiers().unwrap().tail;
        let ticket = store
            .maintenance()
            .compact(compaction_options(algorithm, until))
            .unwrap();
        let budget = PollBudget(std::num::NonZeroUsize::new(1).unwrap());
        for _ in 0..20 {
            store.maintenance().poll(budget).unwrap();
            assert!(ticket.try_report().unwrap().is_none());
        }
        assert_eq!(control.reads.load(Ordering::SeqCst), 1);
        let Submission::Pending(mut user) = session
            .read(Serial(0), ReadCompaction(0), Default::default())
            .unwrap()
        else {
            panic!("用户冷读取应挂起")
        };
        assert_eq!(control.reads.load(Ordering::SeqCst), 2);
        assert!(matches!(
            session.wait_maintenance(&ticket, Deadline(Instant::now())),
            Err(Error::DeadlineExceeded)
        ));
        control.paused.store(false, Ordering::SeqCst);
        assert_eq!(
            finish_compaction(&mut session, &ticket)
                .as_ref()
                .as_ref()
                .unwrap()
                .copied,
            400
        );
        assert!(matches!(
            session.wait(&mut user, deadline()).unwrap().unwrap(),
            crate::api::completion::Outcome::Success(0)
        ));
        assert_eq!(store.inner.log.frontiers().unwrap().begin, LogAddress(0));
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
}
#[test]
fn 压缩读取失败不会自动重试且新动作可重新执行同一范围() {
    use crate::api::maintenance::CompactionAlgorithm;
    let (store, control) = setup();
    let mut session = store.start_session(Default::default()).unwrap();
    let until = store.inner.log.frontiers().unwrap().tail;
    control.reads.store(0, Ordering::SeqCst);
    control.fail.store(true, Ordering::SeqCst);
    let ticket = store
        .maintenance()
        .compact(compaction_options(CompactionAlgorithm::Lookup, until))
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*report, Err(Error::CompactionFailed { copied: 0, cause, .. }) if matches!(&**cause, Error::Io(_)))
    );
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);
    assert!(!store.inner.failed.load(Ordering::SeqCst));
    let ticket = store
        .maintenance()
        .compact(compaction_options(CompactionAlgorithm::Lookup, until))
        .unwrap();
    assert_eq!(
        finish_compaction(&mut session, &ticket)
            .as_ref()
            .as_ref()
            .unwrap()
            .copied,
        400
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 公开压缩专家恐慌终结报告一次且关闭释放任务没有实例引用环() {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        crate::schema::builtin::SerializedValue::new(PanicCodec(calls.clone())),
    ))
    .device(Box::new(crate::device::null::NullDeviceFactory))
    .create()
    .unwrap();
    let weak = Arc::downgrade(&store.inner);
    let mut session = store.start_session(Default::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(0), PanicPut).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let ticket = store
        .maintenance()
        .compact(compaction_options(
            crate::api::maintenance::CompactionAlgorithm::Lookup,
            store.inner.log.frontiers().unwrap().tail,
        ))
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    assert!(
        matches!(&*report, Err(Error::CompactionFailed { copied: 0, cause, .. }) if matches!(&**cause, Error::InvalidState(_)))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(store.maintenance().poll(PollBudget::default()).is_err());
    assert!(Arc::ptr_eq(&report, &ticket.try_report().unwrap().unwrap()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    assert!(weak.upgrade().is_none());
}
