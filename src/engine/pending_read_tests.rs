//! 用公开 Session/Ticket 驱动已淘汰记录读取，刷盘准备仍由内部测试接口完成。
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
        panic!("打开")
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
fn 会话轮询隔离回调且结果预算在收取后释放() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let mut second = store.start_session(SessionOptions::default()).unwrap();
    let a = Rc::new(Cell::new(0));
    let b = Rc::new(Cell::new(0));
    let Submission::Pending(mut ta) = first
        .read(Serial(0), request(0, &a), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("应挂起")
    };
    let Submission::Pending(mut tb) = second
        .read(Serial(0), request(1, &b), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("应挂起")
    };
    assert!(
        first
            .read(Serial(1), request(0, &a), ReadOptions::default())
            .is_err()
    );
    for _ in 0..4 {
        first.poll(PollBudget::default()).unwrap();
    }
    assert_eq!(a.get(), 1);
    assert_eq!(b.get(), 0);
    assert!(matches!(tb.try_take(), Ok(TicketState::Pending)));
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
    for _ in 0..4 {
        second.poll(PollBudget::default()).unwrap();
    }
    assert_eq!(b.get(), 1);
    assert!(
        matches!(tb.try_take(), Ok(TicketState::Ready(Ok(Outcome::Success(value)))) if *value==1)
    );
}
#[test]
fn 丢弃票据仍执行读取而丢弃会话明确终结未执行请求() {
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(ticket) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("应挂起")
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
        panic!("应挂起")
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
fn 公开写入轮询驱动自动刷盘且两页预算可读回多窗口数据() {
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
                assert!(done, "磁盘读取没有在预算内终结");
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
        panic!("本测试只插入新键或替换已淘汰的冷键")
    }
}
#[test]
fn 容量不足的写入挂起且恢复后不重复调用替换回调() {
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
                assert!(done, "等待空间的写入没有终结");
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
            result.expect("挂起更新未终结")
        }
    };
    match result.unwrap() {
        Outcome::Success(value) => value,
        _ => panic!("预期成功更新"),
    }
}
#[test]
fn 冷键查询期间发生替换时先重查新值再计算() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let mut second = store.start_session(SessionOptions::default()).unwrap();
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
    assert_eq!(finish_number(&mut second, replacement), 100);
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
fn 读改写新建跨越容量窗口且两个冷键增量不会丢失() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let mut second = store.start_session(SessionOptions::default()).unwrap();
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
    let b = second
        .rmw(Serial(0), request(0), RmwOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(a, Submission::Pending(_)) && matches!(b, Submission::Pending(_)));
    assert_eq!(finish_number(&mut first, a), 1);
    assert_eq!(finish_number(&mut second, b), 2);
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
    assert_eq!(copies.get(), 2);
    let reads = Rc::new(Cell::new(0));
    for key in [0, 1000, 1199, 1399] {
        let serial = Serial(2000 + key);
        let result = second
            .read(serial, self::request(key, &reads), ReadOptions::default())
            .map_err(|r| r.reason)
            .unwrap();
        let result = match result {
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
        assert!(
            matches!(result, Ok(Outcome::Success(value)) if *value == if key == 0 { 2 } else { 1 })
        );
    }
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
        assert!(!self.panic, "删除完成回调恐慌");
        self.key
    }
}
#[test]
fn 删除等待期间重查替换记录且强制墓碑可以等待空间() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let mut second = store.start_session(SessionOptions::default()).unwrap();
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
    assert!(matches!(pending, Submission::Pending(_)));
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
    assert_eq!(finish_number(&mut second, replace), 100);
    assert_eq!(finish_number(&mut first, pending), 0);
    let missing = first
        .delete(Serial(1), delete(999), DeleteOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    assert!(matches!(missing, Submission::Ready(Ok(Outcome::NotFound))));
    assert_eq!(calls.get(), 1);
    let mut waiting = 0;
    for i in 0..400 {
        let result = first
            .delete(
                Serial(i + 2),
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
        assert_eq!(calls.get(), i as usize + 2);
    }
    assert!(waiting >= 3);
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
}
#[test]
fn 磁盘删除完成回调恐慌只终结一次且保留已生效墓碑() {
    use crate::schema::KeyCodec;
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = session
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
        .unwrap()
    else {
        panic!("应等待磁盘")
    };
    let mut result = None;
    for _ in 0..100 {
        session.poll(PollBudget::default()).unwrap();
        if let TicketState::Ready(value) = ticket.try_take().unwrap() {
            result = Some(value);
            break;
        }
    }
    assert!(matches!(
        result.unwrap(),
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
    assert!(matches!(ticket.try_take(), Err(TicketError::AlreadyTaken)));
}

fn wait_deadline() -> Deadline {
    Deadline(std::time::Instant::now() + std::time::Duration::from_secs(5))
}
#[test]
fn 等待超时保留请求且错会话不能推进票据() {
    let store = setup();
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let mut second = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = first
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("应挂起")
    };
    assert!(matches!(
        second.wait(&mut ticket, wait_deadline()),
        Err(Error::InvalidState(_))
    ));
    assert_eq!(calls.get(), 0);
    assert!(matches!(
        first.wait(&mut ticket, Deadline(std::time::Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert!(matches!(ticket.try_take(), Ok(TicketState::Pending)));
    assert!(
        matches!(first.wait(&mut ticket, wait_deadline()).unwrap(), Ok(Outcome::Success(value)) if *value==0)
    );
    assert_eq!(calls.get(), 1);
    assert!(matches!(
        first.wait(&mut ticket, wait_deadline()),
        Err(Error::InvalidState(_))
    ));
}
#[test]
fn 关闭超时后停止接受但仍可排空并在关闭后收取结果() {
    let store = setup();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let calls = Rc::new(Cell::new(0));
    let Submission::Pending(mut ticket) = session
        .read(Serial(0), request(0, &calls), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap()
    else {
        panic!("应挂起")
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
    // 已完成结果不因为等待截止时间已过而丢失。
    assert!(
        matches!(session.wait(&mut ticket, Deadline(std::time::Instant::now())).unwrap(), Ok(Outcome::Success(value)) if *value==0)
    );
    assert!(session.close(wait_deadline()).unwrap().drained);
}
#[test]
fn 单次排空报告剩余且截止时间排空不消费票据结果() {
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
    let mut ticket = ticket.expect("写满日志应挂起");
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
fn 设备延迟期间等待超时后仍可恢复完成() {
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
    let engine = Arc::get_mut(&mut store.inner).expect("测试会话已退出");
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
        panic!("应挂起")
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
    // 第一次轮询只冻结和编码第一页；尚未向设备提交目录、打开或写入操作。
    session.poll(PollBudget::default()).unwrap();
    session.close(wait_deadline()).unwrap();
    (store, probe, device)
}
#[test]
fn 存储关闭完成已启动页的目录打开与写入各阶段() {
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
fn 存储排空超时不会提前关闭设备且恢复后可以继续() {
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
fn 刷盘失败仍排空设备但不发布成功边界也不自动重试() {
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
fn 引擎已失败时关闭只归还在途缓冲而不推进刷盘边界() {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, probe, _) = shutdown_setup();
    // 目录与打开已完成，页写入尚未返回。
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
fn 放弃会话后的迟到读取由存储关闭排空且不会调用用户() {
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
        panic!("应挂起")
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
fn 原生文件存储关闭排空已启动页并保留可校验帧() {
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
fn 单位预算公平推进多请求且排空后未收结果仍施加背压() {
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
            panic!("应挂起")
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
        assert!(!deadline.expired(), "单位预算不得饿死其他请求");
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
fn 并发关闭只允许一个推进者且另一调用立即返回繁忙() {
    use std::sync::atomic::Ordering::SeqCst;
    let (store, probe, _) = shutdown_setup();
    probe.paused.store(true, SeqCst);
    let worker_store = store.clone();
    let worker = std::thread::spawn(move || worker_store.shutdown(wait_deadline()));
    let deadline = wait_deadline();
    while probe.submitted.load(SeqCst) == 0 {
        assert!(!deadline.expired(), "关闭线程应开始提交后台 I/O");
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
fn 真实挂起读取跨版本切分后保留旧身份与序号并独立完成() {
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
        panic!("应挂起")
    };
    let old_id = old_ticket.id();
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
        panic!("应挂起")
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
fn 维护版本前进后新注册会话继承原子返回的版本() {
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
    // 此处只驱动协调器组件，没有写出检查点，也没有公开成功检查点 API。
    let session = store.start_session(SessionOptions::default()).unwrap();
    assert_eq!(session.runtime.current.version, CheckpointVersion(1));
    assert!(session.runtime.previous.is_none());
}

#[test]
fn 正常关闭保留动作前和动作中的会话切分且不伪造放弃失败() {
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
fn 切分后接受的新序号不进入旧检查点关闭切分() {
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
        panic!("应挂起")
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
fn 新版本替换与读改写不原地修改旧记录但同版本仍可更新() {
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
