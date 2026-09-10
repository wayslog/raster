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
