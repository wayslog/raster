//! 用真实值许可制造刷页同类争用；通过公开 Session/Ticket 验证等待与用户错误的边界。
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
        panic!("已有日志记录")
    };
    let calls = Rc::new(Cell::new(0));
    let (submission, pending, before, failed) = std::thread::scope(|scope| {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = &store;
        let holder = scope.spawn(move || {
            // 与刷页的稳定编码一样，独占值许可不占用业务条带锁。
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
        "内部许可争用必须保留原请求等待，不能作为业务 Busy 终结"
    );
    assert_eq!(before, 0);
    assert!(!failed, "未调用用户代码的争用不能失败关闭");
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
fn 冻结源读取争用使读改写等待且释放后只计算一次() {
    contention(Mode::RmwCopy)
}
#[test]
fn 驻留读取争用不返回业务繁忙错误() {
    contention(Mode::Read)
}
#[test]
fn 原地写入许可争用不错误失败关闭() {
    contention(Mode::Upsert)
}
#[test]
fn 原地读改写许可争用不错误失败关闭() {
    contention(Mode::Rmw)
}
#[test]
fn 用户读取和复制回调的繁忙错误终结一次且不重放() {
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
