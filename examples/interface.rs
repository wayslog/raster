//! 真实冷页请求展示异构票据、跳号、拒绝归还、超时续等和会话关闭后的结果所有权。
use raster::{
    RasterKV, Session, Submission,
    api::{Outcome, TicketState, operation::*},
    config::Config,
    device::memory::MemoryDeviceFactory,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(30))
}
#[derive(Debug)]
struct Put(u64);
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = ();
    fn replacement(&mut self) -> std::result::Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> std::result::Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[derive(Debug)]
struct Number {
    key: u64,
    callbacks: Rc<Cell<usize>>,
}
impl Keyed<Schema> for Number {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadOperation<Schema> for Number {
    type Output = u64;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<u64, Error> {
        self.callbacks.set(self.callbacks.get() + 1);
        Ok(*value.view())
    }
}
#[derive(Debug)]
struct Text(Number);
impl Keyed<Schema> for Text {
    fn key(&self) -> &u64 {
        &self.0.key
    }
}
impl ReadOperation<Schema> for Text {
    type Output = String;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> std::result::Result<String, Error> {
        self.0.callbacks.set(self.0.callbacks.get() + 1);
        Ok(format!("值为{}", value.view()))
    }
}
fn take<T: 'static>(
    session: &mut Session<Schema>,
    submission: Submission<T>,
) -> Result<Outcome<T>> {
    Ok(match submission {
        Submission::Ready(result) => result?,
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline())??,
    })
}
fn demo(store: &RasterKV<Schema>) -> Result<()> {
    let mut session = store.start_session(Default::default())?;
    for n in 0..400 {
        let submitted = session
            .upsert(Serial(n), Put(n))
            .map_err(|rejected| rejected.reason)?;
        if !matches!(take(&mut session, submitted)?, Outcome::Success(())) {
            return Err("初始化写入失败".into());
        }
    }
    let callbacks = Rc::new(Cell::new(0));
    let number = session
        .read(
            Serial(400),
            Number {
                key: 0,
                callbacks: callbacks.clone(),
            },
            ReadOptions::default(),
        )
        .map_err(|rejected| rejected.reason)?;
    let text = session
        .read(
            Serial(410),
            Text(Number {
                key: 1,
                callbacks: callbacks.clone(),
            }),
            ReadOptions::default(),
        )
        .map_err(|rejected| rejected.reason)?;
    let (Submission::Pending(mut number), Submission::Pending(mut text)) = (number, text) else {
        return Err("小内存配置没有产生预期的两次冷页挂起".into());
    };
    let rejected = match session.read(
        Serial(405),
        Number {
            key: 99,
            callbacks: callbacks.clone(),
        },
        ReadOptions::default(),
    ) {
        Err(rejected) => rejected,
        Ok(_) => return Err("倒退序号被意外接受".into()),
    };
    if rejected.request.key != 99
        || session.last_accepted() != Some(Serial(410))
        || callbacks.get() != 0
    {
        return Err("拒绝没有完整归还请求或改变了接受进度".into());
    }
    // 已经过期的截止时间不消费仍在途票据，也不重新提交读取。
    if !matches!(
        session.wait(&mut number, Deadline(Instant::now())),
        Err(Error::DeadlineExceeded)
    ) {
        return Err("预期有界等待超时".into());
    }
    if !matches!(number.try_take(), Ok(TicketState::Pending)) {
        return Err("超时改变了票据状态".into());
    }
    let before = store.diagnostics()?;
    if before.pending_requests != 2 {
        return Err("诊断中的挂起数量不匹配".into());
    }
    session.refresh()?;
    let _progress = session.complete_pending(WaitMode::Once)?;
    session.close(deadline())?;
    drop(session);
    let number = match number
        .try_take()
        .map_err(|e| format!("数字票据错误：{e:?}"))?
    {
        TicketState::Ready(result) => result?,
        TicketState::Pending => return Err("会话关闭后请求仍未完成".into()),
    };
    let output = match text
        .try_take()
        .map_err(|e| format!("文本票据错误：{e:?}"))?
    {
        TicketState::Ready(result) => result?,
        TicketState::Pending => return Err("会话关闭后文本仍未完成".into()),
    };
    if !matches!(number, Outcome::Success(0))
        || !matches!(output, Outcome::Success(ref value) if value == "值为1")
        || callbacks.get() != 2
    {
        return Err("异构结果或回调次数不匹配".into());
    }
    if !matches!(text.try_take(), Err(TicketError::AlreadyTaken)) {
        return Err("同一票据被重复收取".into());
    }
    let after = store.diagnostics()?;
    if after.active_sessions != 0 || after.active_requests != 0 {
        return Err("关闭后仍有活跃会话或请求".into());
    }
    println!(
        "公开票据生命周期通过：两次异构 Pending，跳号接受，倒退拒绝，超时不取消，关闭后收取拥有型结果，每个回调一次。"
    );
    Ok(())
}
fn main() -> Result<()> {
    fn shared<T: Send + Sync>() {}
    shared::<RasterKV<Schema>>();
    let mut config = Config::default();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(MemoryDeviceFactory))
        .create()?;
    let result = demo(&store);
    let cleanup = store.shutdown(deadline());
    match (result, cleanup) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Err(error), Err(cleanup)) => Err(format!("示例失败：{error}；收尾失败：{cleanup}").into()),
    }
}
