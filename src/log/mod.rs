//! 混合日志的页状态、访问许可与分配接口；不直接调用用户操作。
use crate::{config::LogConfig, schema::value::ValuePlan, types::*};
use std::{marker::PhantomData, rc::Rc};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Frontiers {
    pub begin: LogAddress,
    pub head: LogAddress,
    pub safe_head: LogAddress,
    pub read_only: LogAddress,
    pub safe_read_only: LogAddress,
    pub flushed_until: LogAddress,
    pub tail: LogAddress,
}
/// 共享引擎的运行期边界；Frontiers 是读取后的逻辑快照。
pub(crate) struct AtomicFrontiers {
    begin: crate::sync::AtomicU64,
    head: crate::sync::AtomicU64,
    safe_head: crate::sync::AtomicU64,
    read_only: crate::sync::AtomicU64,
    safe_read_only: crate::sync::AtomicU64,
    flushed_until: crate::sync::AtomicU64,
    tail: crate::sync::AtomicU64,
}
pub(crate) struct PageState {
    pub id: PageId,
    pub generation: Generation,
    pub frozen: bool,
    pub closed: bool,
    pub flushed: bool,
    pub readers: usize,
    pub io_references: usize,
}
pub(crate) struct RecordReservation {
    address: LogAddress,
    plan: ValuePlan,
    generation: Generation,
}
pub(crate) struct RecordLease<'a> {
    address: LogAddress,
    generation: Generation,
    guard: PhantomData<&'a ()>,
    local: PhantomData<Rc<()>>,
}
pub(crate) struct MutationGate {
    active_writers: crate::sync::AtomicU64,
}
pub(crate) struct ReplacementPermit<'a> {
    gate: &'a MutationGate,
    local: PhantomData<Rc<()>>,
}
pub(crate) struct SharedUpdatePermit<'a> {
    gate: &'a MutationGate,
    local: PhantomData<Rc<()>>,
}
pub(crate) struct HybridLog {
    config: LogConfig,
    frontiers: AtomicFrontiers,
    // 页池控制元数据的同步入口，热点记录访问不使用整池独占锁。
    pages: crate::sync::Mutex<Vec<PageState>>,
}
impl HybridLog {
    pub fn reserve(&self, _plan: ValuePlan) -> Result<RecordReservation, Error> {
        Err(Error::unimplemented("log::reserve"))
    }
    pub fn finish_initialization(
        &self,
        _reservation: RecordReservation,
    ) -> Result<LogAddress, Error> {
        Err(Error::unimplemented("log::finish_initialization"))
    }
    pub fn lease(&self, _address: LogAddress) -> Result<RecordLease<'_>, Error> {
        Err(Error::unimplemented("log::lease"))
    }
    pub fn abandon(&self, _reservation: RecordReservation) -> Result<(), Error> {
        Err(Error::unimplemented("log::abandon"))
    }
    pub fn advance_read_only(&self, _target: LogAddress) -> Result<(), Error> {
        Err(Error::unimplemented("log::advance"))
    }
    pub fn flush_step(&self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("log::flush"))
    }
}
impl MutationGate {
    pub fn try_update(&self) -> Result<SharedUpdatePermit<'_>, Error> {
        Err(Error::unimplemented("log::mutation_gate"))
    }
    /// 不得带共享许可升级，也不得跨 Pending 持有此许可。
    pub fn try_replace(&self) -> Result<ReplacementPermit<'_>, Error> {
        Err(Error::unimplemented("log::mutation_gate"))
    }
}
