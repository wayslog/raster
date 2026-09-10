//! 会话持有本线程请求；不允许把用户上下文移动到设备线程。
use super::{
    completion::*,
    maintenance::{MaintenanceTicket, SharedReport},
    operation::*,
};
use crate::{
    engine::{Engine, SessionRuntime},
    schema::Schema,
    types::*,
};
use std::{marker::PhantomData, rc::Rc, sync::Arc};

#[derive(Clone, Copy, Debug, Default)]
pub struct SessionOptions {
    pub id: Option<SessionId>,
}
#[derive(Clone, Copy, Debug)]
pub struct CloseReport {
    pub session: SessionId,
    pub drained: bool,
}

/// 线程绑定会话；在途任务未来由本对象排空，而非由 Ticket 驱动。
///
/// ```compile_fail
/// use raster::{Session, schema::Schema};
/// fn require_send<T: Send>() {}
/// fn invalid<S: Schema>() { require_send::<Session<S>>(); }
/// ```
/// ```compile_fail
/// use raster::{Session, schema::Schema};
/// fn require_sync<T: Sync>() {}
/// fn invalid<S: Schema>() { require_sync::<Session<S>>(); }
/// ```
pub struct Session<S: Schema> {
    pub(crate) engine: Arc<Engine<S>>,
    pub(crate) id: SessionId,
    pub(crate) runtime: SessionRuntime,
    pub(crate) local: PhantomData<Rc<()>>,
}
impl<S: Schema> Session<S> {
    pub fn id(&self) -> SessionId {
        self.id
    }
    pub fn last_accepted(&self) -> Option<Serial> {
        self.runtime.current.last_accepted
    }
    pub fn read<R: ReadOperation<S>>(
        &mut self,
        serial: Serial,
        request: R,
        options: ReadOptions,
    ) -> Result<Submission<R::Output>, Rejected<R>> {
        self.engine
            .read(&mut self.runtime, serial, request, options)
    }
    pub fn upsert<U: UpsertOperation<S>>(
        &mut self,
        serial: Serial,
        request: U,
    ) -> Result<Submission<U::Output>, Rejected<U>> {
        self.engine.upsert(&mut self.runtime, serial, request)
    }
    pub fn rmw<M: RmwOperation<S>>(
        &mut self,
        serial: Serial,
        request: M,
        options: RmwOptions,
    ) -> Result<Submission<M::Output>, Rejected<M>> {
        self.engine.rmw(&mut self.runtime, serial, request, options)
    }
    pub fn delete<D: DeleteOperation<S>>(
        &mut self,
        serial: Serial,
        request: D,
        options: DeleteOptions,
    ) -> Result<Submission<D::Output>, Rejected<D>> {
        self.engine
            .delete(&mut self.runtime, serial, request, options)
    }
    pub fn refresh(&mut self) -> Result<Progress, Error> {
        Err(Error::unimplemented("coordination::refresh"))
    }
    pub fn poll(&mut self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("engine::poll"))
    }
    pub fn try_take<T: 'static>(
        &mut self,
        ticket: &mut Ticket<T>,
    ) -> Result<TicketState<T>, TicketError> {
        if ticket.id().store != self.engine.id || ticket.id().session != self.id {
            return Err(TicketError::WrongSession);
        }
        ticket.try_take()
    }
    pub fn wait<T: 'static>(
        &mut self,
        _ticket: &mut Ticket<T>,
        _deadline: Deadline,
    ) -> Result<OperationResult<T>, Error> {
        Err(Error::unimplemented("engine::wait"))
    }
    pub fn complete_pending(&mut self, _mode: WaitMode) -> Result<DrainReport, Error> {
        Err(Error::unimplemented("engine::drain"))
    }
    pub fn wait_maintenance<R>(
        &mut self,
        _ticket: &MaintenanceTicket<R>,
        _deadline: Deadline,
    ) -> Result<SharedReport<R>, Error> {
        Err(Error::unimplemented("maintenance::wait"))
    }
    /// 超时后保留 Session；成功只代表排空和注销，不自动检查点。
    pub fn close(&mut self, _deadline: Deadline) -> Result<CloseReport, Error> {
        Err(Error::unimplemented("coordination::close"))
    }
}
