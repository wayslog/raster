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

/// 线程绑定会话；本对象推进在途请求，Ticket 只收取结果。
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
    pub(crate) participant: Option<crate::epoch::ParticipantId>,
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
        if self.participant.is_none() {
            return Ok(Progress::default());
        }
        let phase_advanced = self.engine.observe_session(&mut self.runtime)?;
        Ok(Progress {
            completed: 0,
            remaining: self.runtime.pending(),
            phase_advanced,
        })
    }
    pub fn poll(&mut self, budget: PollBudget) -> Result<Progress, Error> {
        if self.participant.is_none() {
            return Ok(Progress::default());
        }
        self.engine.poll_session(&mut self.runtime, budget)
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
    /// 已完成结果可立即收取；截止时间只限制等待，不消费仍在途的票据。
    pub fn wait<T: 'static>(
        &mut self,
        ticket: &mut Ticket<T>,
        deadline: Deadline,
    ) -> Result<OperationResult<T>, Error> {
        loop {
            match self.try_take(ticket).map_err(|error| match error {
                TicketError::WrongSession => Error::InvalidState("票据不属于该存储或会话"),
                TicketError::AlreadyTaken => Error::InvalidState("票据结果已收取"),
                TicketError::AlreadyCompleted => Error::InvalidState("票据已经完成"),
                TicketError::BorrowConflict => Error::InvalidState("票据存在借用冲突"),
            })? {
                TicketState::Ready(result) => return Ok(result),
                TicketState::Pending => {}
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            self.poll(PollBudget::default())?;
            std::thread::yield_now();
        }
    }
    /// 只排空会话请求；Drained 不声明后台刷盘或检查点完成。
    pub fn complete_pending(&mut self, mode: WaitMode) -> Result<DrainReport, Error> {
        match mode {
            WaitMode::Once => {
                let progress = self.poll(PollBudget::default())?;
                Ok(if progress.remaining == 0 {
                    DrainReport::Drained
                } else {
                    DrainReport::Pending(progress)
                })
            }
            WaitMode::Until(deadline) => {
                while self.runtime.pending() != 0 {
                    if deadline.expired() {
                        return Err(Error::DeadlineExceeded);
                    }
                    self.poll(PollBudget::default())?;
                    std::thread::yield_now();
                }
                Ok(DrainReport::Drained)
            }
        }
    }
    pub fn wait_maintenance<R>(
        &mut self,
        _ticket: &MaintenanceTicket<R>,
        _deadline: Deadline,
    ) -> Result<SharedReport<R>, Error> {
        Err(Error::unimplemented("maintenance::wait"))
    }
    /// 超时后保留 Session；成功只代表排空和注销，不自动检查点。
    pub fn close(&mut self, deadline: Deadline) -> Result<CloseReport, Error> {
        if self.participant.is_some() {
            self.runtime.closing = true;
            self.complete_pending(WaitMode::Until(deadline))?;
            let participant = self.participant.expect("参与者存在");
            let current = (
                self.runtime.current.version,
                self.runtime.cut(self.runtime.current.version)?,
            );
            let previous = self
                .runtime
                .previous
                .as_ref()
                .map(|old| self.runtime.cut(old.version).map(|cut| (old.version, cut)))
                .transpose()?;
            self.engine.coordinator.leave_drained(current, previous)?;
            self.engine.epoch.unregister(participant)?;
            self.participant = None;
        }
        Ok(CloseReport {
            session: self.id,
            drained: true,
        })
    }
}

impl<S: Schema> Drop for Session<S> {
    fn drop(&mut self) {
        self.runtime.current.tasks.clear();
        if let Some(previous) = &mut self.runtime.previous {
            previous.tasks.clear();
        }
        if let Some(participant) = self.participant.take() {
            let _ = self.engine.epoch.unregister(participant);
            let _ = self.engine.coordinator.leave(self.id);
            if self
                .engine
                .coordinator
                .snapshot()
                .is_ok_and(|state| state.phase == crate::coordination::Phase::Failed)
            {
                self.engine
                    .failed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}
