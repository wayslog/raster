//! The session holds this thread's request;Moving user context to the device thread is not allowed.
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

/// Thread bound session;This object advances requests in transit,Ticket Only receive results.
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
    pub(crate) thread_session: Option<crate::engine::thread_sessions::ThreadSession>,
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
        let engine = &self.engine;
        let _guard = match self
            .participant
            .ok_or(Error::InvalidState("session_closed"))
            .and_then(|participant| engine.epoch.enter(participant))
        {
            Ok(guard) => guard,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        engine.read(&mut self.runtime, serial, request, options)
    }
    pub fn upsert<U: UpsertOperation<S>>(
        &mut self,
        serial: Serial,
        request: U,
    ) -> Result<Submission<U::Output>, Rejected<U>> {
        let engine = &self.engine;
        let _guard = match self
            .participant
            .ok_or(Error::InvalidState("session_closed"))
            .and_then(|participant| engine.epoch.enter(participant))
        {
            Ok(guard) => guard,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        engine.upsert(&mut self.runtime, serial, request)
    }
    pub fn rmw<M: RmwOperation<S>>(
        &mut self,
        serial: Serial,
        request: M,
        options: RmwOptions,
    ) -> Result<Submission<M::Output>, Rejected<M>> {
        let engine = &self.engine;
        let _guard = match self
            .participant
            .ok_or(Error::InvalidState("session_closed"))
            .and_then(|participant| engine.epoch.enter(participant))
        {
            Ok(guard) => guard,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        engine.rmw(&mut self.runtime, serial, request, options)
    }
    pub fn delete<D: DeleteOperation<S>>(
        &mut self,
        serial: Serial,
        request: D,
        options: DeleteOptions,
    ) -> Result<Submission<D::Output>, Rejected<D>> {
        let engine = &self.engine;
        let _guard = match self
            .participant
            .ok_or(Error::InvalidState("session_closed"))
            .and_then(|participant| engine.epoch.enter(participant))
        {
            Ok(guard) => guard,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        engine.delete(&mut self.runtime, serial, request, options)
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
        let engine = &self.engine;
        let _guard = engine
            .epoch
            .enter(self.participant.expect("Confirmed participant exists"))?;
        engine.poll_session(&mut self.runtime, budget)
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
    /// Completed results are available immediately;Deadline only limits waiting,Do not consume bills that are still in transit.
    pub fn wait<T: 'static>(
        &mut self,
        ticket: &mut Ticket<T>,
        deadline: Deadline,
    ) -> Result<OperationResult<T>, Error> {
        loop {
            match self.try_take(ticket).map_err(|error| match error {
                TicketError::WrongSession => {
                    Error::InvalidState("Ticket does not belong to this store or session")
                }
                TicketError::AlreadyTaken => {
                    Error::InvalidState("The ticket result has been collected")
                }
                TicketError::AlreadyCompleted => {
                    Error::InvalidState("The ticket has been completed")
                }
                TicketError::BorrowConflict => {
                    Error::InvalidState("The note has a borrowing conflict")
                }
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
    /// Drain session requests only;Drained Do not declare background flush or checkpoint completion.
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
    /// Advance conversation in this thread,Wait for automatic maintenance to idle or stop;Accepted tasks will not be canceled after timeout.
    pub fn wait_auto_compaction(
        &mut self,
        deadline: Deadline,
    ) -> Result<super::maintenance::AutoCompactionStatus, Error> {
        loop {
            let status = self.engine.auto_compaction_status()?;
            if status.is_quiescent() {
                return Ok(status);
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            // The scheduling thread is responsible for summarizing maintenance failures;It is still necessary to advance the completion and failure closure of this session..
            if let Err(error) = self.poll(PollBudget::default())
                && !self.engine.failed.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(error);
            }
            self.engine.auto_compaction.wait_change(deadline)?;
        }
    }
    /// Promote old version requests and maintenance in this thread,Returns shared final report of repeatable observations.
    /// Waiting errors are handled separately from maintenance errors within reports;Not canceling ticket after timeout.
    pub fn wait_maintenance<R>(
        &mut self,
        ticket: &MaintenanceTicket<R>,
        deadline: Deadline,
    ) -> Result<SharedReport<R>, Error> {
        if ticket.store != self.engine.id {
            return Err(Error::InvalidState(
                "Maintenance tickets belong to other storage",
            ));
        }
        loop {
            if let Some(report) = ticket.try_report()? {
                return Ok(report);
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            if let Err(error) = self
                .poll(PollBudget::default())
                .and_then(|_| self.engine.poll_maintenance(PollBudget::default()))
            {
                self.engine.fail_checkpoint()?;
                self.engine.fail_growth()?;
                self.engine.fail_compaction()?;
                self.engine.fail_gc()?;
                self.engine.fail_checkpoint_release()?;
                if let Some(report) = ticket.try_report()? {
                    return Ok(report);
                }
                if self.engine.compaction_report_pending(ticket.id)? {
                    std::thread::yield_now();
                    continue;
                }
                return Err(error);
            }
            std::thread::yield_now();
        }
    }
    /// retained after timeout Session;Success only means emptying and logging out,No automatic checkpoints.
    pub fn close(&mut self, deadline: Deadline) -> Result<CloseReport, Error> {
        if self.participant.is_some() {
            self.runtime.closing = true;
            self.complete_pending(WaitMode::Until(deadline))?;
            let participant = self.participant.expect("Participants exist");
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
            self.thread_session = None;
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
