//! Delete 只检查驻留记录，盲删发布后执行一次完成回调。
use super::{
    Engine, SessionRuntime,
    pending::{PendingTask, TaskStep},
};
use crate::{
    api::{
        Submission,
        completion::{Completer, OperationResult, Outcome, Ticket, TicketState},
        operation::{DeleteOperation, DeleteOptions, DeleteOutcome},
    },
    device::IoCompletion,
    index::{IndexHead, PublishResult},
    log::ValueAccess,
    schema::Schema,
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};
struct DeleteTask<S: Schema, O: DeleteOperation<S>> {
    engine: Arc<Engine<S>>,
    monitor: super::metrics::Monitor,
    request: Option<O>,
    options: DeleteOptions,
    completed: bool,
    key: Vec<u8>,
    hash: KeyHash,
    effect: Effect,
    complete: Completer<O::Output>,
    id: RequestId,
    serial: Serial,
    version: CheckpointVersion,
    permit: super::version_permit::VersionPermit,
}
impl<S: Schema, O: DeleteOperation<S>> DeleteTask<S, O> {
    fn advance(&mut self, _budget: PollBudget) -> Result<Option<Outcome<O::Output>>, Error> {
        let engine = self.engine.clone();
        let resolved = engine.resolve_index(self.hash, &self.key)?;
        let entry = resolved.entry;
        if !self.options.force_tombstone && !entry.present {
            return Ok(Some(Outcome::NotFound));
        }
        if let Some(lease) = engine.log.find_mutable(&self.key, resolved.head)? {
            let begin = engine.log.frontiers()?.begin;
            let remove = !self.options.force_tombstone
                && resolved.head == Some(lease.address()?)
                && lease.previous().is_none_or(|previous| previous < begin);
            let head = if remove {
                IndexHead::Empty
            } else {
                resolved.head.map_or(IndexHead::Empty, IndexHead::Log)
            };
            match lease.tombstone_at_version(self.version, || {
                engine
                    .index
                    .compare_publish(entry, head)
                    .map(|result| matches!(result, PublishResult::Published))
            })? {
                ValueAccess::Ready(Some(true)) => {
                    self.effect = Effect::Applied;
                    return Ok(Some(Outcome::Success(
                        self.request
                            .take()
                            .expect("请求尚未完成")
                            .complete(if remove {
                                DeleteOutcome::IndexRemoved
                            } else {
                                DeleteOutcome::TombstoneWritten
                            }),
                    )));
                }
                ValueAccess::Contended | ValueAccess::Ready(Some(false)) => return Ok(None),
                ValueAccess::Ready(None) => {}
            }
        }
        engine.log.tombstone_fits(self.key.len())?;
        let reservation = match engine.log.reserve_tombstone(&self.key, resolved.head) {
            Ok(reservation) => reservation,
            Err(Error::CapacityExceeded) if engine.storage.device.capabilities().supports_files => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let address = engine
            .log
            .finish_initialization(reservation.with_version(self.version))?;
        match engine
            .index
            .compare_publish(entry, IndexHead::Log(address))?
        {
            PublishResult::Published => {
                self.effect = Effect::Applied;
                Ok(Some(Outcome::Success(
                    self.request
                        .take()
                        .expect("请求尚未完成")
                        .complete(DeleteOutcome::TombstoneWritten),
                )))
            }
            PublishResult::Conflict(_) => {
                engine.log.retire(address)?;
                self.monitor.invalidate();
                Ok(None)
            }
        }
    }
    fn finish(&mut self, result: OperationResult<O::Output>) {
        let result = if let Some(request) = self.request.take() {
            self.engine.finish_request(request, result, self.effect)
        } else {
            result
        };
        self.completed = true;
        self.engine
            .complete_tracked(&mut self.monitor, self.id, &self.complete, result);
    }
    fn run_locked(&mut self, budget: PollBudget) -> TaskStep {
        if self.completed {
            return TaskStep::Complete;
        }
        if self.engine.failed.load(Ordering::SeqCst) {
            self.abandon(OperationError {
                cause: Error::InvalidState("引擎已失败关闭"),
                effect: self.effect,
            });
            return TaskStep::Complete;
        }
        match self.permit.ready() {
            Ok(true) => {}
            Ok(false) => return TaskStep::Retry,
            Err(cause) => {
                self.abandon(OperationError {
                    cause,
                    effect: self.effect,
                });
                return TaskStep::Complete;
            }
        }
        let result = match catch_unwind(AssertUnwindSafe(|| self.advance(budget))) {
            Ok(result) => result,
            Err(_) => {
                self.engine.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("写入回调恐慌"))
            }
        };
        if matches!(result, Ok(None)) {
            return TaskStep::Retry;
        }
        if result.is_err() && self.effect == Effect::Unknown {
            self.engine.failed.store(true, Ordering::SeqCst);
        }
        self.finish(
            result
                .map(|value| value.expect("已排除等待空间"))
                .map_err(|cause| OperationError {
                    cause,
                    effect: self.effect,
                }),
        );
        TaskStep::Complete
    }
}
impl<S: Schema, O: DeleteOperation<S>> PendingTask for DeleteTask<S, O> {
    fn id(&self) -> RequestId {
        self.id
    }
    fn serial(&self) -> Serial {
        self.serial
    }
    fn version(&self) -> CheckpointVersion {
        self.version
    }
    fn on_io(&mut self, _: IoCompletion) -> Result<(), Error> {
        Err(Error::InvalidState("盲删没有等待磁盘查询"))
    }
    fn step(&mut self, budget: PollBudget) -> TaskStep {
        let engine = self.engine.clone();
        let _gate =
            match engine.operations[self.hash.0 as usize % engine.operations.len()].try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => return TaskStep::Retry,
                Err(_) => {
                    self.abandon(OperationError {
                        cause: Error::InvalidState("操作仲裁锁中毒"),
                        effect: self.effect,
                    });
                    return TaskStep::Complete;
                }
            };
        self.run_locked(budget)
    }
    fn abandon(&mut self, error: OperationError) {
        self.finish(Err(error));
    }
}
impl<S: Schema, O: DeleteOperation<S>> Drop for DeleteTask<S, O> {
    fn drop(&mut self) {
        if !self.completed {
            self.abandon(OperationError {
                cause: Error::SessionAbandoned,
                effect: self.effect,
            });
        }
        let _ = self.engine.io.release(self.id);
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn delete<O: DeleteOperation<S>>(
        self: &Arc<Self>,
        session: &mut SessionRuntime,
        serial: Serial,
        request: O,
        options: DeleteOptions,
    ) -> Result<Submission<O::Output>, Rejected<O>> {
        if let Err(reason) = self.observe_session(session) {
            return Err(Rejected { request, reason });
        }
        let (hash, key) = match self.prepare(session, serial, &request) {
            Ok(value) => value,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        let _gate = match self.operations[hash.0 as usize % self.operations.len()].try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                return Err(Rejected {
                    request,
                    reason: Error::Busy,
                });
            }
        };
        if session.pending() >= self.config.session.max_pending {
            return Err(Rejected {
                request,
                reason: Error::Busy,
            });
        }
        let credit = match session.reserve_result(self.config.session.max_results) {
            Ok(credit) => credit,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        let id = match self.io.reserve(session.id) {
            Ok(id) => id,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        let permit = match self.version_permits.reserve(hash, session.current.version) {
            Ok(permit) => permit,
            Err(reason) => {
                let _ = self.io.release(id);
                return Err(Rejected { request, reason });
            }
        };
        if let Err(reason) = self.admit(session, serial) {
            let _ = self.io.release(id);
            return Err(Rejected { request, reason });
        }
        let (mut ticket, complete) = Ticket::pair_bounded(id, credit);
        let mut task = DeleteTask {
            monitor: self.metrics.accept(super::metrics::Kind::Delete),
            engine: self.clone(),
            request: Some(request),
            options,
            completed: false,
            key,
            hash,
            effect: Effect::NotApplied,
            complete,
            id,
            serial,
            version: session.current.version,
            permit,
        };
        if matches!(task.run_locked(PollBudget::default()), TaskStep::Complete) {
            let TicketState::Ready(result) = ticket.try_take().expect("内部票据可收取")
            else {
                unreachable!("任务已经完成")
            };
            Ok(Submission::Ready(result))
        } else {
            task.monitor.pending();
            session.current.tasks.insert(id.slot, Box::new(task));
            Ok(Submission::Pending(ticket))
        }
    }
}
