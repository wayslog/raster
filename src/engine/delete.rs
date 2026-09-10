//! Delete 查询键与墓碑元数据，发布后才执行一次完成回调。
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
    log::lookup::{LogLookup, LookupStep},
    schema::Schema,
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};
struct DeleteTask<S: Schema, O: DeleteOperation<S>> {
    engine: Arc<Engine<S>>,
    request: Option<O>,
    lookup: Option<(crate::index::EntrySnapshot, LogLookup)>,
    options: DeleteOptions,
    completed: bool,
    key: Vec<u8>,
    hash: KeyHash,
    effect: Effect,
    complete: Completer<O::Output>,
    id: RequestId,
    serial: Serial,
    version: CheckpointVersion,
}
impl<S: Schema, O: DeleteOperation<S>> DeleteTask<S, O> {
    fn advance(&mut self, budget: PollBudget) -> Result<Option<Outcome<O::Output>>, Error> {
        let engine = self.engine.clone();
        let entry = if self.options.force_tombstone {
            engine.index.prepare(self.hash)?
        } else {
            if self.lookup.is_none() {
                let entry = engine.index.prepare(self.hash)?;
                let lookup = engine.log.lookup_metadata(
                    &engine.storage,
                    self.key.clone(),
                    Engine::<S>::head(entry)?,
                    self.version,
                    super::io_hub::CompletionHub::route(self.id),
                )?;
                self.lookup = Some((entry, lookup));
            }
            let (snapshot, lookup) = self.lookup.as_mut().expect("查询已创建");
            let source = lookup.step(&engine.log, &engine.storage, budget)?;
            if matches!(source, LookupStep::AwaitingIo | LookupStep::Continue) {
                return Ok(None);
            }
            let entry = engine.index.prepare(self.hash)?;
            let changed = entry != *snapshot;
            self.lookup = None;
            if changed {
                return Ok(None);
            }
            match source {
                LookupStep::Tombstone | LookupStep::Missing => return Ok(Some(Outcome::NotFound)),
                LookupStep::Present => {}
                _ => return Err(Error::InvalidState("删除查询返回了意外的值状态")),
            }
            entry
        };
        engine.log.tombstone_fits(self.key.len())?;
        let reservation = match engine
            .log
            .reserve_tombstone(&self.key, Engine::<S>::head(entry)?)
        {
            Ok(reservation) => reservation,
            Err(Error::CapacityExceeded) if engine.storage.device.capabilities().supports_files => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let address = engine.log.finish_initialization(reservation)?;
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
                Err(Error::Busy)
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
        if !matches!(
            catch_unwind(AssertUnwindSafe(|| self.complete.finish(result))),
            Ok(Ok(()))
        ) {
            self.engine.failed.store(true, Ordering::SeqCst);
        }
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
    fn on_io(&mut self, completion: IoCompletion) -> Result<(), Error> {
        self.lookup
            .as_mut()
            .ok_or(Error::InvalidState("Delete 没有等待磁盘查询"))?
            .1
            .accept(&self.engine.storage, completion)
            .map_err(|rejected| rejected.reason)
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
        if let Err(reason) = self.admit(session, serial) {
            let _ = self.io.release(id);
            return Err(Rejected { request, reason });
        }
        let (mut ticket, complete) = Ticket::pair_bounded(id, credit);
        let mut task = DeleteTask {
            engine: self.clone(),
            request: Some(request),
            lookup: None,
            options,
            completed: false,
            key,
            hash,
            effect: Effect::NotApplied,
            complete,
            id,
            serial,
            version: session.current.version,
        };
        if matches!(task.run_locked(PollBudget::default()), TaskStep::Complete) {
            let TicketState::Ready(result) = ticket.try_take().expect("内部票据可收取")
            else {
                unreachable!("任务已经完成")
            };
            Ok(Submission::Ready(result))
        } else {
            session.current.tasks.insert(id.slot, Box::new(task));
            Ok(Submission::Pending(ticket))
        }
    }
}
