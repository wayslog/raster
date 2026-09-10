//! 读取上下文由会话持有；磁盘等待与结果槽分离，回调只在推进线程执行。
use super::{
    Engine, SessionRuntime,
    io_hub::CompletionHub,
    pending::{PendingTask, TaskStep},
};
use crate::{
    api::{
        Submission,
        completion::{AbortReason, Completer, OperationResult, Outcome, Ticket, TicketState},
        operation::{ReadOperation, ReadOptions},
    },
    device::IoCompletion,
    log::lookup::{LogLookup, LookupStep},
    schema::{Schema, ValueRead},
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};
struct ReadTask<S: Schema, O: ReadOperation<S>> {
    engine: Arc<Engine<S>>,
    request: Option<O>,
    lookup: LogLookup,
    options: ReadOptions,
    complete: Completer<O::Output>,
    id: RequestId,
    serial: Serial,
    version: CheckpointVersion,
}
impl<S: Schema, O: ReadOperation<S>> ReadTask<S, O> {
    fn finish(&mut self, result: OperationResult<O::Output>) {
        if let Some(request) = self.request.take() {
            let result = self
                .engine
                .finish_request(request, result, Effect::NotApplied);
            let delivered = catch_unwind(AssertUnwindSafe(|| self.complete.finish(result)));
            if !matches!(delivered, Ok(Ok(()))) {
                self.engine.failed.store(true, Ordering::SeqCst);
            }
        }
    }
}
impl<S: Schema, O: ReadOperation<S>> PendingTask for ReadTask<S, O> {
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
            .accept(&self.engine.storage, completion)
            .map_err(|rejected| rejected.reason)
    }
    fn step(&mut self, budget: PollBudget) -> TaskStep {
        if self.request.is_none() {
            return TaskStep::Complete;
        }
        if self.engine.failed.load(Ordering::SeqCst) {
            self.abandon(OperationError {
                cause: Error::InvalidState("引擎已失败关闭"),
                effect: Effect::NotApplied,
            });
            return TaskStep::Complete;
        }
        let result = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Outcome<O::Output>>, Error> {
                let request = self.request.as_mut().expect("请求尚未终结");
                match self
                    .lookup
                    .step(&self.engine.log, &self.engine.storage, budget)?
                {
                    LookupStep::Continue | LookupStep::AwaitingIo => Ok(None),
                    LookupStep::Present => Err(Error::InvalidState("值查询只返回了元数据")),
                    LookupStep::Missing => Ok(Some(Outcome::NotFound)),
                    LookupStep::Tombstone => Ok(Some(if self.options.abort_if_tombstone {
                        Outcome::Aborted(AbortReason::Tombstone)
                    } else {
                        Outcome::NotFound
                    })),
                    LookupStep::Resident(value) => value
                        .read(|view| request.read(ValueRead { view }))?
                        .map(|value| Some(Outcome::Success(value))),
                    LookupStep::Decoded(value) => value
                        .read(|view| request.read(ValueRead { view }))?
                        .map(|value| Some(Outcome::Success(value))),
                }
            },
        ));
        match result {
            Ok(Ok(None)) => TaskStep::Retry,
            Ok(result) => {
                self.finish(
                    result
                        .map(|value| value.expect("已排除未完成结果"))
                        .map_err(|cause| OperationError {
                            cause,
                            effect: Effect::NotApplied,
                        }),
                );
                TaskStep::Complete
            }
            Err(_) => {
                self.engine.failed.store(true, Ordering::SeqCst);
                self.abandon(OperationError {
                    cause: Error::InvalidState("读取回调恐慌"),
                    effect: Effect::NotApplied,
                });
                TaskStep::Complete
            }
        }
    }
    fn abandon(&mut self, error: OperationError) {
        self.finish(Err(error));
    }
}
impl<S: Schema, O: ReadOperation<S>> Drop for ReadTask<S, O> {
    fn drop(&mut self) {
        if self.request.is_some() {
            self.abandon(OperationError {
                cause: Error::SessionAbandoned,
                effect: Effect::NotApplied,
            });
        }
        let _ = self.engine.io.release(self.id);
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn read<O: ReadOperation<S>>(
        self: &Arc<Self>,
        session: &mut SessionRuntime,
        serial: Serial,
        request: O,
        options: ReadOptions,
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
        let lookup = self
            .index
            .prepare(hash)
            .and_then(Self::head)
            .and_then(|head| {
                self.log.lookup(
                    &self.storage,
                    key,
                    head,
                    session.current.version,
                    CompletionHub::route(id),
                )
            });
        let lookup = match lookup {
            Ok(lookup) => lookup,
            Err(cause) => {
                let _ = self.io.release(id);
                return Ok(Submission::Ready(self.finish_request(
                    request,
                    Err(OperationError {
                        cause,
                        effect: Effect::NotApplied,
                    }),
                    Effect::NotApplied,
                )));
            }
        };
        let (mut ticket, complete) = Ticket::pair_bounded(id, credit);
        let mut task = ReadTask {
            engine: self.clone(),
            request: Some(request),
            lookup,
            options,
            complete,
            id,
            serial,
            version: session.current.version,
        };
        if matches!(task.step(PollBudget::default()), TaskStep::Complete) {
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
