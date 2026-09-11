//! Upsert 等待空间时保留已生成值；每次发布在同键仲裁内取得最新链头。
use super::{
    Engine, SessionRuntime,
    pending::{PendingTask, TaskStep},
};
use crate::{
    api::{
        Submission,
        completion::{Completer, OperationResult, Outcome, Ticket, TicketState},
        operation::{UpdateDecision, UpsertOperation},
    },
    device::IoCompletion,
    index::{IndexHead, PublishResult},
    log::ValueAccess,
    schema::{OwnedValueOf, Schema, ValueLayout, ValueUpdate, value::ValuePlan},
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};
struct Prepared<S: Schema, T> {
    value: Option<OwnedValueOf<S>>,
    output: Option<T>,
    plan: ValuePlan,
}
struct UpsertTask<S: Schema, O: UpsertOperation<S>> {
    engine: Arc<Engine<S>>,
    monitor: super::metrics::Monitor,
    request: Option<O>,
    prepared: Option<Prepared<S, O::Output>>,
    key: Vec<u8>,
    hash: KeyHash,
    effect: Effect,
    complete: Completer<O::Output>,
    id: RequestId,
    serial: Serial,
    version: CheckpointVersion,
    permit: super::version_permit::VersionPermit,
}
impl<S: Schema, O: UpsertOperation<S>> UpsertTask<S, O> {
    fn advance(&mut self) -> Result<Option<Outcome<O::Output>>, Error> {
        let engine = self.engine.clone();
        let resolved = engine.resolve_index(self.hash, &self.key)?;
        let entry = resolved.entry;
        let head = resolved.head;
        if self.prepared.is_none() {
            let request = self.request.as_mut().expect("请求未完成");
            if let Some(lease) = engine
                .log
                .find_mutable(&self.key, head)?
                .filter(|lease| !lease.is_tombstone())
            {
                self.effect = Effect::Unknown;
                match lease.update_at_version(self.version, |view| {
                    request.update_in_place(ValueUpdate { view })
                })? {
                    ValueAccess::Ready(Some(UpdateDecision::Updated(output))) => {
                        self.effect = Effect::Applied;
                        return Ok(Some(Outcome::Success(output)));
                    }
                    ValueAccess::Ready(Some(UpdateDecision::Append) | None) => {
                        self.effect = Effect::NotApplied
                    }
                    ValueAccess::Contended => {
                        self.effect = Effect::NotApplied;
                        return Ok(None);
                    }
                }
            }
            let (value, output) = request.replacement()?;
            let plan = engine.schema.value_layout().plan(&value)?.validate()?;
            engine.log.record_fits(self.key.len(), plan)?;
            self.prepared = Some(Prepared {
                value: Some(value),
                output: Some(output),
                plan,
            });
        }
        let prepared = self.prepared.as_mut().expect("已有拥有型值");
        let allocation = match engine.log.allocate_record(&self.key, head, prepared.plan) {
            Ok(allocation) => allocation,
            Err(Error::CapacityExceeded) if engine.storage.device.capabilities().supports_files => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let reservation = allocation.initialize(prepared.value.take().expect("尚未消费值"))?;
        let address = engine
            .log
            .finish_initialization(reservation.with_version(self.version))?;
        match engine
            .index
            .compare_publish(entry, IndexHead::Log(address))?
        {
            PublishResult::Published => {
                self.effect = Effect::Applied;
                let output = prepared.output.take().expect("尚未交付输出");
                self.prepared = None;
                Ok(Some(Outcome::Success(output)))
            }
            PublishResult::Conflict(_) => {
                engine.log.retire(address)?;
                self.monitor.invalidate();
                Err(Error::Busy)
            }
        }
    }
    fn finish(&mut self, mut result: OperationResult<O::Output>) {
        if catch_unwind(AssertUnwindSafe(|| drop(self.prepared.take()))).is_err() {
            self.engine.failed.store(true, Ordering::SeqCst);
            let _ = catch_unwind(AssertUnwindSafe(|| drop(result)));
            result = Err(OperationError {
                cause: Error::InvalidState("待追加值析构恐慌"),
                effect: self.effect,
            });
        }
        if let Some(request) = self.request.take() {
            let result = self.engine.finish_request(request, result, self.effect);
            self.engine
                .complete_tracked(&mut self.monitor, self.id, &self.complete, result);
        }
    }
    fn run_locked(&mut self) -> TaskStep {
        if self.request.is_none() {
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
        let result = match catch_unwind(AssertUnwindSafe(|| self.advance())) {
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
impl<S: Schema, O: UpsertOperation<S>> PendingTask for UpsertTask<S, O> {
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
        Err(Error::InvalidState("Upsert 不拥有该设备请求"))
    }
    fn step(&mut self, _: PollBudget) -> TaskStep {
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
        self.run_locked()
    }
    fn abandon(&mut self, error: OperationError) {
        self.finish(Err(error));
    }
}
impl<S: Schema, O: UpsertOperation<S>> Drop for UpsertTask<S, O> {
    fn drop(&mut self) {
        if self.request.is_some() {
            self.abandon(OperationError {
                cause: Error::SessionAbandoned,
                effect: self.effect,
            });
        }
        let _ = self.engine.io.release(self.id);
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn upsert<O: UpsertOperation<S>>(
        self: &Arc<Self>,
        session: &mut SessionRuntime,
        serial: Serial,
        request: O,
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
        let mut task = UpsertTask {
            monitor: self.metrics.accept(super::metrics::Kind::Upsert),
            engine: self.clone(),
            request: Some(request),
            prepared: None,
            key,
            hash,
            effect: Effect::NotApplied,
            complete,
            id,
            serial,
            version: session.current.version,
            permit,
        };
        if matches!(task.run_locked(), TaskStep::Complete) {
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
