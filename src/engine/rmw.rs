//! RMW 磁盘等待后重查链头；空间不足时释放旧值并重新计算，不能覆盖并发更新。
use super::{
    Engine, SessionRuntime,
    pending::{PendingTask, TaskStep},
};
use crate::{
    api::{
        Submission,
        completion::{Completer, OperationResult, Outcome, Ticket, TicketState},
        operation::{RmwOperation, RmwOptions, UpdateDecision},
    },
    device::IoCompletion,
    index::{IndexHead, PublishResult},
    log::lookup::{LogLookup, LookupStep},
    schema::{Schema, ValueLayout, ValueRead, ValueUpdate},
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};
struct RmwTask<S: Schema, O: RmwOperation<S>> {
    engine: Arc<Engine<S>>,
    request: Option<O>,
    lookup: Option<(crate::index::EntrySnapshot, LogLookup)>,
    options: RmwOptions,
    skip_in_place: bool,
    key: Vec<u8>,
    hash: KeyHash,
    effect: Effect,
    complete: Completer<O::Output>,
    id: RequestId,
    serial: Serial,
    version: CheckpointVersion,
    permit: super::version_permit::VersionPermit,
}
impl<S: Schema, O: RmwOperation<S>> RmwTask<S, O> {
    fn advance(&mut self, budget: PollBudget) -> Result<Option<Outcome<O::Output>>, Error> {
        let engine = self.engine.clone();
        if self.lookup.is_none() {
            let resolved = engine.resolve_index(self.hash, &self.key)?;
            let entry = resolved.entry;
            let lookup = engine.log.lookup(
                &engine.storage,
                self.key.clone(),
                resolved.head,
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
        let request = self.request.as_mut().expect("请求尚未终结");
        let (value, output) = match source {
            LookupStep::Present => return Err(Error::InvalidState("值查询只返回了元数据")),
            LookupStep::Resident(lease) => {
                if !self.skip_in_place {
                    self.effect = Effect::Unknown;
                    match lease.update_at_version(self.version, |view| {
                        request.update_in_place(ValueUpdate { view })
                    })? {
                        Some(UpdateDecision::Updated(output)) => {
                            self.effect = Effect::Applied;
                            return Ok(Some(Outcome::Success(output)));
                        }
                        Some(UpdateDecision::Append) | None => {
                            self.effect = Effect::NotApplied;
                            self.skip_in_place = true;
                        }
                    }
                }
                lease.read(|view| request.copy_update(ValueRead { view }))??
            }
            LookupStep::Decoded(value) => {
                value.read(|view| request.copy_update(ValueRead { view }))??
            }
            LookupStep::Tombstone | LookupStep::Missing => {
                if !self.options.create_if_missing {
                    return Ok(Some(Outcome::NotFound));
                }
                request.initial()?
            }
            LookupStep::AwaitingIo | LookupStep::Continue => unreachable!("等待已返回"),
        };
        let plan = engine.schema.value_layout().plan(&value)?.validate()?;
        engine.log.record_fits(self.key.len(), plan)?;
        let allocation = match engine.log.allocate_record(
            &self.key,
            engine.resolve_index(self.hash, &self.key)?.head,
            plan,
        ) {
            Ok(allocation) => allocation,
            Err(Error::CapacityExceeded) if engine.storage.device.capabilities().supports_files => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let address = engine
            .log
            .finish_initialization(allocation.initialize(value)?.with_version(self.version))?;
        match engine
            .index
            .compare_publish(entry, IndexHead::Log(address))?
        {
            PublishResult::Published => {
                self.effect = Effect::Applied;
                Ok(Some(Outcome::Success(output)))
            }
            PublishResult::Conflict(_) => {
                engine.log.retire(address)?;
                Err(Error::Busy)
            }
        }
    }
    fn finish(&mut self, result: OperationResult<O::Output>) {
        if let Some(request) = self.request.take() {
            let result = self.engine.finish_request(request, result, self.effect);
            if !matches!(
                catch_unwind(AssertUnwindSafe(|| self.complete.finish(result))),
                Ok(Ok(()))
            ) {
                self.engine.failed.store(true, Ordering::SeqCst);
            }
        }
    }
    fn run_locked(&mut self, budget: PollBudget) -> TaskStep {
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
impl<S: Schema, O: RmwOperation<S>> PendingTask for RmwTask<S, O> {
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
            .ok_or(Error::InvalidState("RMW 没有等待磁盘查询"))?
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
impl<S: Schema, O: RmwOperation<S>> Drop for RmwTask<S, O> {
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
    pub(crate) fn rmw<O: RmwOperation<S>>(
        self: &Arc<Self>,
        session: &mut SessionRuntime,
        serial: Serial,
        request: O,
        options: RmwOptions,
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
        let mut task = RmwTask {
            engine: self.clone(),
            request: Some(request),
            lookup: None,
            options,
            skip_in_place: false,
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
            session.current.tasks.insert(id.slot, Box::new(task));
            Ok(Submission::Pending(ticket))
        }
    }
}
