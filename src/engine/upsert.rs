//! Upsert Keep generated values while waiting for space;Get the latest link head in the same key quorum for each release.
use super::{
    Engine, SessionRuntime,
    pending::{PendingTask, TaskStep},
};
use crate::{
    api::{
        Submission,
        completion::{Completer, OperationResult, Outcome, Ticket},
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
enum UpsertStep<T> {
    Ready(OperationResult<T>),
    Retry,
}
struct UpsertState<S: Schema, O: UpsertOperation<S>> {
    monitor: super::metrics::Monitor,
    request: Option<O>,
    prepared: Option<Prepared<S, O::Output>>,
    key: super::EncodedKey,
    hash: KeyHash,
    effect: Effect,
    complete: Option<Completer<O::Output>>,
    mailbox: super::io_hub::OperationRoute,
    serial: Serial,
    version: CheckpointVersion,
    permit: Option<super::version_permit::VersionPermit>,
}
impl<S: Schema, O: UpsertOperation<S>> UpsertState<S, O> {
    fn advance(&mut self, engine: &Engine<S>) -> Result<Option<Outcome<O::Output>>, Error> {
        let resolved = engine.resolve_index(self.hash, &self.key)?;
        let entry = resolved.entry;
        let head = resolved.head;
        if self.prepared.is_none() {
            let request = self.request.as_mut().expect("Request not completed");
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
        let prepared = self.prepared.as_mut().expect("Already possessive value");
        let allocation = match engine.log.allocate_record(&self.key, head, prepared.plan) {
            Ok(allocation) => allocation,
            Err(Error::CapacityExceeded) if engine.storage.device.capabilities().supports_files => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let reservation =
            allocation.initialize(prepared.value.take().expect("Value not yet consumed"))?;
        let address = engine
            .log
            .finish_initialization(reservation.with_version(self.version))?;
        match engine
            .index
            .compare_publish(entry, IndexHead::Log(address))?
        {
            PublishResult::Published => {
                self.effect = Effect::Applied;
                let output = prepared.output.take().expect("Output not delivered yet");
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
    fn finalize(
        &mut self,
        engine: &Engine<S>,
        mut result: OperationResult<O::Output>,
    ) -> Option<OperationResult<O::Output>> {
        let request = self.request.take()?;
        if catch_unwind(AssertUnwindSafe(|| drop(self.prepared.take()))).is_err() {
            engine.failed.store(true, Ordering::SeqCst);
            let _ = catch_unwind(AssertUnwindSafe(|| drop(result)));
            result = Err(OperationError {
                cause: Error::InvalidState("Destruction panic of pending value"),
                effect: self.effect,
            });
        }
        Some(engine.finish_request(request, result, self.effect))
    }
    fn finish(&mut self, engine: &Engine<S>, result: OperationResult<O::Output>) {
        if let Some(result) = self.finalize(engine, result) {
            if let Some(complete) = &self.complete {
                engine.complete_tracked(
                    &mut self.monitor,
                    self.mailbox.registered_id(),
                    complete,
                    result,
                );
            } else {
                // There is no ticket yet for the first simultaneous promotion;Exception expansion can only end and fail to close,Can't pretend to be informed.
                engine.failed.store(true, Ordering::SeqCst);
                self.monitor
                    .finish(super::metrics::Completed::Failed(Effect::Unknown), None);
                let _ = catch_unwind(AssertUnwindSafe(|| drop(result)));
            }
        }
    }
    fn run_locked(&mut self, engine: &Engine<S>) -> UpsertStep<O::Output> {
        if engine.failed.load(Ordering::SeqCst) {
            return UpsertStep::Ready(Err(OperationError {
                cause: Error::InvalidState("engine_failed_closed"),
                effect: self.effect,
            }));
        }
        match engine
            .version_permits
            .ready_initial(self.hash, self.permit.as_ref())
        {
            Ok(true) => {}
            Ok(false) => return UpsertStep::Retry,
            Err(cause) => {
                return UpsertStep::Ready(Err(OperationError {
                    cause,
                    effect: self.effect,
                }));
            }
        }
        let result = match catch_unwind(AssertUnwindSafe(|| self.advance(engine))) {
            Ok(result) => result,
            Err(_) => {
                engine.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("Write callback panic"))
            }
        };
        if matches!(result, Ok(None)) {
            return UpsertStep::Retry;
        }
        if result.is_err() && self.effect == Effect::Unknown {
            engine.failed.store(true, Ordering::SeqCst);
        }
        UpsertStep::Ready(
            result
                .map(|value| value.expect("Waiting space excluded"))
                .map_err(|cause| OperationError {
                    cause,
                    effect: self.effect,
                }),
        )
    }
    fn shutdown(&mut self, engine: &Engine<S>) {
        if self.request.is_some() {
            self.finish(
                engine,
                Err(OperationError {
                    cause: Error::SessionAbandoned,
                    effect: self.effect,
                }),
            );
        }
        let _ = engine.io.release_operation(&mut self.mailbox);
    }
}
/// The initial call borrows the engine; ownership is acquired only for suspension.
struct InitialUpsert<'a, S: Schema, O: UpsertOperation<S>> {
    engine: &'a Engine<S>,
    state: Option<UpsertState<S, O>>,
}
impl<S: Schema, O: UpsertOperation<S>> Drop for InitialUpsert<'_, S, O> {
    fn drop(&mut self) {
        if let Some(state) = &mut self.state {
            state.shutdown(self.engine);
        }
    }
}
struct UpsertTask<S: Schema, O: UpsertOperation<S>> {
    engine: Arc<Engine<S>>,
    state: UpsertState<S, O>,
}
impl<S: Schema, O: UpsertOperation<S>> PendingTask for UpsertTask<S, O> {
    fn id(&self) -> RequestId {
        self.state.mailbox.id()
    }
    fn serial(&self) -> Serial {
        self.state.serial
    }
    fn version(&self) -> CheckpointVersion {
        self.state.version
    }
    fn on_io(&mut self, _: IoCompletion) -> Result<(), Error> {
        Err(Error::InvalidState("Upsert does not own device requests"))
    }
    fn step(&mut self, _: PollBudget) -> TaskStep {
        let state = &mut self.state;
        let engine = &self.engine;
        if state.request.is_none() {
            return TaskStep::Complete;
        }
        let _gate =
            match engine.operations[state.hash.0 as usize % engine.operations.len()].try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => return TaskStep::Retry,
                Err(_) => {
                    state.finish(
                        engine,
                        Err(OperationError {
                            cause: Error::InvalidState("Operation arbitration lock poisoning"),
                            effect: state.effect,
                        }),
                    );
                    return TaskStep::Complete;
                }
            };
        match state.run_locked(engine) {
            UpsertStep::Retry => TaskStep::Retry,
            UpsertStep::Ready(result) => {
                state.finish(engine, result);
                TaskStep::Complete
            }
        }
    }
    fn abandon(&mut self, error: OperationError) {
        self.state.finish(&self.engine, Err(error));
    }
}
impl<S: Schema, O: UpsertOperation<S>> Drop for UpsertTask<S, O> {
    fn drop(&mut self) {
        self.state.shutdown(&self.engine);
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
        let _gate = match super::operation_gate::try_lock(
            &self.operations[hash.0 as usize % self.operations.len()],
        ) {
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
        let mut mailbox = match self.io.reserve_operation(session.id) {
            Ok(mailbox) => mailbox,
            Err(reason) => return Err(Rejected { request, reason }),
        };
        let id = mailbox.id();
        let permit = match self
            .version_permits
            .reserve_initial(hash, session.current.version)
        {
            Ok(permit) => permit,
            Err(reason) => {
                let _ = self.io.release_operation(&mut mailbox);
                return Err(Rejected { request, reason });
            }
        };
        if let Err(reason) = self.admit(session, serial) {
            let _ = self.io.release_operation(&mut mailbox);
            return Err(Rejected { request, reason });
        }
        let mut initial = InitialUpsert {
            engine: self,
            state: Some(UpsertState {
                monitor: session.tracker.accept(super::metrics::Kind::Upsert),
                request: Some(request),
                prepared: None,
                key,
                hash,
                effect: Effect::NotApplied,
                complete: None,
                mailbox,
                serial,
                version: session.current.version,
                permit,
            }),
        };
        let task = initial.state.as_mut().expect("Initial state is present");
        match task.run_locked(self) {
            UpsertStep::Ready(result) => {
                let result = task
                    .finalize(self, result)
                    .expect("Synchronous requests are terminated only once");
                self.record_ready(&mut task.monitor, task.mailbox.registered_id(), &result);
                Ok(Submission::Ready(result))
            }
            UpsertStep::Retry => {
                if let Err(cause) = self
                    .version_permits
                    .activate(hash, task.version, &mut task.permit)
                    .and_then(|()| self.io.activate_operation(&mut task.mailbox))
                {
                    let result = task
                        .finalize(
                            self,
                            Err(OperationError {
                                cause,
                                effect: task.effect,
                            }),
                        )
                        .expect("Rejected mailbox registration is finalized");
                    self.record_ready(&mut task.monitor, task.mailbox.registered_id(), &result);
                    return Ok(Submission::Ready(result));
                }
                let (ticket, complete) = Ticket::pair_bounded(id, credit);
                task.complete = Some(complete);
                task.monitor.pending();
                let state = initial
                    .state
                    .take()
                    .expect("Suspended state transfers once");
                session.current.tasks.insert(
                    id.slot,
                    Box::new(UpsertTask {
                        engine: self.clone(),
                        state,
                    }),
                );
                Ok(Submission::Pending(ticket))
            }
        }
    }
}

#[cfg(test)]
mod tests;
