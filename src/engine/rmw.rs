//! RMW Check the chain head again after disk wait;Release old values and recalculate when there is insufficient space,Cannot override concurrent updates.
use super::{
    Engine, SessionRuntime,
    pending::TaskStep,
    task::{BorrowedTask, TaskWork},
};
use crate::{
    api::{
        Submission,
        completion::{Completer, OperationResult, Outcome, Ticket},
        operation::{RmwOperation, RmwOptions, UpdateDecision},
    },
    device::IoCompletion,
    index::{IndexHead, PublishResult},
    log::{
        ValueAccess,
        lookup::{LogLookup, LookupStep},
    },
    schema::{Schema, ValueLayout, ValueRead, ValueUpdate},
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};
struct RmwTask<S: Schema, O: RmwOperation<S>> {
    schema: std::marker::PhantomData<fn() -> S>,
    monitor: super::metrics::Monitor,
    request: Option<O>,
    lookup: Option<(crate::index::EntrySnapshot, LogLookup)>,
    options: RmwOptions,
    skip_in_place: bool,
    key: super::EncodedKey,
    hash: KeyHash,
    effect: Effect,
    complete: Option<Completer<O::Output>>,
    ready: Option<OperationResult<O::Output>>,
    mailbox: super::io_hub::OperationRoute,
    serial: Serial,
    version: CheckpointVersion,
    permit: super::version_permit::VersionPermit,
}
impl<S: Schema, O: RmwOperation<S>> RmwTask<S, O> {
    fn prepare_pending(&mut self, engine: &Arc<Engine<S>>) -> Result<bool, Error> {
        let awaiting_route = self
            .lookup
            .as_ref()
            .is_some_and(|(_, lookup)| lookup.awaiting_route());
        engine.io.activate_operation(&mut self.mailbox)?;
        if let Some((_, lookup)) = &mut self.lookup {
            lookup.enable_io();
        }
        Ok(awaiting_route)
    }
    fn run_initial(&mut self, engine: &Arc<Engine<S>>, budget: PollBudget) -> TaskStep {
        if matches!(self.run_locked(engine, budget), TaskStep::Complete) {
            return TaskStep::Complete;
        }
        match self.prepare_pending(engine) {
            // Resume only a lookup stopped before its first device submission.
            Ok(true) => self.run_locked(engine, budget),
            Ok(false) => TaskStep::Retry,
            Err(cause) => {
                self.finish(
                    engine,
                    Err(OperationError {
                        cause,
                        effect: self.effect,
                    }),
                );
                TaskStep::Complete
            }
        }
    }
    fn advance(
        &mut self,
        engine: &Arc<Engine<S>>,
        budget: PollBudget,
    ) -> Result<Option<Outcome<O::Output>>, Error> {
        if self.lookup.is_none() {
            let mut resolved = engine.resolve_index(self.hash, &self.key)?;
            if !resolved.entry.present {
                if matches!(
                    engine.index.reserve_empty(resolved.entry)?,
                    PublishResult::Conflict(_)
                ) {
                    return Ok(None);
                }
                resolved = engine.resolve_index(self.hash, &self.key)?;
            }
            let entry = resolved.entry;
            let mut lookup = engine.log.lookup_deferred(
                &engine.storage,
                self.key.clone(),
                resolved.head,
                super::io_hub::CompletionHub::route(self.mailbox.id()),
            )?;
            if self.mailbox.registered_id().is_some() {
                lookup.enable_io();
            }
            self.lookup = Some((entry, lookup));
        }
        let (snapshot, lookup) = self.lookup.as_mut().expect("query_created");
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
        let request = self
            .request
            .as_mut()
            .expect("The request has not yet been finalized");
        let (value, output) = match source {
            LookupStep::Present => {
                return Err(Error::InvalidState("Value query only returned metadata"));
            }
            LookupStep::Resident(lease) => {
                if !self.skip_in_place {
                    self.effect = Effect::Unknown;
                    match lease.update_at_version(self.version, |view| {
                        request.update_in_place(ValueUpdate { view })
                    })? {
                        ValueAccess::Ready(Some(UpdateDecision::Updated(output))) => {
                            self.effect = Effect::Applied;
                            return Ok(Some(Outcome::Success(output)));
                        }
                        ValueAccess::Ready(Some(UpdateDecision::Append) | None) => {
                            self.effect = Effect::NotApplied;
                            self.skip_in_place = true;
                        }
                        ValueAccess::Contended => {
                            self.effect = Effect::NotApplied;
                            return Ok(None);
                        }
                    }
                }
                match lease.try_read(|view| request.copy_update(ValueRead { view }))? {
                    ValueAccess::Ready(value) => value?,
                    ValueAccess::Contended => return Ok(None),
                }
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
            LookupStep::AwaitingIo | LookupStep::Continue => unreachable!("Wait has returned"),
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
                self.monitor.invalidate();
                Err(Error::Busy)
            }
        }
    }
    fn finish(&mut self, engine: &Arc<Engine<S>>, result: OperationResult<O::Output>) {
        if let Some(request) = self.request.take() {
            let result = engine.finish_request(request, result, self.effect);
            if let Some(complete) = &self.complete {
                engine.complete_tracked(
                    &mut self.monitor,
                    self.mailbox.registered_id(),
                    complete,
                    result,
                );
            } else {
                engine.record_ready(&mut self.monitor, self.mailbox.registered_id(), &result);
                self.ready = Some(result);
            }
        }
    }
    fn run_locked(&mut self, engine: &Arc<Engine<S>>, budget: PollBudget) -> TaskStep {
        if self.request.is_none() {
            return TaskStep::Complete;
        }
        if engine.failed.load(Ordering::SeqCst) {
            self.abandon(
                engine,
                OperationError {
                    cause: Error::InvalidState("engine_failed_closed"),
                    effect: self.effect,
                },
            );
            return TaskStep::Complete;
        }
        match self.permit.ready() {
            Ok(true) => {}
            Ok(false) => return TaskStep::Retry,
            Err(cause) => {
                self.abandon(
                    engine,
                    OperationError {
                        cause,
                        effect: self.effect,
                    },
                );
                return TaskStep::Complete;
            }
        }
        let result = match catch_unwind(AssertUnwindSafe(|| self.advance(engine, budget))) {
            Ok(result) => result,
            Err(_) => {
                engine.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("Write callback panic"))
            }
        };
        if matches!(result, Ok(None)) {
            return TaskStep::Retry;
        }
        if result.is_err() && self.effect == Effect::Unknown {
            engine.failed.store(true, Ordering::SeqCst);
        }
        self.finish(
            engine,
            result
                .map(|value| value.expect("Waiting space excluded"))
                .map_err(|cause| OperationError {
                    cause,
                    effect: self.effect,
                }),
        );
        TaskStep::Complete
    }
}
impl<S: Schema, O: RmwOperation<S>> TaskWork<S> for RmwTask<S, O> {
    fn id(&self) -> RequestId {
        self.mailbox.id()
    }
    fn serial(&self) -> Serial {
        self.serial
    }
    fn version(&self) -> CheckpointVersion {
        self.version
    }
    fn on_io(&mut self, engine: &Arc<Engine<S>>, completion: IoCompletion) -> Result<(), Error> {
        self.lookup
            .as_mut()
            .ok_or(Error::InvalidState("RMW No waiting for disk queries"))?
            .1
            .accept(&engine.storage, completion)
            .map_err(|rejected| rejected.reason)
    }
    fn step(&mut self, engine: &Arc<Engine<S>>, budget: PollBudget) -> TaskStep {
        let _gate =
            match engine.operations[self.hash.0 as usize % engine.operations.len()].try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => return TaskStep::Retry,
                Err(_) => {
                    self.abandon(
                        engine,
                        OperationError {
                            cause: Error::InvalidState("Operation arbitration lock poisoning"),
                            effect: self.effect,
                        },
                    );
                    return TaskStep::Complete;
                }
            };
        self.run_locked(engine, budget)
    }
    fn abandon(&mut self, engine: &Arc<Engine<S>>, error: OperationError) {
        self.finish(engine, Err(error));
    }
    fn cleanup(&mut self, engine: &Arc<Engine<S>>) {
        if self.request.is_some() {
            self.abandon(
                engine,
                OperationError {
                    cause: Error::SessionAbandoned,
                    effect: self.effect,
                },
            );
        }
        let _ = engine.io.release_operation(&mut self.mailbox);
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
        let permit = match self.version_permits.reserve(hash, session.current.version) {
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
        let mut task = BorrowedTask::new(
            self,
            RmwTask {
                monitor: self.metrics.accept(super::metrics::Kind::Rmw),
                schema: std::marker::PhantomData,
                request: Some(request),
                lookup: None,
                options,
                skip_in_place: false,
                key,
                hash,
                effect: Effect::NotApplied,
                complete: None,
                ready: None,
                mailbox,
                serial,
                version: session.current.version,
                permit,
            },
        );
        if matches!(
            task.run_initial(self, PollBudget::default()),
            TaskStep::Complete
        ) {
            Ok(Submission::Ready(
                task.ready
                    .take()
                    .expect("Synchronous RMW completed exactly once"),
            ))
        } else {
            let (ticket, complete) = Ticket::pair_bounded(id, credit);
            task.complete = Some(complete);
            task.monitor.pending();
            session
                .current
                .tasks
                .insert(id.slot, Box::new(task.into_owned()));
            Ok(Submission::Pending(ticket))
        }
    }
}
