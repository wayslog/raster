//! The read context is held by the session;Disk waits separated from result slots,The callback is only executed on the advancing thread.
use super::{
    Engine, SessionRuntime,
    io_hub::CompletionHub,
    pending::{PendingTask, TaskStep},
};
use crate::{
    api::{
        Submission,
        completion::{AbortReason, Completer, OperationResult, Outcome, Ticket},
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
    monitor: super::metrics::Monitor,
    request: Option<O>,
    lookup: Option<LogLookup>,
    observed: Option<crate::index::EntrySnapshot>,
    key: super::EncodedKey,
    hash: KeyHash,
    options: ReadOptions,
    complete: Option<Completer<O::Output>>,
    ready: Option<OperationResult<O::Output>>,
    id: RequestId,
    serial: Serial,
    version: CheckpointVersion,
    permit: super::version_permit::VersionPermit,
}
impl<S: Schema, O: ReadOperation<S>> ReadTask<S, O> {
    fn finish(&mut self, result: OperationResult<O::Output>) {
        if let Some(request) = self.request.take() {
            let result = self
                .engine
                .finish_request(request, result, Effect::NotApplied);
            if let Some(complete) = &self.complete {
                self.engine
                    .complete_tracked(&mut self.monitor, self.id, complete, result);
            } else {
                self.engine
                    .record_ready(&mut self.monitor, self.id, &result);
                self.ready = Some(result);
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
            .as_mut()
            .ok_or(Error::InvalidState("Read without waiting for disk query"))?
            .accept(&self.engine.storage, completion)
            .map_err(|rejected| rejected.reason)
    }
    fn step(&mut self, budget: PollBudget) -> TaskStep {
        if self.request.is_none() {
            return TaskStep::Complete;
        }
        if self.engine.failed.load(Ordering::SeqCst) {
            self.abandon(OperationError {
                cause: Error::InvalidState("engine_failed_closed"),
                effect: Effect::NotApplied,
            });
            return TaskStep::Complete;
        }
        match self.permit.ready() {
            Ok(true) => {}
            Ok(false) => return TaskStep::Retry,
            Err(cause) => {
                self.abandon(OperationError {
                    cause,
                    effect: Effect::NotApplied,
                });
                return TaskStep::Complete;
            }
        }
        let result = catch_unwind(AssertUnwindSafe(
            || -> Result<Option<Outcome<O::Output>>, Error> {
                if self.lookup.is_none() {
                    let resolved = self.engine.resolve_index(self.hash, &self.key)?;
                    if self.engine.config.cache.enabled {
                        self.engine
                            .metrics
                            .cache(super::metrics::CacheEvent::Lookup);
                    }
                    if let Some(record) = resolved.cached {
                        if record.source < self.engine.log.frontiers()?.begin {
                            // GC Cache header replaced;Reparse index,Live keys after migration cannot be reported as truncated.
                            return Ok(None);
                        }
                        let encoded = crate::format::Record::decode(record.encoded())?;
                        if encoded.header.version != record.version {
                            return Err(Error::InvalidState("Cache record version mismatch"));
                        }
                        self.engine.metrics.cache(super::metrics::CacheEvent::Hit);
                        if let crate::index::IndexHead::Cache(address) = resolved.entry.head {
                            self.engine.cache.touch(address)?;
                        }
                        let value = self.engine.log.decode_temporary(encoded.value)?;
                        return value
                            .read(|view| {
                                self.request
                                    .as_mut()
                                    .expect("The request has not yet been finalized")
                                    .read(ValueRead { view })
                            })?
                            .map(|value| Some(Outcome::Success(value)));
                    }
                    self.observed = Some(resolved.entry);
                    self.lookup = Some(self.engine.log.lookup(
                        &self.engine.storage,
                        self.key.clone(),
                        resolved.head,
                        CompletionHub::route(self.id),
                    )?);
                }
                let request = self
                    .request
                    .as_mut()
                    .expect("The request has not yet been finalized");
                match self.lookup.as_mut().expect("query_created").step(
                    &self.engine.log,
                    &self.engine.storage,
                    budget,
                )? {
                    LookupStep::Continue | LookupStep::AwaitingIo => Ok(None),
                    LookupStep::Present => {
                        Err(Error::InvalidState("Value query only returned metadata"))
                    }
                    LookupStep::Missing => {
                        // Compaction may have moved the chain head and pushed forward while the cold read was waiting begin;Missing old link does not mean missing key.
                        if self.observed != Some(self.engine.index.prepare(self.hash)?) {
                            self.lookup = None;
                            self.observed = None;
                            Ok(None)
                        } else {
                            Ok(Some(Outcome::NotFound))
                        }
                    }
                    LookupStep::Tombstone => Ok(Some(if self.options.abort_if_tombstone {
                        Outcome::Aborted(AbortReason::Tombstone)
                    } else {
                        Outcome::NotFound
                    })),
                    LookupStep::Resident(value) => {
                        match value.try_read_live(|view| request.read(ValueRead { view }))? {
                            crate::log::ValueAccess::Ready(Some(value)) => {
                                value.map(|value| Some(Outcome::Success(value)))
                            }
                            crate::log::ValueAccess::Ready(None) => {
                                Ok(Some(if self.options.abort_if_tombstone {
                                    Outcome::Aborted(AbortReason::Tombstone)
                                } else {
                                    Outcome::NotFound
                                }))
                            }
                            crate::log::ValueAccess::Contended => {
                                self.lookup = None;
                                self.observed = None;
                                Ok(None)
                            }
                        }
                    }
                    LookupStep::Decoded(value) => {
                        self.engine.populate_cache(
                            self.hash,
                            self.observed.expect("Index snapshot saved"),
                            self.lookup.as_ref().expect("query_created"),
                        )?;
                        value
                            .read(|view| request.read(ValueRead { view }))?
                            .map(|value| Some(Outcome::Success(value)))
                    }
                }
            },
        ));
        match result {
            Ok(Ok(None)) => TaskStep::Retry,
            Ok(result) => {
                self.finish(
                    result
                        .map(|value| value.expect("Incomplete results excluded"))
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
                    cause: Error::InvalidState("Read callback panic"),
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
        let mut task = ReadTask {
            monitor: self.metrics.accept(super::metrics::Kind::Read),
            engine: self.clone(),
            request: Some(request),
            lookup: None,
            observed: None,
            key,
            hash,
            options,
            complete: None,
            ready: None,
            id,
            serial,
            version: session.current.version,
            permit,
        };
        if matches!(task.step(PollBudget::default()), TaskStep::Complete) {
            Ok(Submission::Ready(
                task.ready
                    .take()
                    .expect("Synchronous read completed exactly once"),
            ))
        } else {
            // Reserve the result credit before admission, but allocate a ticket
            // only when completion must outlive this call.
            let (ticket, complete) = Ticket::pair_bounded(id, credit);
            task.complete = Some(complete);
            task.monitor.pending();
            session.current.tasks.insert(id.slot, Box::new(task));
            Ok(Submission::Pending(ticket))
        }
    }
}
