//! Single storage compression:Physical scan selection candidate,Conditional copy re-verification;Ordinary compression does not advance begin.
use super::{
    Engine,
    conditional_copy::{ConditionalCopy, CopyResult},
};
use crate::{
    api::maintenance::{
        CheckpointReport, CompactionAlgorithm, CompactionOptions, CompactionReport, GcReport,
        MaintenanceCompleter, MaintenanceTicket,
    },
    coordination::{Action, Phase},
    format::Record,
    maintenance::scan::{Scan, ScanStep},
    schema::Schema,
    types::*,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, TryLockError, atomic::Ordering},
};

#[path = "compaction_follow_up.rs"]
mod follow_up;
use follow_up::Stage;
#[path = "compaction_workers.rs"]
mod workers;
use workers::Workers;

#[derive(Default)]
pub(crate) struct CompactionRuntime {
    job: Option<Job>,
}
struct Job {
    id: MaintenanceId,
    options: CompactionOptions,
    stage: Stage,
    checkpoint: Option<CheckpointReport>,
    gc: Option<GcReport>,
    scan: Scan,
    scanned: bool,
    candidates: BTreeMap<Vec<u8>, LogAddress>,
    key_bytes: usize,
    copying: Option<ConditionalCopy>,
    workers: Option<Workers>,
    copied: u64,
    failure: Option<Error>,
    complete: MaintenanceCompleter<CompactionReport>,
    reported: bool,
}
impl Job {
    fn step<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        self.collect_workers(engine)?;
        if !matches!(self.stage, Stage::Copy) {
            return self.follow_up(engine);
        }
        if self.failure.is_some() {
            if let Some(workers) = &self.workers {
                workers.abort();
            }
            if !self.collect_workers(engine)? {
                return Ok(false);
            }
            if let Some(copy) = &mut self.copying
                && !copy.drain(&engine.storage)?
            {
                return Ok(false);
            }
            if !self.scan.drain(&engine.storage)? {
                return Ok(false);
            }
            return self.finish(engine);
        }
        if let Some(workers) = &mut self.workers
            && let Some(copy) = self.copying.take()
        {
            return match workers.submit(copy) {
                Ok(()) => Ok(true),
                Err(rejected) => {
                    self.copying = Some(rejected.request);
                    if matches!(rejected.reason, Error::Busy) {
                        Ok(false)
                    } else {
                        Err(rejected.reason)
                    }
                }
            };
        }
        if let Some(copy) = &mut self.copying {
            // Verify count before possible release,Error reports cannot miss migrations that have taken effect.
            let next = self.copied.checked_add(1).ok_or(Error::CapacityExceeded)?;
            match engine.conditional_copy(copy, PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            {
                Ok(CopyResult::Copied(_)) => {
                    self.copied = next;
                    self.copying = None;
                }
                Ok(CopyResult::Obsolete) => {
                    self.copying = None;
                }
                Ok(CopyResult::Retry) => return Ok(false),
                Err(error) => {
                    if copy.published_address().is_some() {
                        self.copied = next;
                    }
                    return Err(error);
                }
            }
            return Ok(true);
        }
        if self.scanned {
            if let Some((key, source)) = self.candidates.pop_first() {
                self.key_bytes -= key.len();
                self.copying = Some(engine.new_conditional_copy(self.id, source, key)?);
                return Ok(true);
            }
            if let Some(workers) = &self.workers {
                workers.close();
            }
            if !self.collect_workers(engine)? {
                return Ok(false);
            }
            return self.finish(engine);
        }
        match self.scan.step(engine)? {
            ScanStep::Pending => Ok(false),
            ScanStep::End => {
                self.scanned = true;
                Ok(true)
            }
            ScanStep::Record(address, bytes) => {
                let record = Record::decode(&bytes)?;
                if record.header.invalid {
                    return Ok(true);
                }
                if record.header.version > engine.coordinator.snapshot()?.version {
                    return Err(Error::InvalidFormat(
                        "Compression scan record version exceeds current version",
                    ));
                }
                let mut key = Vec::new();
                key.try_reserve_exact(record.key.len())
                    .map_err(|_| Error::OutOfMemory)?;
                key.extend_from_slice(record.key);
                match self.options.algorithm {
                    CompactionAlgorithm::Lookup => {
                        self.copying = Some(engine.new_conditional_copy(self.id, address, key)?);
                    }
                    CompactionAlgorithm::ScanDedup => {
                        if let Some(previous) = self.candidates.get_mut(&key) {
                            *previous = address;
                        } else {
                            let bytes = self
                                .key_bytes
                                .checked_add(key.len())
                                .ok_or(Error::CapacityExceeded)?;
                            if self.candidates.len()
                                >= engine.config.maintenance.max_compaction_keys
                                || bytes > engine.config.maintenance.max_compaction_key_bytes
                            {
                                return Err(Error::CapacityExceeded);
                            }
                            self.candidates.insert(key, address);
                            self.key_bytes = bytes;
                        }
                    }
                }
                Ok(true)
            }
        }
    }
    fn collect_workers<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        if let Some(workers) = &mut self.workers {
            let progress = workers.poll(engine)?;
            self.copied = progress.copied;
            if let Some(error) = progress.failure {
                self.fail(error);
            }
            Ok(progress.finished)
        } else {
            Ok(true)
        }
    }
    fn finish<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let state = engine.coordinator.snapshot()?;
        if state.phase != Phase::Failed {
            engine.coordinator.advance(self.id, Phase::Compacting)?;
            engine.coordinator.finish_action(self.id)?;
        }
        // End the copy action first,Then strive for follow-up actions one by one;The ticket still represents the entire composite task.
        self.stage = if self.failure.is_some() {
            Stage::Report
        } else if self.options.checkpoint {
            Stage::StartCheckpoint
        } else if self.options.shift_begin {
            Stage::StartGc
        } else {
            Stage::Report
        };
        if matches!(self.stage, Stage::Report) {
            self.publish_report()?;
        }
        Ok(true)
    }
    fn error(&mut self, cause: Error) -> Error {
        Error::CompactionFailed {
            until: self.options.until,
            copied: self.copied,
            checkpoint: self.checkpoint.take().map(Box::new),
            gc: self.gc.take().map(Box::new),
            cause: Box::new(cause),
        }
    }
    fn publish_report(&mut self) -> Result<(), Error> {
        let result = if let Some(cause) = self.failure.take() {
            Err(self.error(cause))
        } else {
            Ok(CompactionReport {
                until: self.options.until,
                copied: self.copied,
                gc: self.gc.take(),
                checkpoint: self.checkpoint.take(),
            })
        };
        self.complete.finish(result)?;
        self.reported = true;
        Ok(())
    }
    fn fail(&mut self, cause: Error) {
        if self.failure.is_none() {
            self.failure = Some(cause);
        }
    }
    fn report_failed<S: Schema>(&mut self, engine: &Engine<S>, cause: Error) -> Result<(), Error> {
        if !self.reported {
            if let Some(workers) = &self.workers {
                workers.abort();
            }
            if !self.collect_workers(engine)? {
                return Ok(());
            }
            let child = match self.stage {
                Stage::Checkpoint(_) => {
                    engine.fail_checkpoint()?;
                    true
                }
                Stage::Gc(_) => {
                    engine.fail_gc()?;
                    true
                }
                _ => false,
            };
            if child && !self.collect_child()? {
                // Another driver is still publishing sub-results,You can't throw away part of the effect with a generic error first.
                return Ok(());
            }
            self.fail(cause);
            self.publish_report()?;
        }
        Ok(())
    }
}
impl CompactionRuntime {
    pub(crate) fn is_active(&self) -> bool {
        self.job.as_ref().is_some_and(|job| !job.reported)
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn compaction_pending(&self) -> Result<bool, Error> {
        match self.compaction.try_lock() {
            Ok(runtime) => Ok(runtime.is_active()),
            Err(TryLockError::WouldBlock) => Ok(true),
            Err(_) => Err(Error::InvalidState("compaction_task_lock_poisoned")),
        }
    }
    pub(crate) fn start_compaction(
        self: &Arc<Self>,
        options: CompactionOptions,
    ) -> Result<MaintenanceTicket<CompactionReport>, Error> {
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("storage_closed_or_failed"));
        }
        if options.workers == 0 {
            return Err(Error::InvalidConfig {
                field: "compaction.workers",
                reason: "The number of compression worker threads must be non-zero",
            });
        }
        if options.workers > self.config.maintenance.max_compaction_workers {
            return Err(Error::InvalidConfig {
                field: "compaction.workers",
                reason: "Configured compression thread budget exceeded",
            });
        }
        if options.checkpoint {
            self.checkpoint_capabilities()?;
        }
        let caps = self.storage.device.capabilities();
        if options.shift_begin && caps.supports_files && !caps.supports_directory_sync {
            return Err(Error::UnsupportedDurability);
        }
        let mut runtime = self.compaction.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Poisoned(_) => Error::InvalidState("compaction_task_lock_poisoned"),
        })?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        let begin = self.log.frontiers()?.begin;
        let scan = Scan::new(self, begin, options.until)?;
        if self.coordinator.snapshot()?.id.is_some() {
            return Err(Error::Busy);
        }
        let workers = if options.workers > 1 {
            Some(Workers::new(self, options.workers)?)
        } else {
            None
        };
        let id = self.coordinator.start_action(Action::Compact)?;
        let (ticket, complete) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            options,
            stage: Stage::Copy,
            checkpoint: None,
            gc: None,
            scan,
            scanned: false,
            candidates: BTreeMap::new(),
            key_bytes: 0,
            copying: None,
            workers,
            copied: 0,
            failure: None,
            complete,
            reported: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_compaction(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.compaction.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("compaction_task_lock_poisoned")),
        };
        let Some(job) = &mut runtime.job else {
            return Ok((false, false));
        };
        if job.reported {
            return Ok((false, false));
        }
        let draining = job.failure.is_some();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self)));
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                self.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("Compression scan or layout panic"))
            }
        };
        let advanced = match result {
            Ok(advanced) => advanced,
            Err(error) => {
                if draining || matches!(error, Error::InvalidState(_)) {
                    self.failed.store(true, Ordering::SeqCst);
                }
                job.fail(error);
                false
            }
        };
        // Ordinary failure means emptying first and then terminating the action.;Panic reported by unity failure shutdown path termination and waiting for shutdown to return device resources.
        if self.failed.load(Ordering::SeqCst) {
            job.report_failed(
                self,
                Error::InvalidState("Engine failed to shut down during compression"),
            )?;
            return Err(Error::InvalidState(
                "Compression failed to close,see_maintenance_report",
            ));
        }
        let finished = job.reported;
        if finished {
            runtime.job = None;
        }
        Ok((advanced, finished))
    }
    pub(crate) fn fail_compaction(&self) -> Result<(), Error> {
        let mut runtime = match self.compaction.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("compaction_task_lock_poisoned")),
        };
        if let Some(job) = &mut runtime.job {
            job.report_failed(
                self,
                Error::InvalidState("Engine or global action failed,compression terminated"),
            )?;
        }
        Ok(())
    }
    pub(crate) fn compaction_report_pending(&self, id: MaintenanceId) -> Result<bool, Error> {
        match self.compaction.try_lock() {
            Ok(runtime) => Ok(runtime
                .job
                .as_ref()
                .is_some_and(|job| job.id == id && !job.reported)),
            Err(TryLockError::WouldBlock) => Ok(true),
            Err(_) => Err(Error::InvalidState("compaction_task_lock_poisoned")),
        }
    }
    /// Failure to close first causes the worker thread to stop all publishing,Then shut down the device and reclaim the retention slot.
    pub(crate) fn stop_compaction_workers(&self, deadline: Deadline) -> Result<(), Error> {
        loop {
            let mut runtime = match self.compaction.try_lock() {
                Ok(runtime) => runtime,
                Err(TryLockError::WouldBlock) => {
                    if deadline.expired() {
                        return Err(Error::DeadlineExceeded);
                    }
                    std::thread::yield_now();
                    continue;
                }
                Err(_) => return Err(Error::InvalidState("compaction_task_lock_poisoned")),
            };
            let Some(job) = &mut runtime.job else {
                return Ok(());
            };
            if let Some(workers) = &job.workers {
                workers.abort();
            }
            if job.collect_workers(self)? {
                return Ok(());
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            drop(runtime);
            std::thread::yield_now();
        }
    }
    /// Only clean up exception tasks after the device is shut down and all requests have been returned,The lease on the en route cannot be released early.
    pub(crate) fn release_stopped_compaction(&self) -> Result<(), Error> {
        self.fail_compaction()?;
        self.compaction
            .lock()
            .map_err(|_| Error::InvalidState("compaction_task_lock_poisoned"))?
            .job = None;
        Ok(())
    }
}
