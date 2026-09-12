//! Online expansion to participate in global actions;Migration does not require cross-business callbacks,Completion report awaits safe release of old table.
use super::Engine;
use crate::{
    api::maintenance::{IndexGrowthReport, MaintenanceCompleter, MaintenanceTicket},
    coordination::{Action, Phase},
    epoch::DeferredAction,
    index::growth::GrowthProgress,
    schema::Schema,
    types::*,
};
use std::sync::{TryLockError, atomic::Ordering};
#[derive(Default)]
pub(crate) struct GrowthRuntime {
    job: Option<Job>,
}
struct Job {
    id: MaintenanceId,
    complete: MaintenanceCompleter<IndexGrowthReport>,
    progress: Option<GrowthProgress>,
    reclaimed: bool,
    failed: bool,
    finished: bool,
}
impl Job {
    fn step<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let state = engine.coordinator.snapshot()?;
        if state.id != Some(self.id) || state.phase == Phase::Failed {
            return Err(Error::InvalidState(
                "The expansion operation has expired or failed.",
            ));
        }
        match state.phase {
            Phase::GrowPrepare => Ok(false),
            Phase::GrowCopy => {
                let mut advanced = false;
                if self.progress.is_none() {
                    self.progress = Some(engine.index.begin_growth()?);
                    advanced = true;
                }
                // Request to obtain one of the locks in the same direction;Use all here try_lock,Never wait for user callback.
                let mut gates = Vec::new();
                gates
                    .try_reserve_exact(engine.operations.len())
                    .map_err(|_| Error::OutOfMemory)?;
                for gate in &engine.operations {
                    match gate.try_lock() {
                        Ok(guard) => gates.push(guard),
                        Err(TryLockError::WouldBlock) => return Ok(advanced),
                        Err(_) => {
                            return Err(Error::InvalidState(
                                "Capacity expansion encounters business arbitration lock poisoning",
                            ));
                        }
                    }
                }
                let progress = engine.cache.with_normalized_index(&engine.index, || {
                    engine.index.grow_step(PollBudget(
                        std::num::NonZeroUsize::new(1).expect("fixed budget"),
                    ))
                })?;
                self.progress = Some(progress);
                if progress.complete {
                    engine.epoch.defer(DeferredAction::ReleaseIndex(Generation(
                        progress.generation.0 - 1,
                    )))?;
                    engine.epoch.advance()?;
                    engine.coordinator.advance(self.id, Phase::GrowCopy)?;
                }
                Ok(true)
            }
            Phase::Publish => {
                let progress = self.progress.ok_or(Error::InvalidState(
                    "Expansion is missing migration results",
                ))?;
                for action in engine.epoch.collect()? {
                    match action {
                        DeferredAction::ReleaseIndex(generation)
                            if generation.0 + 1 == progress.generation.0 =>
                        {
                            engine.index.release_retired(generation)?;
                            self.reclaimed = true;
                        }
                        _ => {
                            return Err(Error::InvalidState(
                                "Expansion received unsupported epoch release action",
                            ));
                        }
                    }
                }
                if !self.reclaimed {
                    return Ok(false);
                }
                engine.coordinator.finish_action(self.id)?;
                self.complete.finish(Ok(IndexGrowthReport {
                    old_buckets: progress.old_buckets,
                    new_buckets: progress.new_buckets,
                    generation: progress.generation,
                }))?;
                self.finished = true;
                Ok(true)
            }
            _ => Err(Error::InvalidState(
                "The expansion operation is at the wrong stage",
            )),
        }
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn start_growth(&self) -> Result<MaintenanceTicket<IndexGrowthReport>, Error> {
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("storage_closed_or_failed"));
        }
        let mut runtime = self.growth.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Poisoned(_) => Error::InvalidState("Expansion task lock poisoning"),
        })?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        let id = self.coordinator.start_action(Action::GrowIndex)?;
        let (ticket, complete) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            complete,
            progress: None,
            reclaimed: false,
            failed: false,
            finished: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_growth(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.growth.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("Expansion task lock poisoning")),
        };
        let Some(job) = &mut runtime.job else {
            return Ok((false, false));
        };
        if job.failed {
            return Ok((false, false));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self)))
            .unwrap_or(Err(Error::InvalidState("Expansion promotes panic")));
        match result {
            Err(error) => {
                job.complete.finish(Err(error))?;
                job.failed = true;
                Err(Error::InvalidState(
                    "Expansion failed,see_maintenance_report",
                ))
            }
            Ok(advanced) => {
                let finished = job.finished;
                if finished {
                    runtime.job = None;
                }
                Ok((advanced, finished))
            }
        }
    }
    pub(crate) fn fail_growth(&self) -> Result<(), Error> {
        let mut runtime = match self.growth.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("Expansion task lock poisoning")),
        };
        if let Some(job) = &mut runtime.job
            && !job.failed
            && !job.finished
        {
            job.complete.finish(Err(Error::InvalidState(
                "Engine or coordination action failed,Expansion terminated",
            )))?;
            job.failed = true;
        }
        Ok(())
    }
}
