//! Maintenance polling only advances devices and global barriers,Do not execute user callbacks for other sessions.
use super::Engine;
use crate::{coordination::Phase, schema::Schema, types::*};
use std::sync::atomic::Ordering;
impl<S: Schema> Engine<S> {
    pub(crate) fn poll_maintenance(&self, budget: PollBudget) -> Result<Progress, Error> {
        let _timer = self.metrics.timer(true);
        let result = self.maintenance_step(budget);
        if result.is_err() {
            self.failed.store(true, Ordering::SeqCst);
            self.fail_checkpoint()?;
            self.fail_growth()?;
            self.fail_compaction()?;
            self.fail_gc()?;
            self.fail_checkpoint_release()?;
            let state = self.coordinator.snapshot()?;
            if let Some(id) = state.id
                && state.phase != Phase::Failed
            {
                self.coordinator
                    .fail_action(id, Error::InvalidState("Maintenance promotion failed"))?;
            }
        }
        result
    }
    fn maintenance_step(&self, budget: PollBudget) -> Result<Progress, Error> {
        if self.failed.load(Ordering::SeqCst) || self.coordinator.snapshot()?.phase == Phase::Failed
        {
            return Err(Error::InvalidState(
                "Engine or maintenance action has failed",
            ));
        }
        self.io.poll(&*self.storage.device, budget)?;
        self.progress_scans()?;
        let mut advanced = self.progress_storage()?;
        let mut completed = 0;
        for _ in 0..budget.0.get() {
            let (checkpoint_progress, checkpoint_completed) = self.progress_checkpoint()?;
            let (growth_progress, growth_completed) = self.progress_growth()?;
            let (compaction_progress, compaction_completed) = self.progress_compaction()?;
            let (gc_progress, gc_completed) = self.progress_gc()?;
            let (release_progress, release_completed) = self.progress_checkpoint_release()?;
            advanced |= release_progress;
            completed += usize::from(release_completed);
            advanced |= gc_progress;
            completed += usize::from(gc_completed);
            advanced |= compaction_progress;
            completed += usize::from(compaction_completed);
            advanced |= growth_progress;
            completed += usize::from(growth_completed);
            advanced |= checkpoint_progress;
            completed += usize::from(checkpoint_completed);
            let state = self.coordinator.snapshot()?;
            let Some(id) = state.id else {
                break;
            };
            // Only the participant barrier is crossed here;index snapshot,Brushing and publishing must be done by actual material drivers.
            if !matches!(
                state.phase,
                Phase::PrepareIndex
                    | Phase::Prepare
                    | Phase::InProgress
                    | Phase::WaitPending
                    | Phase::GrowPrepare
            ) {
                break;
            }
            match self.coordinator.advance(id, state.phase) {
                Ok(_) => advanced = true,
                Err(Error::Busy) => break,
                Err(error) => {
                    if self.coordinator.snapshot()? != state {
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        let state = self.coordinator.snapshot()?;
        if state.phase == Phase::Failed {
            return Err(Error::InvalidState("Maintenance action failed"));
        }
        Ok(Progress {
            completed,
            remaining: usize::from(state.id.is_some() || self.compaction_pending()?),
            phase_advanced: advanced,
        })
    }
}
