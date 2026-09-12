//! Maintain separation of acceptance and completion;Index-only checkpoint does not declare session persistence successful.
use crate::{engine::Engine, schema::Schema, types::*};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
pub enum CheckpointKind {
    Full,
    Index,
    Log,
}
#[derive(Clone, Debug)]
pub struct RecoverySet {
    pub store: StoreId,
    pub index: CheckpointToken,
    pub log: CheckpointToken,
}
#[derive(Clone, Debug)]
pub struct DurableProgress {
    pub session: SessionId,
    pub serial: Serial,
    pub version: CheckpointVersion,
}
#[derive(Clone, Debug)]
pub struct CheckpointReport {
    pub kind: CheckpointKind,
    pub token: CheckpointToken,
    pub version: CheckpointVersion,
    pub begin: LogAddress,
    pub end: LogAddress,
    pub sessions: Vec<DurableProgress>,
}
#[derive(Clone, Debug)]
pub struct RecoveryReport {
    pub set: RecoverySet,
    pub version: CheckpointVersion,
    pub sessions: Vec<DurableProgress>,
}
#[derive(Clone, Copy, Debug)]
pub enum CompactionAlgorithm {
    ScanDedup,
    Lookup,
}
#[derive(Clone, Debug)]
pub struct CompactionOptions {
    pub algorithm: CompactionAlgorithm,
    pub until: LogAddress,
    pub workers: usize,
    pub shift_begin: bool,
    pub checkpoint: bool,
}
#[derive(Clone, Debug)]
pub enum PhysicalReclamation {
    Completed,
    /// Old page leases or in-flight reads still require the old range;This action has ended,You can press the same button later begin Try again.
    DeferredByRuntime {
        begin: LogAddress,
        end: LogAddress,
    },
    DeferredByRecoverySet {
        blockers: Vec<RecoverySet>,
        begin: LogAddress,
        end: LogAddress,
    },
}
#[derive(Clone, Debug)]
pub struct GcReport {
    pub begin: LogAddress,
    pub index_cleaned: bool,
    /// The number of segment deletions confirmed by directory synchronization this time,Contains continuation of previously failed deletions.
    pub deleted_segments: u64,
    pub physical: PhysicalReclamation,
}
#[derive(Clone, Debug)]
pub struct CheckpointReleaseReport {
    pub token: CheckpointToken,
    pub retirement: CheckpointRetirement,
    /// The number of materials confirmed not to exist by directory synchronization this time(Contains items that were missing before retrying),Not the number of new additions and deletions.
    pub confirmed_absent_materials: u64,
    pub physical: PhysicalReclamation,
}
#[derive(Clone, Debug)]
pub struct CompactionReport {
    pub until: LogAddress,
    pub copied: u64,
    pub gc: Option<GcReport>,
    pub checkpoint: Option<CheckpointReport>,
}
/// Stopped/Failed Only reported after the dispatch thread has collected;The failed resource is still represented by shutdown return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutoCompactionPhase {
    Disabled,
    Idle,
    Scheduled,
    Compacting,
    Reclaiming,
    Stopping,
    Stopped,
    Failed,
}
/// The bounded state only saves the latest compression and subsequent physical recovery reports;Holding old snapshots will not be overwritten.
#[derive(Clone, Debug)]
pub struct AutoCompactionStatus {
    pub phase: AutoCompactionPhase,
    pub active: Option<MaintenanceId>,
    pub completed_compactions: u64,
    pub last_compaction: Option<SharedReport<CompactionReport>>,
    pub last_reclamation: Option<SharedReport<GcReport>>,
    pub failure: Option<Arc<Error>>,
    pub log_bytes: u64,
    pub budget_reached: bool,
}
impl AutoCompactionStatus {
    /// Instantaneous idleness does not guarantee that it will not be scheduled again in the future.;Request to stop first and then wait to confirm thread exit.
    pub fn is_quiescent(&self) -> bool {
        matches!(
            self.phase,
            AutoCompactionPhase::Disabled
                | AutoCompactionPhase::Idle
                | AutoCompactionPhase::Stopped
                | AutoCompactionPhase::Failed
        )
    }
}
#[derive(Clone, Debug)]
pub struct IndexGrowthReport {
    pub old_buckets: usize,
    pub new_buckets: usize,
    pub generation: Generation,
}

pub type SharedReport<R> = Arc<Result<R, Error>>;
enum ReportState<R> {
    Pending,
    Ready(SharedReport<R>),
    Taken,
}
pub struct MaintenanceTicket<R> {
    pub(crate) store: StoreId,
    pub(crate) id: MaintenanceId,
    result: Arc<Mutex<ReportState<R>>>,
}
impl<R> MaintenanceTicket<R> {
    pub(crate) fn pair(store: StoreId, id: MaintenanceId) -> (Self, MaintenanceCompleter<R>) {
        let result = Arc::new(Mutex::new(ReportState::Pending));
        (
            Self {
                store,
                id,
                result: result.clone(),
            },
            MaintenanceCompleter { result },
        )
    }

    pub fn id(&self) -> MaintenanceId {
        self.id
    }
    pub fn try_report(&self) -> Result<Option<SharedReport<R>>, Error> {
        let slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("Maintenance reports that the lock is poisoned"))?;
        match &*slot {
            ReportState::Pending => Ok(None),
            ReportState::Ready(report) => Ok(Some(report.clone())),
            ReportState::Taken => Err(Error::InvalidState(
                "Internal maintenance results have been removed",
            )),
        }
    }
    /// Only used by subtask tickets that are not published externally;Move original error,Reserve OS Causes and partial effects.
    pub(crate) fn take_owned_report(&mut self) -> Result<Option<Result<R, Error>>, Error> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("Maintenance reports that the lock is poisoned"))?;
        match std::mem::replace(&mut *slot, ReportState::Taken) {
            ReportState::Pending => {
                *slot = ReportState::Pending;
                Ok(None)
            }
            ReportState::Ready(report) => match Arc::try_unwrap(report) {
                Ok(report) => Ok(Some(report)),
                Err(report) => {
                    // The completion end may hold a copy when it just publishes the result.,Advance later,No blocking wait.
                    *slot = ReportState::Ready(report);
                    Ok(None)
                }
            },
            ReportState::Taken => Err(Error::InvalidState(
                "Repeated removal of internal maintenance results",
            )),
        }
    }
}
/// The action holds the only completion end,Once a report is set up, it cannot be replaced.
pub(crate) struct MaintenanceCompleter<R> {
    result: Arc<Mutex<ReportState<R>>>,
}
impl<R> MaintenanceCompleter<R> {
    pub fn finish(&self, report: Result<R, Error>) -> Result<SharedReport<R>, Error> {
        let mut slot = self
            .result
            .lock()
            .map_err(|_| Error::InvalidState("Maintenance reports that the lock is poisoned"))?;
        if !matches!(*slot, ReportState::Pending) {
            return Err(Error::InvalidState("Maintenance report has ended"));
        }
        let report = Arc::new(report);
        *slot = ReportState::Ready(report.clone());
        Ok(report)
    }
}
impl<R> Drop for MaintenanceCompleter<R> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.result.lock()
            && matches!(*slot, ReportState::Pending)
        {
            *slot = ReportState::Ready(Arc::new(Err(Error::InvalidState(
                "Maintenance tasks are abandoned before they are completed",
            ))));
        }
    }
}
pub struct Maintenance<S: Schema> {
    pub(crate) inner: Arc<Engine<S>>,
}
impl<S: Schema> Maintenance<S> {
    pub fn auto_compaction_status(&self) -> Result<AutoCompactionStatus, Error> {
        self.inner.auto_compaction_status()
    }
    /// Idempotent requests stop;Tasks that have been accepted will continue to be emptied.,Ordinary errors do not automatically retry.
    pub fn stop_auto_compaction(&self) -> Result<(), Error> {
        self.inner.auto_compaction.request_stop()
    }
    /// Wait for the current automatic maintenance idle or scheduling thread to end;When there are active sessions, each session must continue to advance..
    pub fn wait_auto_compaction(&self, deadline: Deadline) -> Result<AutoCompactionStatus, Error> {
        loop {
            let status = self.auto_compaction_status()?;
            if status.is_quiescent() {
                return Ok(status);
            }
            if deadline.expired() {
                return Err(Error::DeadlineExceeded);
            }
            self.inner.auto_compaction.wait_change(deadline)?;
        }
    }
    /// Explicitly abandon a checkpoint token;If it is still valid Log The reference is deferred and the action occupied is released..
    pub fn release_checkpoint(
        &self,
        token: CheckpointToken,
    ) -> Result<MaintenanceTicket<CheckpointReleaseReport>, Error> {
        self.inner.start_checkpoint_release(token)
    }
    /// Accept one of three checkpoints,Return to pending tickets.Index No commitment to session progress,Log Must be bound
    /// Index materials submitted in this instance;Only successful reports represent persistence completion.
    pub fn checkpoint(
        &self,
        kind: CheckpointKind,
    ) -> Result<MaintenanceTicket<CheckpointReport>, Error> {
        self.inner.start_checkpoint(kind)
    }
    /// Migrate the records that still need to be retained according to the specified algorithm;Options explicitly determine subsequent checkpoints and logical truncation.
    /// Accepted tasks may copy some records even if they fail,Error reporting preserves actual impact,Can't blindly replay.
    pub fn compact(
        &self,
        options: CompactionOptions,
    ) -> Result<MaintenanceTicket<CompactionReport>, Error> {
        self.inner.start_compaction(options)
    }
    /// Truncate the logical log before the specified address;The caller must first confirm that the latest required values have been migrated.
    /// Physical recycling can be postponed due to lease,Successful startup does not mean that deletion has been completed.
    pub fn shift_begin(&self, address: LogAddress) -> Result<MaintenanceTicket<GcReport>, Error> {
        self.inner.start_gc(address)
    }
    /// Double the number of buckets online;Save after waiting for report new_buckets,Subsequent recovery needs to match the capacity of the material.
    pub fn grow_index(&self) -> Result<MaintenanceTicket<IndexGrowthReport>, Error> {
        self.inner.start_growth()
    }
    pub fn poll(&self, budget: PollBudget) -> Result<Progress, Error> {
        self.inner.poll_maintenance(budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn internal_subtasks_hand_over_the_original_error_and_the_removed_status_is_prohibited_from_terminating_again()
     {
        let (mut ticket, complete) =
            MaintenanceTicket::<()>::pair(StoreId([1; 16]), MaintenanceId(1));
        assert!(ticket.take_owned_report().unwrap().is_none());
        let shared = complete
            .finish(Err(Error::Io(std::io::Error::from_raw_os_error(13))))
            .unwrap();
        assert!(
            ticket.take_owned_report().unwrap().is_none(),
            "Shared observations are not over yet"
        );
        drop(shared);
        let result = ticket.take_owned_report().unwrap().unwrap();
        assert!(matches!(result, Err(Error::Io(error)) if error.raw_os_error()==Some(13)));
        assert!(complete.finish(Ok(())).is_err());
        drop(complete);
        assert!(
            ticket.try_report().is_err(),
            "Completion-side destructor cannot change the removed result to another finalizer"
        );
    }
}
