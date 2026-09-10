//! 复合维护按步骤释放/重新争取全局动作，不能持锁等待自己发起的检查点。
use crate::types::*;

pub(crate) enum MaintenanceStep {
    Scan,
    ConditionalCopy,
    ShiftBegin,
    CleanIndex,
    ReclaimSegments,
    Checkpoint,
    Complete,
}
pub(crate) struct MaintenanceJob {
    pub id: MaintenanceId,
    pub step: MaintenanceStep,
    pub until: LogAddress,
}
pub(crate) struct RetainedRange {
    pub token: CheckpointToken,
    pub begin: LogAddress,
    pub end: LogAddress,
}
pub(crate) struct ReclamationPlan {
    pub logical_begin: LogAddress,
    pub retained: Vec<RetainedRange>,
}
impl MaintenanceJob {
    pub fn poll(&mut self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("maintenance::job"))
    }
}
impl ReclamationPlan {
    pub fn deletable_segments(&self) -> Result<Vec<u64>, Error> {
        Err(Error::unimplemented("maintenance::retention"))
    }
}
