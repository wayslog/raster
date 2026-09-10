//! 检查点材料、提交证据和恢复重放；不会把启动或文件写完当作持久化成功。
use crate::{format::Manifest, types::*};
pub(crate) mod directory;
pub(crate) mod material;
pub(crate) mod publication;

pub(crate) enum CheckpointPhase {
    CollectIndex,
    CutSessions,
    DrainOld,
    Freeze,
    WriteMaterial,
    SyncMaterial,
    WriteManifest,
    PublishCommit,
    Complete,
    Failed,
}
pub(crate) struct CheckpointMachine {
    pub token: CheckpointToken,
    pub phase: CheckpointPhase,
    pub manifest: Manifest,
}
pub(crate) struct RecoveryPlan {
    pub manifest: Manifest,
    pub replay_from: LogAddress,
    pub replay_until: LogAddress,
}
impl CheckpointMachine {
    pub fn step(&mut self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("checkpoint::step"))
    }
}
impl RecoveryPlan {
    pub fn validate(&self) -> Result<(), Error> {
        Err(Error::unimplemented("checkpoint::validate"))
    }
    pub fn replay_step(&mut self, _budget: PollBudget) -> Result<Progress, Error> {
        Err(Error::unimplemented("checkpoint::replay"))
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod publication_tests;
