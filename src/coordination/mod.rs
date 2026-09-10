//! 会话登记、双版本上下文与顶层动作仲裁；与安全回收 epoch 分开。
use crate::types::*;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Action {
    CheckpointFull,
    CheckpointIndex,
    CheckpointLog,
    Recover,
    Gc,
    GrowIndex,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Phase {
    Rest,
    PrepareIndex,
    IndexSnapshot,
    Prepare,
    InProgress,
    WaitPending,
    WaitFlush,
    Publish,
    GcIo,
    GcIndex,
    GrowPrepare,
    GrowCopy,
    Failed,
}
pub(crate) struct SystemState {
    pub action: Option<Action>,
    pub phase: Phase,
    pub version: CheckpointVersion,
}
pub(crate) struct SessionCut {
    pub session: SessionId,
    pub last_accepted: Option<Serial>,
    pub old_pending: usize,
}
pub(crate) struct Coordinator {
    state: crate::sync::Mutex<SystemState>,
}
impl Coordinator {
    pub fn start_action(&self, _action: Action) -> Result<MaintenanceId, Error> {
        Err(Error::unimplemented("coordination::start_action"))
    }
    pub fn enroll(&self, _session: SessionId) -> Result<(), Error> {
        Err(Error::unimplemented("coordination::enroll"))
    }
    pub fn acknowledge(&self, _cut: SessionCut, _phase: Phase) -> Result<(), Error> {
        Err(Error::unimplemented("coordination::acknowledge"))
    }
    pub fn fail_action(&self, _cause: Error) -> Result<(), Error> {
        Err(Error::unimplemented("coordination::fail"))
    }
    pub fn leave(&self, _session: SessionId) -> Result<(), Error> {
        Err(Error::unimplemented("coordination::leave"))
    }
}
