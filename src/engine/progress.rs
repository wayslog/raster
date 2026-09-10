//! 完成路由、重试与阶段推进的统一接口；不能用最大完成序号代替持久化进度。
use crate::{device::IoCompletion, types::*};

pub(crate) enum ResumeReason {
    Io(IoCompletion),
    EpochAdvanced,
    PhaseChanged,
    SpaceAvailable,
}
pub(crate) trait CompletionRouter {
    fn route(&mut self, completion: IoCompletion) -> Result<(), Error>;
    fn progress(&mut self, budget: PollBudget) -> Result<Progress, Error>;
}
