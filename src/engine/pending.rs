//! 异构挂起任务只属于 Session；跨等待仅保存逻辑标识和拥有型输入。
use crate::{device::IoCompletion, types::*};
use std::collections::BTreeMap;

pub(crate) enum TaskStep {
    Complete,
    AwaitingIo,
    Retry,
    Failed(OperationError),
}
pub(crate) trait PendingTask: 'static {
    fn id(&self) -> RequestId;
    fn serial(&self) -> Serial;
    fn version(&self) -> CheckpointVersion;
    fn on_io(&mut self, completion: IoCompletion) -> Result<(), Error>;
    fn step(&mut self) -> TaskStep;
    fn abandon(&mut self, error: OperationError);
}
pub(crate) struct ExecutionContext {
    pub version: CheckpointVersion,
    pub last_accepted: Option<Serial>,
    pub tasks: BTreeMap<u64, Box<dyn PendingTask>>,
}
pub(crate) struct SessionRuntime {
    pub current: ExecutionContext,
    pub previous: Option<ExecutionContext>,
    pub closing: bool,
}

impl SessionRuntime {
    pub fn new(last_accepted: Option<Serial>) -> Self {
        Self {
            current: ExecutionContext {
                version: CheckpointVersion(0),
                last_accepted,
                tasks: BTreeMap::new(),
            },
            previous: None,
            closing: false,
        }
    }
    pub fn pending(&self) -> usize {
        self.current.tasks.len() + self.previous.as_ref().map_or(0, |old| old.tasks.len())
    }
}
