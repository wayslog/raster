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
    fn step(&mut self, budget: PollBudget) -> TaskStep;
    fn abandon(&mut self, error: OperationError);
}
pub(crate) struct ExecutionContext {
    pub version: CheckpointVersion,
    pub last_accepted: Option<Serial>,
    pub tasks: BTreeMap<u64, Box<dyn PendingTask>>,
}
pub(crate) struct SessionRuntime {
    pub id: SessionId,
    pub current: ExecutionContext,
    pub previous: Option<ExecutionContext>,
    pub closing: bool,
    pub poll_cursor: Option<u64>,
    results: Vec<std::rc::Weak<()>>,
}

impl SessionRuntime {
    pub fn new(id: SessionId, last_accepted: Option<Serial>) -> Self {
        Self {
            id,
            current: ExecutionContext {
                version: CheckpointVersion(0),
                last_accepted,
                tasks: BTreeMap::new(),
            },
            previous: None,
            closing: false,
            poll_cursor: None,
            results: Vec::new(),
        }
    }
    pub fn reserve_result(&mut self, limit: usize) -> Result<std::rc::Rc<()>, Error> {
        self.results.retain(|credit| credit.strong_count() != 0);
        if self.results.len() >= limit {
            return Err(Error::Busy);
        }
        self.results
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        let credit = std::rc::Rc::new(());
        self.results.push(std::rc::Rc::downgrade(&credit));
        Ok(credit)
    }
    pub fn pending(&self) -> usize {
        self.current.tasks.len() + self.previous.as_ref().map_or(0, |old| old.tasks.len())
    }
}
