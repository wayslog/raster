//! 异构挂起任务只属于 Session；跨等待仅保存逻辑标识和拥有型输入。
use crate::{device::IoCompletion, types::*};
use std::collections::BTreeMap;

pub(crate) enum TaskStep {
    Complete,
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
    pub fn new(id: SessionId, last_accepted: Option<Serial>, version: CheckpointVersion) -> Self {
        Self {
            id,
            current: ExecutionContext {
                version,
                last_accepted,
                tasks: BTreeMap::new(),
            },
            previous: None,
            closing: false,
            poll_cursor: None,
            results: Vec::new(),
        }
    }
    /// 保留旧任务的身份、版本与固定序号；新上下文只承接后续接受的请求。
    pub fn switch_version(&mut self, version: CheckpointVersion) -> Result<bool, Error> {
        if version == self.current.version {
            return Ok(false);
        }
        if self.current.version.0.checked_add(1) != Some(version.0) {
            return Err(Error::InvalidState("会话版本必须连续递增"));
        }
        if self
            .previous
            .as_ref()
            .is_some_and(|old| !old.tasks.is_empty())
        {
            return Err(Error::Busy);
        }
        if self.current.tasks.values().any(|task| {
            task.version() != self.current.version
                || task.id().session != self.id
                || Some(task.serial()) > self.current.last_accepted
        }) {
            return Err(Error::InvalidState("挂起任务与会话上下文不一致"));
        }
        let next = ExecutionContext {
            version,
            last_accepted: self.current.last_accepted,
            tasks: BTreeMap::new(),
        };
        self.previous = Some(std::mem::replace(&mut self.current, next));
        Ok(true)
    }
    pub fn cut(
        &self,
        version: CheckpointVersion,
    ) -> Result<crate::coordination::SessionCut, Error> {
        let context = if self.current.version == version {
            &self.current
        } else {
            self.previous
                .as_ref()
                .filter(|old| old.version == version)
                .ok_or(Error::InvalidState("会话没有该版本上下文"))?
        };
        Ok(crate::coordination::SessionCut {
            session: self.id,
            last_accepted: context.last_accepted,
            old_pending: context.tasks.len(),
        })
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
