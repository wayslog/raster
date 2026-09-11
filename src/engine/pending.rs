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
    // 池只保留预算控制块；外部持有者全部退出后才能复用，不保留用户结果。
    results: Vec<std::rc::Rc<()>>,
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
        if let Some(credit) = self
            .results
            .iter()
            .find(|credit| std::rc::Rc::strong_count(credit) == 1)
        {
            // 历史容量大于当前限额时，空闲槽不代表仍能接受一个请求。
            if self.results.len() > limit
                && self
                    .results
                    .iter()
                    .filter(|credit| std::rc::Rc::strong_count(credit) > 1)
                    .count()
                    >= limit
            {
                return Err(Error::Busy);
            }
            return Ok(std::rc::Rc::clone(credit));
        }
        if self.results.len() >= limit {
            return Err(Error::Busy);
        }
        self.results
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        let credit = std::rc::Rc::new(());
        self.results.push(std::rc::Rc::clone(&credit));
        Ok(credit)
    }
    pub fn pending(&self) -> usize {
        self.current.tasks.len() + self.previous.as_ref().map_or(0, |old| old.tasks.len())
    }
}

#[cfg(test)]
mod result_budget_tests {
    use super::*;
    use crate::api::completion::{Outcome, Ticket, TicketState};

    fn runtime() -> SessionRuntime {
        SessionRuntime::new(SessionId([2; 16]), None, CheckpointVersion(0))
    }

    fn request_id() -> RequestId {
        RequestId {
            store: StoreId([1; 16]),
            session: SessionId([2; 16]),
            slot: 0,
            generation: Generation(1),
        }
    }

    #[test]
    fn 结果名额等待票据和完成端都归还() {
        for complete_first in [false, true] {
            let mut runtime = runtime();
            let credit = runtime.reserve_result(1).unwrap();
            let (mut ticket, complete) = Ticket::pair_bounded(request_id(), credit);
            complete
                .finish(Ok(Outcome::Success(String::from("拥有型结果"))))
                .unwrap();
            assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
            let output = if complete_first {
                drop(complete);
                assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
                ticket.try_take().unwrap()
            } else {
                let output = ticket.try_take().unwrap();
                assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
                drop(complete);
                output
            };
            let next = runtime.reserve_result(1).unwrap();
            // 已收取的旧票据不能归还后续请求正在使用的名额。
            assert!(matches!(ticket.try_take(), Err(TicketError::AlreadyTaken)));
            drop(ticket);
            assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
            drop(next);
            assert!(runtime.reserve_result(1).is_ok());
            assert!(
                matches!(output, TicketState::Ready(Ok(Outcome::Success(value))) if value == "拥有型结果")
            );
        }
    }

    #[test]
    fn 放弃票据后完成端仍占用名额() {
        let mut runtime = runtime();
        let (ticket, complete) =
            Ticket::<u64>::pair_bounded(request_id(), runtime.reserve_result(1).unwrap());
        drop(ticket);
        assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
        complete.finish(Ok(Outcome::Success(7))).unwrap();
        assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
        drop(complete);
        assert!(runtime.reserve_result(1).is_ok());
    }

    #[test]
    fn 历史容量不能绕过降低后的结果限额() {
        let mut runtime = runtime();
        let mut credits = (0..4)
            .map(|_| runtime.reserve_result(4).unwrap())
            .collect::<Vec<_>>();
        credits.truncate(2);
        assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
        assert!(matches!(runtime.reserve_result(2), Err(Error::Busy)));
        credits.truncate(1);
        assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
        let second = runtime.reserve_result(2).unwrap();
        assert!(matches!(runtime.reserve_result(0), Err(Error::Busy)));
        drop(second);
        drop(credits);
        assert!(matches!(runtime.reserve_result(0), Err(Error::Busy)));
        let only = runtime.reserve_result(1).unwrap();
        assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
        drop(only);
        assert!(runtime.reserve_result(1).is_ok());
    }
}
