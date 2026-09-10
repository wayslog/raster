//! 本线程的一次性结果槽；票据不借用 Session，销毁票据不取消请求。
use crate::types::{OperationError, RequestId, TicketError};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbortReason {
    Tombstone,
    ConditionNotMet,
}
#[derive(Debug)]
pub enum Outcome<T> {
    Success(T),
    NotFound,
    Aborted(AbortReason),
}
pub type OperationResult<T> = Result<Outcome<T>, OperationError>;
pub enum Submission<T: 'static> {
    Ready(OperationResult<T>),
    Pending(Ticket<T>),
}
pub enum TicketState<T> {
    Pending,
    Ready(OperationResult<T>),
}

enum Slot<T> {
    Pending,
    Ready(OperationResult<T>),
    Taken,
}

/// 同会话的本线程票据，不允许发送到其他线程。
///
/// ```compile_fail
/// use raster::Ticket;
/// fn require_send<T: Send>() {}
/// require_send::<Ticket<u64>>();
/// ```
/// ```compile_fail
/// use raster::Ticket;
/// fn require_sync<T: Sync>() {}
/// require_sync::<Ticket<u64>>();
/// ```
pub struct Ticket<T: 'static> {
    id: RequestId,
    slot: Rc<RefCell<Slot<T>>>,
}

#[allow(dead_code)] // 挂起执行器接入前，生产构造路径暂未使用。
pub(crate) struct Completer<T: 'static> {
    slot: Rc<RefCell<Slot<T>>>,
}
impl<T: 'static> Ticket<T> {
    #[allow(dead_code)]
    pub(crate) fn pair(id: RequestId) -> (Self, Completer<T>) {
        let slot = Rc::new(RefCell::new(Slot::Pending));
        (
            Self {
                id,
                slot: Rc::clone(&slot),
            },
            Completer { slot },
        )
    }
    pub fn id(&self) -> RequestId {
        self.id
    }
    /// 只观察/收取，不推进设备；关闭 Session 后仍可收取已完成结果。
    pub fn try_take(&mut self) -> Result<TicketState<T>, TicketError> {
        let mut slot = self
            .slot
            .try_borrow_mut()
            .map_err(|_| TicketError::BorrowConflict)?;
        match &*slot {
            Slot::Pending => Ok(TicketState::Pending),
            Slot::Taken => Err(TicketError::AlreadyTaken),
            Slot::Ready(_) => {
                if let Slot::Ready(result) = std::mem::replace(&mut *slot, Slot::Taken) {
                    Ok(TicketState::Ready(result))
                } else {
                    unreachable!("同一独占借用中的槽状态不会改变")
                }
            }
        }
    }
}
#[allow(dead_code)]
impl<T: 'static> Completer<T> {
    pub(crate) fn finish(&self, result: OperationResult<T>) -> Result<(), TicketError> {
        let mut slot = self
            .slot
            .try_borrow_mut()
            .map_err(|_| TicketError::BorrowConflict)?;
        if !matches!(*slot, Slot::Pending) {
            return Err(TicketError::AlreadyCompleted);
        }
        *slot = Slot::Ready(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    fn id() -> RequestId {
        RequestId {
            store: StoreId([1; 16]),
            session: SessionId([2; 16]),
            slot: 0,
            generation: Generation(1),
        }
    }
    #[test]
    fn 结果只能终结和收取一次() {
        let (mut ticket, complete) = Ticket::pair(id());
        assert!(matches!(ticket.try_take(), Ok(TicketState::Pending)));
        assert_eq!(complete.finish(Ok(Outcome::Success(7))), Ok(()));
        assert_eq!(
            complete.finish(Ok(Outcome::Success(8))),
            Err(TicketError::AlreadyCompleted)
        );
        assert!(matches!(
            ticket.try_take(),
            Ok(TicketState::Ready(Ok(Outcome::Success(7))))
        ));
        assert!(matches!(ticket.try_take(), Err(TicketError::AlreadyTaken)));
    }
    #[test]
    fn 完成端销毁后票据仍拥有结果() {
        let (mut ticket, complete) = Ticket::pair(id());
        complete
            .finish(Ok(Outcome::Success(String::from("完成"))))
            .unwrap();
        drop(complete);
        assert!(
            matches!(ticket.try_take(), Ok(TicketState::Ready(Ok(Outcome::Success(value)))) if value == "完成")
        );
    }
    #[test]
    fn 放弃票据不阻止请求终结() {
        let (ticket, complete) = Ticket::pair(id());
        drop(ticket);
        assert_eq!(complete.finish(Ok(Outcome::Success(1))), Ok(()));
    }
}
