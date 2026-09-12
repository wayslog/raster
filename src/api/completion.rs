//! One-time result slot for this thread;Notes are not borrowed Session,Destroying the ticket does not cancel the request.
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

/// This thread ticket for the same session,Not allowed to send to other threads.
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
    credit: Option<Rc<()>>,
}

pub(crate) struct Completer<T: 'static> {
    slot: Rc<RefCell<Slot<T>>>,
    credit: Option<Rc<()>>,
}
impl<T: 'static> Ticket<T> {
    pub(crate) fn pair(id: RequestId) -> (Self, Completer<T>) {
        let slot = Rc::new(RefCell::new(Slot::Pending));
        (
            Self {
                id,
                credit: None,
                slot: Rc::clone(&slot),
            },
            Completer { slot, credit: None },
        )
    }
    pub(crate) fn pair_bounded(id: RequestId, credit: Rc<()>) -> (Self, Completer<T>) {
        let (mut ticket, mut completer) = Self::pair(id);
        ticket.credit = Some(credit.clone());
        completer.credit = Some(credit);
        (ticket, completer)
    }
    pub fn id(&self) -> RequestId {
        self.id
    }
    /// Just observe/charge,Not advancing equipment;close Session You can still receive the completed results after.
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
                    self.credit = None;
                    Ok(TicketState::Ready(result))
                } else {
                    unreachable!("Slot status within the same exclusive borrow does not change")
                }
            }
        }
    }
}
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
    fn results_can_only_be_finalized_and_collected_once() {
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
    fn the_ticket_still_has_the_result_after_the_completion_end_is_destroyed() {
        let (mut ticket, complete) = Ticket::pair(id());
        complete
            .finish(Ok(Outcome::Success(String::from("completed"))))
            .unwrap();
        drop(complete);
        assert!(
            matches!(ticket.try_take(), Ok(TicketState::Ready(Ok(Outcome::Success(value)))) if value == "completed")
        );
    }
    #[test]
    fn abandoning_a_ticket_does_not_prevent_the_request_from_finalizing() {
        let (ticket, complete) = Ticket::pair(id());
        drop(ticket);
        assert_eq!(complete.finish(Ok(Outcome::Success(1))), Ok(()));
    }
}
