//! Heterogeneous pending tasks only belong to Session;Save only logical IDs and owned inputs across awaits.
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
    pub registration: crate::coordination::RegistrationHandle,
    pub tracker: std::sync::Arc<super::metrics::Tracker>,
    pub current: ExecutionContext,
    pub previous: Option<ExecutionContext>,
    pub closing: bool,
    pub poll_cursor: Option<u64>,
    // The pool only retains budget control blocks;It can only be reused after all external holders exit.,User results are not retained.
    results: Vec<std::rc::Rc<()>>,
    pub routes: Option<super::io_hub::SessionRoutes>,
}

impl SessionRuntime {
    pub fn new(
        id: SessionId,
        registered: crate::coordination::RegisteredSession,
        tracker: std::sync::Arc<super::metrics::Tracker>,
        routes: super::io_hub::SessionRoutes,
    ) -> Self {
        let crate::coordination::RegisteredSession {
            handle,
            version,
            last_accepted,
        } = registered;
        Self {
            id,
            registration: handle,
            tracker,
            current: ExecutionContext {
                version,
                last_accepted,
                tasks: BTreeMap::new(),
            },
            previous: None,
            closing: false,
            poll_cursor: None,
            results: Vec::new(),
            routes: Some(routes),
        }
    }
    pub fn reserve_route(&self) -> Result<super::io_hub::SessionRoute, Error> {
        self.routes
            .as_ref()
            .ok_or(Error::InvalidState("session_routes_closed"))?
            .reserve()
    }
    /// Preserve the identity of old tasks,Version and fixed serial number;The new context will only accept subsequent accepted requests..
    pub fn switch_version(&mut self, version: CheckpointVersion) -> Result<bool, Error> {
        if version == self.current.version {
            return Ok(false);
        }
        if self.current.version.0.checked_add(1) != Some(version.0) {
            return Err(Error::InvalidState(
                "The session version must be continuously incremented",
            ));
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
            return Err(Error::InvalidState(
                "The pending task is inconsistent with the session context",
            ));
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
                .ok_or(Error::InvalidState(
                    "The session does not have this version context",
                ))?
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
            // When the historical capacity is greater than the current limit,A free slot does not mean that a request can still be accepted.
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
        let coordinator = crate::coordination::Coordinator::new(1).unwrap();
        let id = SessionId([2; 16]);
        SessionRuntime::new(
            id,
            coordinator.enroll_registered(id).unwrap(),
            super::super::metrics::Metrics::new(false)
                .register()
                .unwrap(),
            std::sync::Arc::new(
                super::super::io_hub::CompletionHub::new(StoreId([1; 16]), 1).unwrap(),
            )
            .session_routes(id, 1)
            .unwrap(),
        )
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
    fn result_places_are_waiting_for_tickets_and_completion_ends_are_returned() {
        for complete_first in [false, true] {
            let mut runtime = runtime();
            let credit = runtime.reserve_result(1).unwrap();
            let (mut ticket, complete) = Ticket::pair_bounded(request_id(), credit);
            complete
                .finish(Ok(Outcome::Success(String::from("Possessive results"))))
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
            // Old tickets that have been collected cannot be returned to the quota that is being used by subsequent requests..
            assert!(matches!(ticket.try_take(), Err(TicketError::AlreadyTaken)));
            drop(ticket);
            assert!(matches!(runtime.reserve_result(1), Err(Error::Busy)));
            drop(next);
            assert!(runtime.reserve_result(1).is_ok());
            assert!(
                matches!(output, TicketState::Ready(Ok(Outcome::Success(value))) if value == "Possessive results")
            );
        }
    }

    #[test]
    fn after_giving_up_the_ticket_the_completion_end_still_occupies_the_quota() {
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
    fn historical_capacity_cannot_bypass_reduced_result_limits() {
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
