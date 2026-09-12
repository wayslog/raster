//! Device completion only passes owned buffers across threads;Mailbox does not hold user requests or callbacks.
use crate::{
    device::{CompletionRoute, Device, IoCompletion},
    types::*,
};
use std::{collections::BTreeMap, sync::Mutex};
struct Mailbox {
    id: RequestId,
    completion: Option<IoCompletion>,
    completions: u64,
}
struct State {
    next: u64,
    mailboxes: Mailboxes,
}
/// Reuse a few slots for synchronous requests without moving tree leaf payloads.
/// Overflow retains exact routing and is bounded by the hub's existing capacity.
struct Mailboxes {
    inline: [Option<Mailbox>; 4],
    overflow: BTreeMap<u64, Mailbox>,
    len: usize,
}
impl Mailboxes {
    fn new() -> Self {
        Self {
            inline: std::array::from_fn(|_| None),
            overflow: BTreeMap::new(),
            len: 0,
        }
    }
    fn len(&self) -> usize {
        self.len
    }
    fn get(&self, route: &u64) -> Option<&Mailbox> {
        self.inline
            .iter()
            .flatten()
            .find(|mailbox| mailbox.id.slot == *route)
            .or_else(|| self.overflow.get(route))
    }
    fn get_mut(&mut self, route: &u64) -> Option<&mut Mailbox> {
        self.inline
            .iter_mut()
            .flatten()
            .find(|mailbox| mailbox.id.slot == *route)
            .or_else(|| self.overflow.get_mut(route))
    }
    fn insert(&mut self, route: u64, mailbox: Mailbox) {
        if let Some(slot) = self.inline.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(mailbox);
        } else {
            self.overflow.insert(route, mailbox);
        }
        self.len += 1;
    }
    fn remove(&mut self, route: &u64) {
        if let Some(slot) = self.inline.iter_mut().find(|slot| {
            slot.as_ref()
                .is_some_and(|mailbox| mailbox.id.slot == *route)
        }) {
            *slot = None;
            self.len -= 1;
        } else if self.overflow.remove(route).is_some() {
            self.len -= 1;
        }
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut Mailbox> {
        self.inline
            .iter_mut()
            .flatten()
            .chain(self.overflow.values_mut())
    }
}
pub(crate) struct CompletionHub {
    store: StoreId,
    capacity: usize,
    state: Mutex<State>,
    polling: Mutex<()>,
}
impl CompletionHub {
    pub fn new(store: StoreId, capacity: usize) -> Result<Self, Error> {
        store.validate()?;
        if capacity == 0 {
            return Err(Error::CapacityExceeded);
        }
        Ok(Self {
            store,
            capacity,
            state: Mutex::new(State {
                next: 0,
                mailboxes: Mailboxes::new(),
            }),
            polling: Mutex::new(()),
        })
    }
    pub fn reserve(&self, session: SessionId) -> Result<RequestId, Error> {
        session.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        if state.mailboxes.len() >= self.capacity {
            return Err(Error::Busy);
        }
        let next = state.next.checked_add(1).ok_or(Error::CapacityExceeded)?;
        let id = RequestId {
            store: self.store,
            session,
            slot: state.next,
            generation: Generation(0),
        };
        state.mailboxes.insert(
            id.slot,
            Mailbox {
                id,
                completion: None,
                completions: 0,
            },
        );
        state.next = next;
        Ok(id)
    }
    /// Route numbers are assigned monotonically within the engine to which the device belongs.,Do not reuse slot numbers,Reject when exhausted.
    pub fn route(id: RequestId) -> CompletionRoute {
        CompletionRoute(id.slot)
    }
    pub fn take(&self, id: RequestId) -> Result<Option<IoCompletion>, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        let mailbox = state
            .mailboxes
            .get_mut(&id.slot)
            .ok_or(Error::InvalidState("The request email does not exist"))?;
        if mailbox.id != id {
            return Err(Error::InvalidState(
                "request mailbox ownership does not match",
            ));
        }
        Ok(mailbox.completion.take())
    }
    pub fn completion_count(&self, id: RequestId) -> Result<u64, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        state
            .mailboxes
            .get(&id.slot)
            .filter(|mailbox| mailbox.id == id)
            .map(|mailbox| mailbox.completions)
            .ok_or(Error::InvalidState("Completion count route does not exist"))
    }
    /// Logout when requesting termination or session abandonment;The device still has the I/O buffer.
    pub fn release(&self, id: RequestId) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        if state
            .mailboxes
            .get(&id.slot)
            .is_none_or(|mailbox| mailbox.id != id)
        {
            return Err(Error::InvalidState(
                "request mailbox ownership does not match",
            ));
        }
        state.mailboxes.remove(&id.slot);
        Ok(())
    }
    /// Only if the device has shutdown,Called after all publishing threads have exited;The final state only discards possession completion,No more delivering tasks.
    pub(crate) fn discard_after_device_shutdown(&self, device: &dyn Device) -> Result<(), Error> {
        // Equipment poll of panic Possible poisoning of serialization locks;Shut down terminated device,Final state recycling does not restore running permissions.
        let _polling = self
            .polling
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut completions = Vec::new();
        loop {
            device.poll(PollBudget::default(), &mut completions)?;
            if completions.is_empty() {
                break;
            }
            completions.clear();
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        for mailbox in state.mailboxes.values_mut() {
            mailbox.completion = None;
        }
        Ok(())
    }
    pub fn poll(&self, device: &dyn Device, budget: PollBudget) -> Result<usize, Error> {
        let _polling = match self.polling.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(0),
            Err(_) => return Err(Error::InvalidState("Complete polling lock poisoning")),
        };
        let mut completions = Vec::new();
        let limit = budget.0.get().min(self.capacity);
        let mut error = device
            .poll(
                PollBudget(std::num::NonZeroUsize::new(limit).expect("Capacity is non-zero")),
                &mut completions,
            )
            .err();
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        let count = completions.len();
        for completion in completions {
            if let Some(mailbox) = state.mailboxes.get_mut(&completion.route.0) {
                mailbox.completions = mailbox.completions.saturating_add(1);
                if mailbox.completion.is_some() {
                    error.get_or_insert(Error::InvalidState(
                        "There are multiple uncollected requests for one request I/O completed",
                    ));
                } else {
                    mailbox.completion = Some(completion);
                }
            } else if completion.route.0 >= state.next {
                error.get_or_insert(Error::InvalidState(
                    "Device returns unassigned completion route",
                ));
            }
            // Unregistered historical routes only release the completion buffer,Abandoned user requests are not invoked.
        }
        match error {
            Some(error) => Err(error),
            None => Ok(count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{IoOperation, IoRequest, memory::MemoryDevice};
    #[test]
    fn released_inline_routes_cannot_consume_late_completions_or_displace_overflow() {
        let hub = CompletionHub::new(StoreId([1; 16]), 7).unwrap();
        let device = MemoryDevice::new(16, 64).unwrap();
        let session = SessionId([2; 16]);
        let original: Vec<_> = (0..7).map(|_| hub.reserve(session).unwrap()).collect();
        assert!(matches!(hub.reserve(session), Err(Error::Busy)));
        for id in &original {
            device
                .submit(IoRequest {
                    route: CompletionHub::route(*id),
                    operation: IoOperation::CreateDirectory(format!("route-{}", id.slot).into()),
                })
                .unwrap();
        }
        hub.release(original[0]).unwrap();
        hub.release(original[3]).unwrap();
        let first = hub.reserve(session).unwrap();
        let second = hub.reserve(session).unwrap();
        assert!(first.slot > original[6].slot && second.slot > first.slot);
        assert!(matches!(hub.reserve(session), Err(Error::Busy)));
        assert_eq!(hub.poll(&device, PollBudget::default()).unwrap(), 7);
        for id in [first, second] {
            assert!(hub.take(id).unwrap().is_none());
            assert_eq!(hub.completion_count(id).unwrap(), 0);
        }
        for index in [1, 2, 4, 5, 6] {
            let id = original[index];
            assert!(hub.take(id).unwrap().unwrap().result.is_ok());
            assert_eq!(hub.completion_count(id).unwrap(), 1);
            hub.release(id).unwrap();
        }
        for id in [first, second] {
            hub.release(id).unwrap();
        }
        assert!(hub.release(original[0]).is_err());
        assert_eq!(hub.state.lock().unwrap().mailboxes.len(), 0);
    }
    #[test]
    fn after_other_session_polling_the_results_will_remain_in_the_original_mailbox_and_cannot_be_collected_if_the_identity_is_wrong()
     {
        let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
        let device = MemoryDevice::new(4, 64).unwrap();
        let first = hub.reserve(SessionId([1; 16])).unwrap();
        let second = hub.reserve(SessionId([2; 16])).unwrap();
        assert!(matches!(hub.reserve(SessionId([3; 16])), Err(Error::Busy)));
        for id in [first, second] {
            device
                .submit(IoRequest {
                    route: CompletionHub::route(id),
                    operation: IoOperation::CreateDirectory(format!("directory{}", id.slot).into()),
                })
                .unwrap();
        }
        assert_eq!(hub.poll(&device, PollBudget::default()).unwrap(), 2);
        let mut wrong = first;
        wrong.session = second.session;
        assert!(hub.take(wrong).is_err());
        assert!(hub.release(wrong).is_err());
        assert!(hub.take(second).unwrap().unwrap().result.is_ok());
        assert!(hub.take(first).unwrap().unwrap().result.is_ok());
        assert!(hub.take(first).unwrap().is_none());
        hub.release(first).unwrap();
        let next = hub.reserve(first.session).unwrap();
        assert!(next.slot > second.slot);
    }
    #[test]
    fn late_completion_after_logging_out_only_recycles_the_buffer_and_does_not_accidentally_throw_new_requests()
     {
        let hub = CompletionHub::new(StoreId([1; 16]), 1).unwrap();
        let device = MemoryDevice::new(4, 64).unwrap();
        let old = hub.reserve(SessionId([1; 16])).unwrap();
        device
            .submit(IoRequest {
                route: CompletionHub::route(old),
                operation: IoOperation::Read {
                    file: crate::device::FileId {
                        slot: 999,
                        generation: Generation(0),
                    },
                    offset: 0,
                    buffer: crate::device::AlignedBuffer::new_zeroed(8, 8).unwrap(),
                },
            })
            .unwrap();
        hub.release(old).unwrap();
        let new = hub.reserve(old.session).unwrap();
        hub.poll(&device, PollBudget::default()).unwrap();
        assert!(hub.take(new).unwrap().is_none());
    }
    #[test]
    fn repeated_uncollected_completion_and_unknown_routing_errors_are_reported_but_other_mailboxes_are_not_covered_or_lost()
     {
        let hub = CompletionHub::new(StoreId([1; 16]), 4).unwrap();
        let device = MemoryDevice::new(4, 64).unwrap();
        let first = hub.reserve(SessionId([1; 16])).unwrap();
        let second = hub.reserve(SessionId([2; 16])).unwrap();
        let submit = |route, name: &str| {
            device
                .submit(IoRequest {
                    route,
                    operation: IoOperation::CreateDirectory(name.into()),
                })
                .unwrap()
        };
        let first_io = submit(CompletionHub::route(first), "first time");
        submit(CompletionHub::route(first), "Repeat");
        let second_io = submit(CompletionHub::route(second), "another session");
        assert!(matches!(
            hub.poll(&device, PollBudget::default()),
            Err(Error::InvalidState(_))
        ));
        assert_eq!(hub.take(first).unwrap().unwrap().id, first_io);
        assert!(hub.take(first).unwrap().is_none());
        assert_eq!(hub.take(second).unwrap().unwrap().id, second_io);
        submit(CompletionRoute(999), "unknown route");
        assert!(matches!(
            hub.poll(&device, PollBudget::default()),
            Err(Error::InvalidState(_))
        ));
        assert!(hub.take(first).unwrap().is_none());
        assert!(hub.take(second).unwrap().is_none());
    }
}
