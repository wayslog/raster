//! Device completion only passes owned buffers across threads;Mailbox does not hold user requests or callbacks.
use crate::{
    device::{CompletionRoute, Device, IoCompletion},
    types::*,
};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
struct Mailbox {
    id: RequestId,
    completion: Option<IoCompletion>,
    completions: u64,
}
struct State {
    mailboxes: Mailboxes,
}
/// A capacity credit and unique identity, owned by one operation task.
/// The engine must register it before exposing a pending ticket or submitting I/O.
#[must_use = "Operation routes must be released by their owning task"]
pub(crate) struct OperationRoute {
    id: RequestId,
    registered: bool,
    released: bool,
}
impl OperationRoute {
    pub fn id(&self) -> RequestId {
        self.id
    }
    pub fn registered_id(&self) -> Option<RequestId> {
        self.registered.then_some(self.id)
    }
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
    #[cfg(test)]
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
    occupied: AtomicUsize,
    next: AtomicU64,
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
            occupied: AtomicUsize::new(0),
            next: AtomicU64::new(0),
            state: Mutex::new(State {
                mailboxes: Mailboxes::new(),
            }),
            polling: Mutex::new(()),
        })
    }
    pub fn reserve(&self, session: SessionId) -> Result<RequestId, Error> {
        let mut route = self.reserve_operation(session)?;
        if let Err(error) = self.activate_operation(&mut route) {
            self.release_operation(&mut route)?;
            return Err(error);
        }
        Ok(route.id)
    }
    pub fn reserve_operation(&self, session: SessionId) -> Result<OperationRoute, Error> {
        session.validate()?;
        if self.state.is_poisoned() {
            return Err(Error::InvalidState("completion_mailbox_lock_poisoned"));
        }
        self.occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.capacity).then(|| count + 1)
            })
            .map_err(|_| Error::Busy)?;
        let slot = match self
            .next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                next.checked_add(1)
            }) {
            Ok(slot) => slot,
            Err(_) => {
                self.occupied.fetch_sub(1, Ordering::Release);
                return Err(Error::CapacityExceeded);
            }
        };
        let id = RequestId {
            store: self.store,
            session,
            slot,
            generation: Generation(0),
        };
        Ok(OperationRoute {
            id,
            registered: false,
            released: false,
        })
    }
    pub fn activate_operation(&self, route: &mut OperationRoute) -> Result<(), Error> {
        if route.id.store != self.store || route.released {
            return Err(Error::InvalidState(
                "operation route ownership does not match",
            ));
        }
        if self.state.is_poisoned() {
            return Err(Error::InvalidState("completion_mailbox_lock_poisoned"));
        }
        if route.registered {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("completion_mailbox_lock_poisoned"))?;
        let id = route.id;
        state.mailboxes.insert(
            id.slot,
            Mailbox {
                id,
                completion: None,
                completions: 0,
            },
        );
        route.registered = true;
        Ok(())
    }
    pub fn release_operation(&self, route: &mut OperationRoute) -> Result<(), Error> {
        if route.id.store != self.store || route.released {
            return Err(Error::InvalidState(
                "operation route ownership does not match",
            ));
        }
        if route.registered {
            self.release(route.id)?;
        } else {
            // No mailbox or I/O buffer exists; this credit can also be returned
            // during failed shutdown without entering a poisoned mailbox table.
            self.occupied.fetch_sub(1, Ordering::Release);
        }
        route.released = true;
        Ok(())
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
        self.occupied.fetch_sub(1, Ordering::Release);
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
            } else if completion.route.0 >= self.next.load(Ordering::Acquire) {
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
mod tests;
