//! Session capacity is reserved once and retained by every outstanding owner.
use super::*;
use std::{
    ops::Deref,
    rc::Rc,
    sync::{Arc, Weak},
};

pub(super) struct SessionCapacity {
    hub: Weak<CompletionHub>,
    limit: usize,
}

impl Drop for SessionCapacity {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.occupied.fetch_sub(self.limit, Ordering::Release);
        }
    }
}

#[allow(
    clippy::redundant_allocation,
    reason = "Local routes clone Rc without atomic writes; only I/O mailboxes clone the inner Arc. Both allocations occur once per session."
)]
type LocalCapacity = Rc<Arc<SessionCapacity>>;

/// Only the issuing session and its local operation routes own this Rc.
/// Mailboxes retain the Send-compatible inner allocation after activation.
pub(crate) struct SessionRoutes {
    capacity: LocalCapacity,
    hub: Arc<CompletionHub>,
    session: SessionId,
    limit: usize,
}

pub(crate) struct SessionRoute {
    route: OperationRoute,
    capacity: LocalCapacity,
    // The retained capacity owns a Weak that prevents this hub allocation
    // address from being reused while the local route can compare it.
    owner: usize,
}

impl CompletionHub {
    pub fn session_routes(
        self: &Arc<Self>,
        session: SessionId,
        limit: usize,
    ) -> Result<SessionRoutes, Error> {
        session.validate()?;
        if limit == 0 {
            return Err(Error::CapacityExceeded);
        }
        if self.state.is_poisoned() {
            return Err(Error::InvalidState("completion_mailbox_lock_poisoned"));
        }
        self.occupied
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count
                    .checked_add(limit)
                    .filter(|next| *next <= self.capacity)
            })
            .map_err(|_| Error::Busy)?;
        Ok(SessionRoutes {
            capacity: Rc::new(Arc::new(SessionCapacity {
                hub: Arc::downgrade(self),
                limit,
            })),
            hub: self.clone(),
            session,
            limit,
        })
    }
}

impl SessionRoutes {
    pub fn reserve(&self) -> Result<SessionRoute, Error> {
        if self.hub.state.is_poisoned() {
            return Err(Error::InvalidState("completion_mailbox_lock_poisoned"));
        }
        // One reference belongs to this issuer. Every other local owner is a
        // live route, including synchronous work and both checkpoint contexts.
        if Rc::strong_count(&self.capacity) > self.limit {
            return Err(Error::Busy);
        }
        let slot = self
            .hub
            .next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                next.checked_add(1)
            })
            .map_err(|_| Error::CapacityExceeded)?;
        Ok(SessionRoute {
            route: OperationRoute {
                id: RequestId {
                    store: self.hub.store,
                    session: self.session,
                    slot,
                    generation: Generation(0),
                },
                registered: false,
                released: false,
                session_capacity: true,
            },
            capacity: self.capacity.clone(),
            owner: std::ptr::from_ref(self.hub.as_ref()).addr(),
        })
    }
}

impl SessionRoute {
    pub fn release(&mut self, hub: &CompletionHub) -> Result<(), Error> {
        if self.owner != std::ptr::from_ref(hub).addr() {
            return Err(Error::InvalidState("session route belongs to another hub"));
        }
        hub.release_operation(&mut self.route)
    }
    pub fn activate(&mut self, hub: &CompletionHub) -> Result<(), Error> {
        if self.owner != std::ptr::from_ref(hub).addr() {
            return Err(Error::InvalidState("session route belongs to another hub"));
        }
        hub.activate_with_capacity(&mut self.route, Some(self.capacity.as_ref().clone()))
    }
}

impl Deref for SessionRoute {
    type Target = OperationRoute;
    fn deref(&self) -> &Self::Target {
        &self.route
    }
}
impl Drop for SessionRoute {
    fn drop(&mut self) {
        // Normal task completion has already released the route. The fallback
        // handles an abandoned local owner without retaining a hub cycle.
        if !self.route.released
            && let Some(hub) = self.capacity.hub.upgrade()
        {
            let _ = hub.release_operation(&mut self.route);
        }
    }
}
