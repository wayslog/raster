//! Stable activation handles keep ordinary session bookkeeping independent.
//! Global state publication falls back to the registry before taking a slot.
use super::*;
use std::sync::Weak;

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) struct RegistrationHandle {
    owner: Weak<Control>,
    slot: Arc<SessionSlot>,
}

pub(crate) struct RegisteredSession {
    pub handle: RegistrationHandle,
    pub version: CheckpointVersion,
    pub last_accepted: Option<Serial>,
}

#[derive(Debug)]
pub(super) struct SlotState {
    pub active: bool,
    pub system: SystemState,
    pub last_accepted: Option<Serial>,
}

#[repr(align(128))]
#[derive(Debug)]
pub(super) struct SessionSlot {
    id: SessionId,
    state: crate::sync::Mutex<SlotState>,
}

struct PoisonNotification<'a> {
    state: &'a crate::sync::Mutex<SlotState>,
    failed: &'a AtomicBool,
}

impl Drop for PoisonNotification<'_> {
    fn drop(&mut self) {
        if self.state.is_poisoned() {
            self.failed.store(true, Ordering::SeqCst);
        }
    }
}

impl SessionSlot {
    pub(super) fn new(
        id: SessionId,
        active: bool,
        system: SystemState,
        last_accepted: Option<Serial>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            state: crate::sync::Mutex::new(SlotState {
                active,
                system,
                last_accepted,
            }),
        })
    }

    #[inline]
    pub(super) fn with_state<R>(
        &self,
        failed: &AtomicBool,
        use_state: impl FnOnce(&mut SlotState) -> Result<R, Error>,
    ) -> Result<R, Error> {
        // The actual guard drops first, preserving Mutex's distinction between
        // a new panic and cleanup during an already active unwind.
        let _notification = PoisonNotification {
            state: &self.state,
            failed,
        };
        let mut state = self.state.lock().map_err(|_| {
            failed.store(true, Ordering::SeqCst);
            Error::InvalidState("session registration lock poisoned")
        })?;
        if failed.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("coordinator failed closed"));
        }
        use_state(&mut state)
    }
}

struct StatePublication<'a> {
    control: &'a Control,
    committed: bool,
}

impl StatePublication<'_> {
    fn commit(mut self) {
        self.committed = true;
        self.control.publishing.store(false, Ordering::SeqCst);
    }
}

impl Drop for StatePublication<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Never expose a partly published version after an error or panic.
            self.control.failed.store(true, Ordering::SeqCst);
            self.control.publishing.store(false, Ordering::SeqCst);
        }
    }
}

impl Coordinator {
    pub(super) fn registered(
        &self,
        slot: &Arc<SessionSlot>,
        version: CheckpointVersion,
        last_accepted: Option<Serial>,
    ) -> RegisteredSession {
        RegisteredSession {
            handle: RegistrationHandle {
                owner: Arc::downgrade(&self.control),
                slot: slot.clone(),
            },
            version,
            last_accepted,
        }
    }

    #[cfg(test)]
    pub(super) fn registration(&self, id: SessionId) -> Result<RegistrationHandle, Error> {
        let registry = self.lock()?;
        let entry = registry
            .sessions
            .get(&id)
            .filter(|entry| entry.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        Ok(self
            .registered(&entry.slot, registry.system.version, None)
            .handle)
    }

    #[inline]
    fn with_registration<R>(
        &self,
        handle: &RegistrationHandle,
        id: SessionId,
        use_state: impl FnOnce(&mut SlotState) -> Result<R, Error>,
    ) -> Result<R, Error> {
        // Weak ownership keeps the control allocation from being reused while
        // a stale handle remains, without upgrading or cloning on each call.
        if !std::ptr::eq(handle.owner.as_ptr(), Arc::as_ptr(&self.control)) || handle.slot.id != id
        {
            return Err(Error::InvalidState("session registration owner mismatch"));
        }
        self.healthy()?;
        let mut use_state = Some(use_state);
        let attempt = handle.slot.with_state(&self.failed, |slot| {
            if self.publishing.load(Ordering::SeqCst) {
                return Ok(None);
            }
            // Check failure after publication: an interrupted publisher stores
            // failure before it clears publishing.
            self.healthy()?;
            if !slot.active {
                return Err(Error::InvalidState("session_not_registered"));
            }
            Ok(Some(use_state
                .take()
                .expect("registration operation exists")(
                slot
            )))
        })?;
        if let Some(result) = attempt {
            return result;
        }
        // The fast attempt released its slot before taking the global lock.
        // A publisher therefore never waits on a slot whose owner waits here.
        let _registry = self.lock()?;
        handle.slot.with_state(&self.failed, |slot| {
            self.healthy()?;
            if !slot.active {
                return Err(Error::InvalidState("session_not_registered"));
            }
            use_state
                .take()
                .expect("registration operation was deferred")(slot)
        })
    }

    #[inline]
    pub fn session_state(
        &self,
        handle: &RegistrationHandle,
        id: SessionId,
    ) -> Result<SystemState, Error> {
        self.with_registration(handle, id, |slot| Ok(slot.system))
    }

    #[inline]
    pub fn accept_registered(
        &self,
        handle: &RegistrationHandle,
        id: SessionId,
        serial: Serial,
        version: CheckpointVersion,
    ) -> Result<(), Error> {
        self.with_registration(handle, id, |slot| {
            if slot.system.phase == Phase::Failed {
                return Err(Error::InvalidState("storage_closed_or_failed"));
            }
            if slot.system.version != version {
                return Err(Error::Busy);
            }
            if slot.last_accepted.is_some_and(|last| serial <= last) {
                return Err(Error::InvalidState(
                    "operation_serial_must_increase_strictly",
                ));
            }
            slot.last_accepted = Some(serial);
            Ok(())
        })
    }

    /// The registry guard serializes publishers and fixes the active set.
    /// Fast access either finishes under its slot before that slot is changed,
    /// or releases the slot and waits for complete publication at the registry.
    pub(super) fn publish_system(
        &self,
        registry: &mut Registry,
        system: SystemState,
    ) -> Result<(), Error> {
        self.publishing.store(true, Ordering::SeqCst);
        let publication = StatePublication {
            control: &self.control,
            committed: false,
        };
        registry.system = system;
        for entry in registry.sessions.values().filter(|entry| entry.active) {
            entry.slot.with_state(&self.failed, |slot| {
                slot.system = system;
                Ok(())
            })?;
        }
        publication.commit();
        Ok(())
    }
}
