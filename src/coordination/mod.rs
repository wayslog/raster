//! Session registration, local admission, and global action/version barriers.
//! Access epochs remain independent from checkpoint coordination.
use crate::types::*;
use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
mod action;
#[cfg(test)]
mod admission_tests;
use action::ActiveAction;
mod session;
use session::SessionSlot;
pub(crate) use session::{RegisteredSession, RegistrationHandle};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    CheckpointFull,
    CheckpointIndex,
    CheckpointLog,
    Recover,
    Gc,
    GrowIndex,
    Compact,
    ReleaseCheckpoint,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Rest,
    PrepareIndex,
    IndexSnapshot,
    Prepare,
    InProgress,
    WaitPending,
    WaitFlush,
    Publish,
    GcIo,
    GcIndex,
    GrowPrepare,
    GrowCopy,
    Compacting,
    ReclaimCheckpoint,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SystemState {
    pub id: Option<MaintenanceId>,
    pub action: Option<Action>,
    pub phase: Phase,
    pub version: CheckpointVersion,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionCut {
    pub session: SessionId,
    pub last_accepted: Option<Serial>,
    pub old_pending: usize,
}
struct Registration {
    active: bool,
    slot: Arc<SessionSlot>,
    recovered: Option<(Serial, CheckpointVersion)>,
}
impl Registration {
    fn last(&self, failed: &AtomicBool) -> Result<Option<Serial>, Error> {
        self.slot.with_state(failed, |slot| Ok(slot.last_accepted))
    }
}
struct Registry {
    system: SystemState,
    action: Option<ActiveAction>,
    next_action: u64,
    sessions: BTreeMap<SessionId, Registration>,
    closed: bool,
}
// Handles retain weak ownership of this allocation to prevent address reuse.
// The registry remains the authority for action arbitration and active sets.
pub(crate) struct Control {
    registry: crate::sync::Mutex<Registry>,
    max_sessions: usize,
    failed: AtomicBool,
    publishing: AtomicBool,
}
pub(crate) struct Coordinator {
    control: Arc<Control>,
}
impl Deref for Coordinator {
    type Target = Control;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.control
    }
}
impl Coordinator {
    #[inline]
    fn healthy(&self) -> Result<(), Error> {
        if self.failed.load(Ordering::SeqCst) || self.registry.is_poisoned() {
            return Err(Error::InvalidState("coordinator failed closed"));
        }
        Ok(())
    }
    fn lock(&self) -> Result<crate::sync::MutexGuard<'_, Registry>, Error> {
        self.healthy()?;
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        self.healthy()?;
        Ok(registry)
    }
    pub fn active_session_ids(&self) -> Result<Vec<SessionId>, Error> {
        let registry = self.lock()?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(
            registry
                .sessions
                .values()
                .filter(|entry| entry.active)
                .count(),
        )
        .map_err(|_| Error::OutOfMemory)?;
        ids.extend(
            registry
                .sessions
                .iter()
                .filter_map(|(&id, entry)| entry.active.then_some(id)),
        );
        Ok(ids)
    }
    pub fn new(max_sessions: usize) -> Result<Self, Error> {
        if max_sessions == 0 {
            return Err(Error::InvalidConfig {
                field: "session.max_sessions",
                reason: "Session capacity must be non-zero",
            });
        }
        Ok(Self {
            control: Arc::new(Control {
                registry: crate::sync::Mutex::new(Registry {
                    system: SystemState {
                        id: None,
                        action: None,
                        phase: Phase::Rest,
                        version: CheckpointVersion(0),
                    },
                    action: None,
                    next_action: 0,
                    sessions: BTreeMap::new(),
                    closed: false,
                }),
                max_sessions,
                failed: AtomicBool::new(false),
                publishing: AtomicBool::new(false),
            }),
        })
    }
    pub fn from_checkpoint(
        max_sessions: usize,
        version: CheckpointVersion,
        sessions: &[(SessionId, Serial)],
    ) -> Result<Self, Error> {
        let mut coordinator = Self::new(max_sessions)?;
        let registry = Arc::get_mut(&mut coordinator.control)
            .expect("new coordinator has no handles")
            .registry
            .get_mut()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        registry.system.version =
            CheckpointVersion(version.0.checked_add(1).ok_or(Error::CapacityExceeded)?);
        for &(id, serial) in sessions {
            id.validate()?;
            if registry
                .sessions
                .insert(
                    id,
                    Registration {
                        active: false,
                        slot: SessionSlot::new(id, false, registry.system, Some(serial)),
                        recovered: Some((serial, version)),
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidFormat("Resume session duplication"));
            }
        }
        Ok(coordinator)
    }
    pub fn resume_registered(
        &self,
        session: SessionId,
    ) -> Result<(RegisteredSession, Serial, CheckpointVersion), Error> {
        let mut registry = self.lock()?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState("storage_closed_or_failed"));
        }
        if registry.action.is_some() {
            return Err(Error::Busy);
        }
        if registry
            .sessions
            .values()
            .filter(|entry| entry.active)
            .count()
            >= self.max_sessions
        {
            return Err(Error::CapacityExceeded);
        }
        let system = registry.system;
        let entry = registry
            .sessions
            .get_mut(&session)
            .ok_or(Error::InvalidState(
                "There is no recovery progress for this session",
            ))?;
        if entry.active {
            return Err(Error::Busy);
        }
        let (serial, durable_version) = entry.recovered.ok_or(Error::InvalidState(
            "The session does not belong to this recovery set",
        ))?;
        let last = entry.last(&self.failed)?;
        // Each activation has a fresh allocation. Retained handles keep the old,
        // inactive allocation and can never act for a resumed session.
        let slot = SessionSlot::new(session, true, system, last);
        let registered = self.registered(&slot, system.version, last);
        entry.slot = slot;
        entry.active = true;
        Ok((registered, serial, durable_version))
    }
    #[cfg(test)]
    pub fn resume(
        &self,
        session: SessionId,
    ) -> Result<(CheckpointVersion, Serial, CheckpointVersion), Error> {
        let (registered, serial, durable) = self.resume_registered(session)?;
        Ok((registered.version, serial, durable))
    }
    #[cfg(test)]
    pub fn last_accepted(&self, id: SessionId) -> Result<Option<Serial>, Error> {
        let registry = self.lock()?;
        registry
            .sessions
            .get(&id)
            .filter(|entry| entry.active)
            .ok_or(Error::InvalidState("session_not_registered"))?
            .last(&self.failed)
    }
    #[cfg(test)]
    pub fn accept_serial(
        &self,
        id: SessionId,
        serial: Serial,
        version: CheckpointVersion,
    ) -> Result<(), Error> {
        self.accept_registered(&self.registration(id)?, id, serial, version)
    }
    pub fn shutdown(&self) -> Result<(), Error> {
        let mut registry = self.lock()?;
        if registry.sessions.values().any(|entry| entry.active) {
            return Err(Error::Busy);
        }
        if registry.action.is_some() && registry.system.phase != Phase::Failed {
            return Err(Error::Busy);
        }
        registry.closed = true;
        Ok(())
    }
    pub fn enroll_registered(&self, session: SessionId) -> Result<RegisteredSession, Error> {
        session.validate()?;
        let mut registry = self.lock()?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState(
                "Storage is closed or coordination action failed",
            ));
        }
        if registry.action.is_some() {
            return Err(Error::Busy);
        }
        if registry
            .sessions
            .get(&session)
            .is_some_and(|entry| entry.active)
        {
            return Err(Error::Busy);
        }
        if registry
            .sessions
            .values()
            .filter(|entry| entry.active)
            .count()
            >= self.max_sessions
        {
            return Err(Error::CapacityExceeded);
        }
        if registry
            .sessions
            .get(&session)
            .is_some_and(|entry| entry.recovered.is_some())
        {
            return Err(Error::InvalidState(
                "Persistent sessions must pass continue_session restore",
            ));
        }
        let last = registry
            .sessions
            .get(&session)
            .map(|entry| entry.last(&self.failed))
            .transpose()?
            .flatten();
        let slot = SessionSlot::new(session, true, registry.system, last);
        let registered = self.registered(&slot, registry.system.version, last);
        registry.sessions.insert(
            session,
            Registration {
                active: true,
                slot,
                recovered: None,
            },
        );
        Ok(registered)
    }
    #[cfg(test)]
    pub fn enroll(&self, session: SessionId) -> Result<CheckpointVersion, Error> {
        self.enroll_registered(session)
            .map(|registered| registered.version)
    }
    pub fn leave(&self, session: SessionId) -> Result<(), Error> {
        let mut registry = self.lock()?;
        let entry = registry
            .sessions
            .get_mut(&session)
            .filter(|entry| entry.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        entry.slot.with_state(&self.failed, |slot| {
            slot.active = false;
            Ok(())
        })?;
        entry.active = false;
        if let Some(action) = &mut registry.action
            && action.participants.contains_key(&session)
        {
            action
                .failure
                .get_or_insert_with(|| Arc::new(Error::SessionAbandoned));
            let mut system = registry.system;
            system.phase = Phase::Failed;
            self.publish_system(&mut registry, system)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_progress_registration_adheres_to_capacity_and_rejects_duplicate_identities_and_version_exhaustion()
     {
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        assert!(Coordinator::from_checkpoint(1, CheckpointVersion(u64::MAX), &[]).is_err());
        assert!(
            Coordinator::from_checkpoint(
                1,
                CheckpointVersion(3),
                &[(a, Serial(7)), (a, Serial(9))]
            )
            .is_err()
        );
        let c = Coordinator::from_checkpoint(
            1,
            CheckpointVersion(3),
            &[(a, Serial(7)), (b, Serial(19))],
        )
        .unwrap();
        assert!(c.enroll(a).is_err());
        assert_eq!(
            c.resume(a).unwrap(),
            (CheckpointVersion(4), Serial(7), CheckpointVersion(3))
        );
        assert!(c.resume(b).is_err());
        assert!(c.accept_serial(a, Serial(7), CheckpointVersion(4)).is_err());
        c.accept_serial(a, Serial(11), CheckpointVersion(4))
            .unwrap();
        c.leave(a).unwrap();
        assert_eq!(
            c.resume(b).unwrap(),
            (CheckpointVersion(4), Serial(19), CheckpointVersion(3))
        );
    }
    #[test]
    fn duplicate_registration_capacity_and_shutdown_rejection_do_not_change_status() {
        let c = Coordinator::new(1).unwrap();
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        c.enroll(a).unwrap();
        assert!(matches!(c.enroll(a), Err(Error::Busy)));
        assert!(matches!(c.enroll(b), Err(Error::CapacityExceeded)));
        assert!(matches!(c.shutdown(), Err(Error::Busy)));
        c.accept_serial(a, Serial(7), CheckpointVersion(0)).unwrap();
        c.leave(a).unwrap();
        c.enroll(b).unwrap();
        c.leave(b).unwrap();
        c.shutdown().unwrap();
        c.shutdown().unwrap();
        assert!(c.enroll(a).is_err());
    }
    #[test]
    fn the_serial_number_can_be_jumped_but_cannot_be_rolled_back_and_re_registration_will_preserve_the_progress()
     {
        let c = Coordinator::new(2).unwrap();
        let a = SessionId([1; 16]);
        let b = SessionId([2; 16]);
        c.enroll(a).unwrap();
        c.enroll(b).unwrap();
        for n in [0, 7, 19] {
            c.accept_serial(a, Serial(n), CheckpointVersion(0)).unwrap();
        }
        for n in [0, 18, 19] {
            assert!(c.accept_serial(a, Serial(n), CheckpointVersion(0)).is_err());
            assert_eq!(c.last_accepted(a).unwrap(), Some(Serial(19)));
        }
        assert_eq!(c.last_accepted(b).unwrap(), None);
        c.leave(a).unwrap();
        assert!(
            c.accept_serial(a, Serial(20), CheckpointVersion(0))
                .is_err()
        );
        c.enroll(a).unwrap();
        assert_eq!(c.last_accepted(a).unwrap(), Some(Serial(19)));
        c.accept_serial(a, Serial(20), CheckpointVersion(0))
            .unwrap();
    }
    #[test]
    fn registration_and_closing_competitions_have_the_same_final_state() {
        for _ in 0..32 {
            let c = Coordinator::new(1).unwrap();
            let barrier = std::sync::Barrier::new(2);
            let id = SessionId([1; 16]);
            let (enroll, shutdown) = std::thread::scope(|s| {
                let a = s.spawn(|| {
                    barrier.wait();
                    c.enroll(id)
                });
                let b = s.spawn(|| {
                    barrier.wait();
                    c.shutdown()
                });
                (a.join().unwrap(), b.join().unwrap())
            });
            assert_ne!(enroll.is_ok(), shutdown.is_ok());
            if enroll.is_ok() {
                c.leave(id).unwrap();
                c.shutdown().unwrap();
            }
            assert!(c.enroll(id).is_err());
        }
    }
}
