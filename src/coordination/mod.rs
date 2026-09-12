//! session registration,Dual-version context and top-level action arbitration;and safe recycling epoch separate.
use crate::types::*;
use std::collections::BTreeMap;
mod action;
use action::ActiveAction;

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
    last_accepted: Option<Serial>,
    recovered: Option<(Serial, CheckpointVersion)>,
}
struct Registry {
    system: SystemState,
    action: Option<ActiveAction>,
    next_action: u64,
    sessions: BTreeMap<SessionId, Registration>,
    closed: bool,
}
pub(crate) struct Coordinator {
    registry: crate::sync::Mutex<Registry>,
    max_sessions: usize,
}
impl Coordinator {
    pub fn active_session_ids(&self) -> Result<Vec<SessionId>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
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
        })
    }
    pub fn from_checkpoint(
        max_sessions: usize,
        version: CheckpointVersion,
        sessions: &[(SessionId, Serial)],
    ) -> Result<Self, Error> {
        let mut coordinator = Self::new(max_sessions)?;
        let registry = coordinator
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
                        last_accepted: Some(serial),
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
    pub fn resume(
        &self,
        session: SessionId,
    ) -> Result<(CheckpointVersion, Serial, CheckpointVersion), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
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
        let version = registry.system.version;
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
        entry.active = true;
        Ok((version, serial, durable_version))
    }
    pub fn last_accepted(&self, id: SessionId) -> Result<Option<Serial>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        let entry = registry
            .sessions
            .get(&id)
            .filter(|e| e.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        Ok(entry.last_accepted)
    }
    /// Only called after other rejection conditions have been checked;Refuse to keep the original serial number.
    pub fn accept_serial(
        &self,
        id: SessionId,
        serial: Serial,
        version: CheckpointVersion,
    ) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState(
                "Storage is closed or coordination action failed",
            ));
        }
        if version != registry.system.version {
            return Err(Error::Busy);
        }
        let entry = registry
            .sessions
            .get_mut(&id)
            .filter(|e| e.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        if entry.last_accepted.is_some_and(|last| serial <= last) {
            return Err(Error::InvalidState(
                "operation_serial_must_increase_strictly",
            ));
        }
        entry.last_accepted = Some(serial);
        Ok(())
    }
    pub fn shutdown(&self) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        if registry.sessions.values().any(|e| e.active) {
            return Err(Error::Busy);
        }
        if registry.action.is_some() && registry.system.phase != Phase::Failed {
            return Err(Error::Busy);
        }
        registry.closed = true;
        Ok(())
    }

    pub fn enroll(&self, session: SessionId) -> Result<CheckpointVersion, Error> {
        session.validate()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
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
        if registry.sessions.values().filter(|e| e.active).count() >= self.max_sessions {
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
        registry
            .sessions
            .entry(session)
            .and_modify(|entry| entry.active = true)
            .or_insert(Registration {
                active: true,
                last_accepted: None,
                recovered: None,
            });
        Ok(registry.system.version)
    }
    pub fn leave(&self, session: SessionId) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        let entry = registry
            .sessions
            .get_mut(&session)
            .filter(|e| e.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        entry.active = false;
        if let Some(action) = &mut registry.action
            && action.participants.contains_key(&session)
        {
            action
                .failure
                .get_or_insert_with(|| std::sync::Arc::new(Error::SessionAbandoned));
            registry.system.phase = Phase::Failed;
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
