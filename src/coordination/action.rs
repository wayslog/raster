//! Action state and session registration share the same lock;Stage confirmation is not equivalent to material persistence.
use super::*;
use std::sync::Arc;
#[derive(Default)]
pub(super) struct Participant {
    departed: bool,
    acknowledged: Option<Phase>,
    cut: Option<SessionCut>,
}
pub(super) struct ActiveAction {
    id: MaintenanceId,
    pub participants: BTreeMap<SessionId, Participant>,
    pub failure: Option<Arc<Error>>,
    completed: Vec<SessionCut>,
}
fn barrier(phase: Phase) -> bool {
    matches!(
        phase,
        Phase::PrepareIndex
            | Phase::Prepare
            | Phase::InProgress
            | Phase::WaitPending
            | Phase::GrowPrepare
    )
}
fn next_phase(action: Action, phase: Phase) -> Option<Phase> {
    use Action::*;
    use Phase::*;
    match (action, phase) {
        (CheckpointFull | CheckpointIndex, PrepareIndex) => Some(IndexSnapshot),
        (CheckpointFull, IndexSnapshot) => Some(Prepare),
        (CheckpointIndex, IndexSnapshot) => Some(WaitFlush),
        (CheckpointFull | CheckpointLog, Prepare) => Some(InProgress),
        (CheckpointFull | CheckpointLog, InProgress) => Some(WaitPending),
        (CheckpointFull | CheckpointLog, WaitPending) => Some(WaitFlush),
        (CheckpointFull | CheckpointIndex | CheckpointLog | Recover, WaitFlush) => Some(Publish),
        (Gc, GcIo) => Some(GcIndex),
        (Gc, GcIndex) => Some(Publish),
        (GrowIndex, GrowPrepare) => Some(GrowCopy),
        (GrowIndex, GrowCopy) => Some(Publish),
        (Compact, Compacting) => Some(Publish),
        (ReleaseCheckpoint, ReclaimCheckpoint) => Some(Publish),
        _ => None,
    }
}
impl Coordinator {
    pub fn snapshot(&self) -> Result<SystemState, Error> {
        Ok(self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?
            .system)
    }
    pub fn start_action(&self, kind: Action) -> Result<MaintenanceId, Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        if registry.closed || registry.system.phase == Phase::Failed {
            return Err(Error::InvalidState(
                "Storage is closed or coordination action failed",
            ));
        }
        if registry.action.is_some()
            || kind == Action::Recover && registry.sessions.values().any(|s| s.active)
        {
            return Err(Error::Busy);
        }
        let next = registry
            .next_action
            .checked_add(1)
            .ok_or(Error::CapacityExceeded)?;
        let id = MaintenanceId(registry.next_action);
        // Sessions that are active but have no business must also participate;The number of operations must not be used to infer exit.
        let participants = registry
            .sessions
            .iter()
            .filter(|(_, s)| s.active)
            .map(|(id, _)| (*id, Participant::default()))
            .collect();
        let completed = registry
            .sessions
            .iter()
            .filter(|(_, s)| !s.active)
            .map(|(id, s)| SessionCut {
                session: *id,
                last_accepted: s.last_accepted,
                old_pending: 0,
            })
            .collect();
        registry.action = Some(ActiveAction {
            id,
            participants,
            failure: None,
            completed,
        });
        registry.next_action = next;
        registry.system.id = Some(id);
        registry.system.action = Some(kind);
        registry.system.phase = match kind {
            Action::CheckpointFull | Action::CheckpointIndex => Phase::PrepareIndex,
            Action::CheckpointLog => Phase::Prepare,
            Action::Recover => Phase::WaitFlush,
            Action::Gc => Phase::GcIo,
            Action::GrowIndex => Phase::GrowPrepare,
            Action::Compact => Phase::Compacting,
            Action::ReleaseCheckpoint => Phase::ReclaimCheckpoint,
        };
        Ok(id)
    }
    /// stages and actions ID must all match;Repeated confirmation can only maintain the same split,The old serial number cannot be tampered with.
    pub fn acknowledge(
        &self,
        id: MaintenanceId,
        cut: SessionCut,
        phase: Phase,
    ) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        if registry.system.phase != phase || !barrier(phase) {
            return Err(Error::InvalidState("Confirm phase mismatch"));
        }
        let registered = registry
            .sessions
            .get(&cut.session)
            .filter(|s| s.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        if cut.last_accepted > registered.last_accepted {
            return Err(Error::InvalidState(
                "Session split exceeds accepted sequence number",
            ));
        }
        if phase == Phase::WaitPending && cut.old_pending != 0 {
            return Err(Error::Busy);
        }
        let action = registry
            .action
            .as_mut()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("maintenance_action_mismatch"))?;
        let participant = action
            .participants
            .get_mut(&cut.session)
            .ok_or(Error::InvalidState(
                "Session does not belong to stage participation collection",
            ))?;
        if matches!(phase, Phase::InProgress | Phase::WaitPending) {
            if let Some(old) = participant.cut {
                if old.last_accepted != cut.last_accepted || cut.old_pending > old.old_pending {
                    return Err(Error::InvalidState("Session old version sharding changed"));
                }
            } else if phase == Phase::WaitPending {
                return Err(Error::InvalidState(
                    "The session has not registered version split",
                ));
            }
            participant.cut = Some(cut);
        }
        participant.acknowledged = Some(phase);
        Ok(())
    }
    /// Called after the driver has completed the actual work in this phase;This method only verifies status and participant barriers.
    pub fn advance(&self, id: MaintenanceId, expected: Phase) -> Result<SystemState, Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        if registry.system.phase != expected {
            return Err(Error::InvalidState("Advance phase mismatch"));
        }
        let action = registry
            .action
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("maintenance_action_mismatch"))?;
        if barrier(expected)
            && action
                .participants
                .values()
                .any(|p| !p.departed && p.acknowledged != Some(expected))
        {
            return Err(Error::Busy);
        }
        let next = next_phase(
            registry
                .system
                .action
                .ok_or(Error::InvalidState("No maintenance action"))?,
            expected,
        )
        .ok_or(Error::InvalidState("Stages cannot be advanced directly"))?;
        if next == Phase::InProgress {
            registry.system.version = CheckpointVersion(
                registry
                    .system
                    .version
                    .0
                    .checked_add(1)
                    .ok_or(Error::CapacityExceeded)?,
            );
        }
        registry.system.phase = next;
        Ok(registry.system)
    }
    /// Publish After the persistence results are confirmed by the actual maintenance driver,to release the action occupation.
    pub fn finish_action(&self, id: MaintenanceId) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        if registry.system.phase != Phase::Publish
            || registry.action.as_ref().is_none_or(|a| a.id != id)
        {
            return Err(Error::InvalidState(
                "Maintenance action has not yet reached the release stage",
            ));
        }
        registry.action = None;
        registry.system.id = None;
        registry.system.action = None;
        registry.system.phase = Phase::Rest;
        Ok(())
    }
    pub fn fail_action(&self, id: MaintenanceId, cause: Error) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        let action = registry
            .action
            .as_mut()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("maintenance_action_mismatch"))?;
        action.failure.get_or_insert_with(|| Arc::new(cause));
        registry.system.phase = Phase::Failed;
        Ok(())
    }
    #[cfg(test)]
    pub fn action_failure(&self, id: MaintenanceId) -> Result<Option<Arc<Error>>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        Ok(registry
            .action
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("maintenance_action_mismatch"))?
            .failure
            .clone())
    }
    /// Called only after the thread owning the session has drained both contexts;Exiting participants retains shards without deleting them.
    pub fn leave_drained(
        &self,
        current: (CheckpointVersion, SessionCut),
        previous: Option<(CheckpointVersion, SessionCut)>,
    ) -> Result<(), Error> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        let session = current.1.session;
        if current.1.old_pending != 0
            || previous.is_some_and(|(_, cut)| cut.old_pending != 0 || cut.session != session)
        {
            return Err(Error::Busy);
        }
        let registered = registry
            .sessions
            .get(&session)
            .filter(|s| s.active)
            .ok_or(Error::InvalidState("session_not_registered"))?;
        if current.1.last_accepted != registered.last_accepted {
            return Err(Error::InvalidState(
                "Accepted sequence number mismatch for closed session",
            ));
        }
        let system = registry.system;
        if let Some(action) = &mut registry.action {
            let participant = action
                .participants
                .get_mut(&session)
                .ok_or(Error::InvalidState(
                    "Session does not belong to action participation set",
                ))?;
            if action.failure.is_none() {
                let version =
                    if matches!(
                        system.action,
                        Some(Action::CheckpointFull | Action::CheckpointLog)
                    ) && matches!(
                        system.phase,
                        Phase::InProgress | Phase::WaitPending | Phase::WaitFlush | Phase::Publish
                    ) {
                        CheckpointVersion(
                            system.version.0.checked_sub(1).ok_or(Error::InvalidState(
                                "Checkpoint old version does not exist",
                            ))?,
                        )
                    } else {
                        system.version
                    };
                let cut = if current.0 == version {
                    current.1
                } else {
                    previous
                        .filter(|(v, _)| *v == version)
                        .ok_or(Error::InvalidState(
                            "Closing session is missing old version sharding",
                        ))?
                        .1
                };
                if participant
                    .cut
                    .is_some_and(|old| old.last_accepted != cut.last_accepted)
                {
                    return Err(Error::InvalidState("Close session changes fixed sharding"));
                }
                participant.cut = Some(cut);
            }
            participant.departed = true;
        }
        registry
            .sessions
            .get_mut(&session)
            .expect("Verified session exists")
            .active = false;
        Ok(())
    }
    pub fn cuts(&self, id: MaintenanceId) -> Result<Vec<SessionCut>, Error> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| Error::InvalidState("Session registry lock poisoning"))?;
        let action = registry
            .action
            .as_ref()
            .filter(|a| a.id == id)
            .ok_or(Error::InvalidState("maintenance_action_mismatch"))?;
        let mut cuts = action.completed.clone();
        for participant in action.participants.values() {
            cuts.push(participant.cut.ok_or(Error::Busy)?);
        }
        cuts.sort_unstable_by_key(|cut| cut.session);
        Ok(cuts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cut(session: SessionId, old_pending: usize) -> SessionCut {
        SessionCut {
            session,
            last_accepted: Some(Serial(7)),
            old_pending,
        }
    }
    #[test]
    fn accept_serial_numbers_and_version_verification_atomically_and_reject_without_consuming_serial_numbers()
     {
        let c = Coordinator::new(1).unwrap();
        let session = SessionId([1; 16]);
        c.enroll(session).unwrap();
        c.accept_serial(session, Serial(7), CheckpointVersion(0))
            .unwrap();
        let id = c.start_action(Action::CheckpointLog).unwrap();
        c.acknowledge(id, cut(session, 0), Phase::Prepare).unwrap();
        c.advance(id, Phase::Prepare).unwrap();
        for version in [0, 2] {
            assert!(matches!(
                c.accept_serial(session, Serial(9), CheckpointVersion(version)),
                Err(Error::Busy)
            ));
            assert_eq!(c.last_accepted(session).unwrap(), Some(Serial(7)));
        }
        c.accept_serial(session, Serial(9), CheckpointVersion(1))
            .unwrap();
        c.acknowledge(id, cut(session, 0), Phase::InProgress)
            .unwrap();
        assert_eq!(c.cuts(id).unwrap()[0].last_accepted, Some(Serial(7)));
    }
    #[test]
    fn the_complete_action_is_confirmed_step_by_step_and_old_requests_cannot_be_released_until_they_are_drained()
     {
        let c = Coordinator::new(2).unwrap();
        let sessions = [SessionId([1; 16]), SessionId([2; 16])];
        for session in sessions {
            c.enroll(session).unwrap();
            c.accept_serial(session, Serial(7), CheckpointVersion(0))
                .unwrap();
        }
        let id = c.start_action(Action::CheckpointFull).unwrap();
        assert!(matches!(c.start_action(Action::Gc), Err(Error::Busy)));
        assert!(matches!(c.enroll(SessionId([3; 16])), Err(Error::Busy)));
        assert!(c.finish_action(id).is_err());
        for phase in [Phase::PrepareIndex, Phase::Prepare, Phase::InProgress] {
            assert!(matches!(c.advance(id, phase), Err(Error::Busy)));
            c.acknowledge(id, cut(sessions[0], 1), phase).unwrap();
            assert!(matches!(c.advance(id, phase), Err(Error::Busy)));
            c.acknowledge(id, cut(sessions[1], 1), phase).unwrap();
            c.advance(id, phase).unwrap();
            if phase == Phase::PrepareIndex {
                c.advance(id, Phase::IndexSnapshot).unwrap();
            }
        }
        assert_eq!(c.snapshot().unwrap().version, CheckpointVersion(1));
        assert!(matches!(
            c.acknowledge(id, cut(sessions[0], 1), Phase::WaitPending),
            Err(Error::Busy)
        ));
        let mut invalid = cut(sessions[0], 0);
        invalid.last_accepted = Some(Serial(6));
        assert!(c.acknowledge(id, invalid, Phase::WaitPending).is_err());
        for session in sessions {
            c.acknowledge(id, cut(session, 0), Phase::WaitPending)
                .unwrap();
        }
        assert_eq!(c.cuts(id).unwrap(), sessions.map(|s| cut(s, 0)));
        c.advance(id, Phase::WaitPending).unwrap();
        c.advance(id, Phase::WaitFlush).unwrap();
        c.finish_action(id).unwrap();
        assert_eq!(c.snapshot().unwrap().phase, Phase::Rest);
        let next = c.start_action(Action::CheckpointLog).unwrap();
        assert_ne!(next, id);
        assert!(c.fail_action(id, Error::SessionAbandoned).is_err());
        assert!(
            c.acknowledge(id, cut(sessions[0], 0), Phase::Prepare)
                .is_err()
        );
    }
    #[test]
    fn index_only_actions_do_not_increment_log_versions_and_recovery_requires_no_active_sessions() {
        let c = Coordinator::new(1).unwrap();
        let session = SessionId([1; 16]);
        c.enroll(session).unwrap();
        assert!(matches!(c.start_action(Action::Recover), Err(Error::Busy)));
        c.leave(session).unwrap();
        let id = c.start_action(Action::CheckpointIndex).unwrap();
        for phase in [Phase::PrepareIndex, Phase::IndexSnapshot, Phase::WaitFlush] {
            c.advance(id, phase).unwrap();
        }
        assert_eq!(c.snapshot().unwrap().version, CheckpointVersion(0));
        c.finish_action(id).unwrap();
        let recovery = c.start_action(Action::Recover).unwrap();
        assert!(matches!(c.shutdown(), Err(Error::Busy)));
        c.advance(recovery, Phase::WaitFlush).unwrap();
        c.finish_action(recovery).unwrap();
        c.shutdown().unwrap();
    }
    #[test]
    fn abandoning_participation_in_the_session_during_the_action_retains_the_first_failed_action_and_prohibits_new_successful_actions()
     {
        let c = Coordinator::new(1).unwrap();
        let session = SessionId([1; 16]);
        c.enroll(session).unwrap();
        let id = c.start_action(Action::CheckpointLog).unwrap();
        c.leave(session).unwrap();
        assert!(matches!(
            &*c.action_failure(id).unwrap().unwrap(),
            Error::SessionAbandoned
        ));
        c.fail_action(id, Error::Codec("second mistake")).unwrap();
        assert!(matches!(
            &*c.action_failure(id).unwrap().unwrap(),
            Error::SessionAbandoned
        ));
        assert!(c.start_action(Action::CheckpointFull).is_err());
        assert!(c.enroll(session).is_err());
        assert!(c.finish_action(id).is_err());
        c.shutdown().unwrap();
    }
    #[test]
    fn registration_and_stage_start_competition_will_not_miss_participants() {
        for _ in 0..32 {
            let c = Coordinator::new(1).unwrap();
            let barrier = std::sync::Barrier::new(2);
            let session = SessionId([1; 16]);
            let (enroll, id) = std::thread::scope(|scope| {
                let registration = scope.spawn(|| {
                    barrier.wait();
                    c.enroll(session)
                });
                let action = scope.spawn(|| {
                    barrier.wait();
                    c.start_action(Action::CheckpointLog).unwrap()
                });
                (registration.join().unwrap(), action.join().unwrap())
            });
            let first = c.advance(id, Phase::Prepare);
            if enroll.is_ok() {
                assert!(matches!(first, Err(Error::Busy)));
                c.acknowledge(
                    id,
                    SessionCut {
                        session,
                        last_accepted: None,
                        old_pending: 0,
                    },
                    Phase::Prepare,
                )
                .unwrap();
                c.advance(id, Phase::Prepare).unwrap();
            } else {
                assert!(matches!(enroll, Err(Error::Busy)));
                first.unwrap();
            }
        }
    }
    #[test]
    fn the_version_is_exhausted_and_refuses_to_advance_and_remains_in_its_original_state() {
        let c = Coordinator::new(1).unwrap();
        c.registry.lock().unwrap().system.version = CheckpointVersion(u64::MAX);
        let id = c.start_action(Action::CheckpointLog).unwrap();
        let before = c.snapshot().unwrap();
        assert!(matches!(
            c.advance(id, Phase::Prepare),
            Err(Error::CapacityExceeded)
        ));
        assert_eq!(c.snapshot().unwrap(), before);
    }
}
