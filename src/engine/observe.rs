//! Session threads observe their registration and acknowledge context cuts.
//! Maintenance drivers remain responsible for material I/O and phase advancement.
use super::{Engine, SessionRuntime};
use crate::{
    coordination::{Phase, SystemState},
    schema::Schema,
    types::*,
};
use std::sync::atomic::Ordering;
struct EntryObservation {
    changed: bool,
    rest: bool,
}
impl<S: Schema> Engine<S> {
    #[inline]
    pub(crate) fn observe_session(&self, session: &mut SessionRuntime) -> Result<bool, Error> {
        self.observe_entry(session)
            .map(|observation| observation.changed)
    }
    /// Only for the initial Read call on its owning session thread. After a
    /// REST observation, a later checkpoint cannot pass Prepare until this
    /// session observes it. Do not observe again before completing the initial
    /// attempt or registering its suspended version.
    pub(super) fn observe_read_entry(&self, session: &mut SessionRuntime) -> Result<bool, Error> {
        self.observe_entry(session)
            .map(|observation| observation.rest)
    }
    fn observe_entry(&self, session: &mut SessionRuntime) -> Result<EntryObservation, Error> {
        let _timer = self.metrics.timer(false);
        let mut changed = false;
        // Each observation has a fixed upper limit;When concurrent actions continue to change, let the caller continue in the next round.
        for _ in 0..8 {
            let state = self
                .coordinator
                .session_state(&session.registration, session.id)?;
            if self.failed.load(Ordering::SeqCst) {
                if state.id.is_none() {
                    return Ok(EntryObservation {
                        changed,
                        rest: false,
                    });
                }
                if let Some(id) = state.id
                    && state.phase != Phase::Failed
                {
                    self.coordinator
                        .fail_action(id, Error::InvalidState("engine_failed_closed"))?;
                }
                return Err(Error::InvalidState("engine_failed_closed"));
            }
            if state.phase == Phase::Failed {
                self.failed.store(true, Ordering::SeqCst);
                return Err(Error::InvalidState("Coordinated action failed"));
            }
            let result = self.observe_at(session, state, &mut changed);
            match result {
                Ok(()) => {
                    return Ok(EntryObservation {
                        changed,
                        rest: state.phase == Phase::Rest,
                    });
                }
                Err(error) => {
                    if self
                        .coordinator
                        .session_state(&session.registration, session.id)?
                        != state
                    {
                        continue;
                    }
                    if matches!(error, Error::Busy) {
                        return Ok(EntryObservation {
                            changed,
                            rest: false,
                        });
                    }
                    return Err(error);
                }
            }
        }
        Ok(EntryObservation {
            changed,
            rest: false,
        })
    }
    fn observe_at(
        &self,
        session: &mut SessionRuntime,
        state: SystemState,
        changed: &mut bool,
    ) -> Result<(), Error> {
        let cut_version = match state.phase {
            Phase::InProgress | Phase::WaitPending => {
                *changed |= session.switch_version(state.version)?;
                Some(CheckpointVersion(
                    state
                        .version
                        .0
                        .checked_sub(1)
                        .ok_or(Error::InvalidState("Old version does not exist"))?,
                ))
            }
            Phase::PrepareIndex | Phase::Prepare | Phase::GrowPrepare => Some(state.version),
            _ => None,
        };
        if session.current.version != state.version {
            return Err(Error::InvalidState(
                "The session has not yet observed the current action version",
            ));
        }
        if let Some(version) = cut_version {
            self.coordinator.acknowledge(
                state
                    .id
                    .ok_or(Error::InvalidState("Stage is missing action identifier"))?,
                session.cut(version)?,
                state.phase,
            )?;
        }
        Ok(())
    }
}
