//! Session threads observe their registration and acknowledge context cuts.
//! Maintenance drivers remain responsible for material I/O and phase advancement.
use super::{Engine, SessionRuntime};
use crate::{
    coordination::{Phase, SystemState},
    schema::Schema,
    types::*,
};
use std::sync::atomic::Ordering;
impl<S: Schema> Engine<S> {
    pub(crate) fn observe_session(&self, session: &mut SessionRuntime) -> Result<bool, Error> {
        let _timer = self.metrics.timer(false);
        let mut changed = false;
        // Each observation has a fixed upper limit;When concurrent actions continue to change, let the caller continue in the next round.
        for _ in 0..8 {
            let state = self
                .coordinator
                .session_state(&session.registration, session.id)?;
            if self.failed.load(Ordering::SeqCst) {
                if state.id.is_none() {
                    return Ok(changed);
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
                Ok(()) => return Ok(changed),
                Err(error) => {
                    if self
                        .coordinator
                        .session_state(&session.registration, session.id)?
                        != state
                    {
                        continue;
                    }
                    if matches!(error, Error::Busy) {
                        return Ok(changed);
                    }
                    return Err(error);
                }
            }
        }
        Ok(changed)
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
