//! Follow-up to compound compression:Checkpoint first and then truncate in upstream order,Does not hold copy action waiting for subtask.
use super::*;
use crate::api::maintenance::CheckpointKind;

pub(super) enum Stage {
    Copy,
    StartCheckpoint,
    Checkpoint(MaintenanceTicket<CheckpointReport>),
    StartGc,
    Gc(MaintenanceTicket<GcReport>),
    Report,
}
impl Job {
    /// If the shutdown fails, the published subtask results will also be collected first.,Cannot lose a successful checkpoint or GC true error.
    pub(super) fn collect_child(&mut self) -> Result<bool, Error> {
        match &mut self.stage {
            Stage::Checkpoint(ticket) => {
                let Some(report) = ticket.take_owned_report()? else {
                    return Ok(false);
                };
                self.stage = if self.options.shift_begin {
                    Stage::StartGc
                } else {
                    Stage::Report
                };
                match report {
                    Ok(report) => self.checkpoint = Some(report),
                    Err(cause) => self.fail(cause),
                }
            }
            Stage::Gc(ticket) => {
                let Some(report) = ticket.take_owned_report()? else {
                    return Ok(false);
                };
                self.stage = Stage::Report;
                match report {
                    Ok(report) => self.gc = Some(report),
                    Err(cause) => self.fail(cause),
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
    pub(super) fn follow_up<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        if self.failure.is_some() {
            self.publish_report()?;
            return Ok(true);
        }
        match &self.stage {
            Stage::StartCheckpoint => match engine.start_checkpoint(CheckpointKind::Full) {
                Ok(ticket) => self.stage = Stage::Checkpoint(ticket),
                Err(Error::Busy) => return Ok(false),
                Err(error) => return Err(error),
            },
            Stage::StartGc => match engine.start_gc(self.options.until) {
                Ok(ticket) => self.stage = Stage::Gc(ticket),
                Err(Error::Busy) => return Ok(false),
                Err(error) => return Err(error),
            },
            Stage::Checkpoint(_) | Stage::Gc(_) => return self.collect_child(),
            Stage::Report => self.publish_report()?,
            Stage::Copy => {
                return Err(Error::InvalidState(
                    "Subsequent actions cannot be performed during the copy phase.",
                ));
            }
        }
        Ok(true)
    }
}
