//! Wait to advance only accepted tickets;Reporting failure retains the original error and some effective information.
use raster::{
    RasterKV, Session, Submission, Ticket,
    api::{
        Outcome,
        maintenance::{MaintenanceTicket, SharedReport},
    },
    schema::Schema,
    types::*,
};
use std::{
    fmt,
    time::{Duration, Instant},
};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(60))
}
fn slice(end: Deadline) -> Deadline {
    Deadline(end.0.min(Instant::now() + Duration::from_secs(5)))
}

pub fn wait<S: Schema, T: 'static>(
    session: &mut Session<S>,
    ticket: &mut Ticket<T>,
    end: Deadline,
) -> Result<Outcome<T>> {
    loop {
        match session.wait(ticket, slice(end)) {
            Ok(result) => return Ok(result?),
            Err(Error::DeadlineExceeded) if !end.expired() => {}
            Err(error) => return Err(error.into()),
        }
    }
}
pub fn take<S: Schema, T: 'static>(
    session: &mut Session<S>,
    submission: Submission<T>,
) -> Result<Outcome<T>> {
    match submission {
        Submission::Ready(result) => Ok(result?),
        Submission::Pending(mut ticket) => wait(session, &mut ticket, deadline()),
    }
}
pub fn success<T>(outcome: Outcome<T>) -> Result<T> {
    match outcome {
        Outcome::Success(value) => Ok(value),
        Outcome::NotFound => Err("Example expected success,The actual key does not exist".into()),
        Outcome::Aborted(reason) => Err(format!("Example conditional abort:{reason:?}").into()),
    }
}

#[derive(Debug)]
struct ReportError<R: fmt::Debug>(SharedReport<R>);
impl<R: fmt::Debug> fmt::Display for ReportError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Maintenance failed:{:?}", self.0)
    }
}
impl<R: fmt::Debug + 'static> std::error::Error for ReportError<R> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0
            .as_ref()
            .as_ref()
            .err()
            .map(|error| error as &dyn std::error::Error)
    }
}
pub fn report<S: Schema, R: Clone + fmt::Debug + 'static>(
    session: &mut Session<S>,
    ticket: &MaintenanceTicket<R>,
) -> Result<R> {
    let end = deadline();
    let shared = loop {
        match session.wait_maintenance(ticket, slice(end)) {
            Ok(report) => break report,
            Err(Error::DeadlineExceeded) if !end.expired() => {}
            Err(error) => return Err(error.into()),
        }
    };
    match shared.as_ref() {
        Ok(report) => Ok(report.clone()),
        Err(_) => Err(Box::new(ReportError(shared))),
    }
}
/// After the session has been exited, advance the original maintenance task to completion.;Busy Will not trigger business resubmission or maintenance.
pub fn shutdown<S: Schema>(store: &RasterKV<S>) -> Result<()> {
    let end = deadline();
    store.maintenance().stop_auto_compaction()?;
    let mut progress_error = None;
    loop {
        match store.shutdown(slice(end)) {
            Ok(report) if report.device_drained => {
                return match progress_error {
                    Some(error) => Err(Box::new(error)),
                    None => Ok(()),
                };
            }
            Ok(_) => return Err("Device is not draining".into()),
            Err(Error::Busy | Error::DeadlineExceeded) if !end.expired() => {
                // Continue with accepted action;after failed shutdown shutdown Still responsible for resource draining.
                if let Err(error) = store.maintenance().poll(PollBudget::default()) {
                    progress_error.get_or_insert(error);
                }
                std::thread::yield_now();
            }
            Err(error) => {
                return match progress_error {
                    Some(progress) => Err(format!(
                        "Maintenance promotion failed:{progress:?};Device closing failed:{error:?}"
                    )
                    .into()),
                    None => Err(error.into()),
                };
            }
        }
    }
}
