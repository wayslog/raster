//! 等待只推进已接受票据；报告失败保留原始错误和部分生效信息。
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
        Outcome::NotFound => Err("示例预期成功，实际键不存在".into()),
        Outcome::Aborted(reason) => Err(format!("示例条件中止：{reason:?}").into()),
    }
}

#[derive(Debug)]
struct ReportError<R: fmt::Debug>(SharedReport<R>);
impl<R: fmt::Debug> fmt::Display for ReportError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "维护失败：{:?}", self.0)
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
/// 会话已退出后推进原维护任务收尾；Busy 不会触发重新提交业务或维护。
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
            Ok(_) => return Err("设备未排空".into()),
            Err(Error::Busy | Error::DeadlineExceeded) if !end.expired() => {
                // 继续已接受的动作；失败关闭后的 shutdown 仍负责资源排空。
                if let Err(error) = store.maintenance().poll(PollBudget::default()) {
                    progress_error.get_or_insert(error);
                }
                std::thread::yield_now();
            }
            Err(error) => {
                return match progress_error {
                    Some(progress) => {
                        Err(format!("维护推进失败：{progress:?}；设备收尾失败：{error:?}").into())
                    }
                    None => Err(error.into()),
                };
            }
        }
    }
}
