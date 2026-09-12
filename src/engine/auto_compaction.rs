//! A bounded-scheduled thread reuse compression with GC bill;Free does not hold storage,Stop without undoing accepted tasks.
use super::Engine;
use crate::{
    api::maintenance::{
        AutoCompactionPhase as Phase, AutoCompactionStatus, CompactionAlgorithm, CompactionOptions,
        CompactionReport, GcReport, MaintenanceTicket, PhysicalReclamation, SharedReport,
    },
    config::AutoCompactionPolicy,
    log::Frontiers,
    schema::Schema,
    types::*,
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, Weak, atomic::Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub(crate) struct AutoCompactionRuntime {
    shared: Arc<Control>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
struct Control {
    state: Mutex<State>,
    wake: Condvar,
}
struct State {
    phase: Phase,
    stop: bool,
    failed: bool,
    task: Option<Task>,
    completed: u64,
    last_compaction: Option<SharedReport<CompactionReport>>,
    last_reclamation: Option<SharedReport<GcReport>>,
    failure: Option<Arc<Error>>,
    reclaim: bool,
}
enum Task {
    Compact(MaintenanceTicket<CompactionReport>),
    Reclaim(MaintenanceTicket<GcReport>),
}
impl Task {
    fn id(&self) -> MaintenanceId {
        match self {
            Self::Compact(ticket) => ticket.id(),
            Self::Reclaim(ticket) => ticket.id(),
        }
    }
}
impl Default for AutoCompactionRuntime {
    fn default() -> Self {
        Self {
            shared: Arc::new(Control {
                state: Mutex::new(State {
                    phase: Phase::Disabled,
                    stop: false,
                    failed: false,
                    task: None,
                    completed: 0,
                    last_compaction: None,
                    last_reclamation: None,
                    failure: None,
                    reclaim: false,
                }),
                wake: Condvar::new(),
            }),
            thread: Mutex::new(None),
        }
    }
}
impl Control {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::InvalidState("Automatic maintenance status lock poisoning"))
    }
    fn fail(&self, error: Error) {
        // The reason why the abnormal ending is still saved;Poisoning itself already means failure,Unable to restore successful schedule.
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.failed = true;
        state.stop = true;
        state.phase = Phase::Stopping;
        if state.failure.is_none() {
            state.failure = Some(Arc::new(error));
        }
        self.wake.notify_all();
    }
    fn pause(&self, duration: Duration) -> Result<(), Error> {
        let state = self.lock()?;
        if state.stop && state.task.is_none() {
            return Ok(());
        }
        drop(
            self.wake
                .wait_timeout(state, duration)
                .map_err(|_| Error::InvalidState("Automatic maintenance waiting lock poisoning"))?,
        );
        Ok(())
    }
}
impl AutoCompactionRuntime {
    pub(crate) fn request_stop(&self) -> Result<(), Error> {
        let mut state = self.shared.lock()?;
        state.stop = true;
        if !matches!(
            state.phase,
            Phase::Disabled | Phase::Stopped | Phase::Failed
        ) {
            state.phase = Phase::Stopping;
        }
        self.shared.wake.notify_all();
        Ok(())
    }
    pub(crate) fn wait_change(&self, deadline: Deadline) -> Result<(), Error> {
        // Single wait is very short,Session Waiters can still refresh their checkpoint barriers in time.
        let duration = deadline
            .0
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(1));
        let state = self.shared.lock()?;
        drop(
            self.shared
                .wake
                .wait_timeout(state, duration)
                .map_err(|_| Error::InvalidState("Automatic maintenance waiting lock poisoning"))?,
        );
        Ok(())
    }
    fn status(&self) -> Result<AutoCompactionStatus, Error> {
        let mut thread = self
            .thread
            .lock()
            .map_err(|_| Error::InvalidState("Automatic maintenance thread lock poisoning"))?;
        if thread.as_ref().is_some_and(|thread| thread.is_finished()) {
            if thread.take().expect("Thread exists").join().is_err() {
                self.shared.fail(Error::InvalidState(
                    "Automatic maintenance thread exits unexpectedly",
                ));
            }
            let mut state = self.shared.lock()?;
            state.phase = if state.failed {
                Phase::Failed
            } else {
                Phase::Stopped
            };
            self.shared.wake.notify_all();
        }
        let state = self.shared.lock()?;
        Ok(AutoCompactionStatus {
            phase: state.phase,
            active: state.task.as_ref().map(Task::id),
            completed_compactions: state.completed,
            last_compaction: state.last_compaction.clone(),
            last_reclamation: state.last_reclamation.clone(),
            failure: state.failure.clone(),
            log_bytes: 0,
            budget_reached: false,
        })
    }
}
impl Drop for AutoCompactionRuntime {
    fn drop(&mut self) {
        // the last one Engine The reference may be released at the end of a step in this thread,Destruction cannot join myself.
        let _ = self.request_stop();
    }
}

/// Use true safe cold areas and page-down alignment;Budget is the trigger threshold,It's not a strict upper limit..
fn target(
    policy: &AutoCompactionPolicy,
    page: u64,
    f: Frontiers,
) -> Result<Option<LogAddress>, Error> {
    let span = f
        .tail
        .0
        .checked_sub(f.begin.0)
        .ok_or(Error::InvalidState("Log span inversion"))?;
    let threshold = (policy.log_size_budget as f64 * policy.trigger_fraction).ceil() as u64;
    if span < threshold {
        return Ok(None);
    }
    let bytes = ((span as f64 * policy.compact_fraction) as u64).min(policy.max_compacted_bytes);
    let until = f
        .begin
        .0
        .saturating_add(bytes)
        .min(f.safe_head.0)
        .min(f.safe_read_only.0);
    let until = LogAddress(until - until % page);
    Ok((until > f.begin).then_some(until))
}
impl<S: Schema> Engine<S> {
    pub(crate) fn start_auto_compaction(self: &Arc<Self>) -> Result<(), Error> {
        if !self.config.maintenance.auto_compaction {
            return Ok(());
        }
        let caps = self.storage.device.capabilities();
        if !caps.supports_files || !caps.supports_directory_sync {
            return Err(Error::UnsupportedDurability);
        }
        let runtime = &self.auto_compaction;
        let mut handle = runtime
            .thread
            .lock()
            .map_err(|_| Error::InvalidState("Automatic maintenance thread lock poisoning"))?;
        {
            let mut state = runtime.shared.lock()?;
            if state.phase != Phase::Disabled || state.stop {
                return Err(Error::InvalidState(
                    "Automatic maintenance cannot be started repeatedly",
                ));
            }
            state.phase = Phase::Idle;
        }
        let weak = Arc::downgrade(self);
        let control = runtime.shared.clone();
        *handle = Some(
            thread::Builder::new()
                .name("rasterautomatic_compression".into())
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| run(weak.clone(), &control)));
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            if let Some(engine) = weak.upgrade()
                                && engine.failed.load(Ordering::SeqCst)
                            {
                                // The acceptance phase may also fail triggered by a device capability query;Synchronously terminate existing manual global actions.
                                let _ = engine.poll_maintenance(PollBudget::default());
                            }
                            control.fail(error);
                        }
                        Err(_) => {
                            if let Some(engine) = weak.upgrade() {
                                engine.failed.store(true, Ordering::SeqCst);
                                let _ = engine.poll_maintenance(PollBudget::default());
                            }
                            control
                                .fail(Error::InvalidState("Automated maintenance schedule panic"));
                        }
                    }
                    // Stopped/Failed by the observer in join Posted later.
                    if let Ok(mut state) = control.lock() {
                        state.phase = Phase::Stopping;
                    }
                    control.wake.notify_all();
                })
                .map_err(Error::Io)?,
        );
        Ok(())
    }
    pub(crate) fn auto_compaction_status(&self) -> Result<AutoCompactionStatus, Error> {
        let mut status = self.auto_compaction.status()?;
        let f = self.log.frontiers()?;
        status.log_bytes = f
            .tail
            .0
            .checked_sub(f.begin.0)
            .ok_or(Error::InvalidState("Log span inversion"))?;
        let budget = self
            .config
            .maintenance
            .auto_compaction_policy
            .log_size_budget;
        status.budget_reached =
            self.config.maintenance.auto_compaction && budget != 0 && status.log_bytes >= budget;
        Ok(status)
    }
}

fn collect(control: &Control) -> Result<bool, Error> {
    let mut state = control.lock()?;
    match &state.task {
        Some(Task::Compact(ticket)) => {
            let Some(report) = ticket.try_report()? else {
                return Ok(false);
            };
            state.completed = state
                .completed
                .checked_add(1)
                .ok_or(Error::CapacityExceeded)?;
            state.failed |= report.is_err();
            state.reclaim = report
                .as_ref()
                .as_ref()
                .ok()
                .and_then(|r| r.gc.as_ref())
                .is_some_and(|r| {
                    matches!(r.physical, PhysicalReclamation::DeferredByRuntime { .. })
                });
            state.last_compaction = Some(report);
        }
        Some(Task::Reclaim(ticket)) => {
            let Some(report) = ticket.try_report()? else {
                return Ok(false);
            };
            state.failed |= report.is_err();
            state.reclaim = report.as_ref().as_ref().ok().is_some_and(|r| {
                matches!(r.physical, PhysicalReclamation::DeferredByRuntime { .. })
            });
            state.last_reclamation = Some(report);
        }
        None => return Ok(true),
    }
    state.task = None;
    state.stop |= state.failed;
    state.phase = if state.stop {
        Phase::Stopping
    } else {
        Phase::Idle
    };
    control.wake.notify_all();
    Ok(true)
}
fn run<S: Schema>(weak: Weak<Engine<S>>, control: &Control) -> Result<(), Error> {
    let mut next_check = Instant::now();
    loop {
        // Upgrade strong references at each step,Storage can be destroyed normally during sleep and idle periods.
        let duration = {
            let Some(engine) = weak.upgrade() else {
                return Ok(());
            };
            let active = {
                let state = control.lock()?;
                if state.stop && state.task.is_none() {
                    return Ok(());
                }
                state.task.is_some()
            };
            let policy = &engine.config.maintenance.auto_compaction_policy;
            if active {
                // A failed push will still complete the failed ticket and stop the worker;Collect the real results first and then end the thread.
                let result = match catch_unwind(AssertUnwindSafe(|| {
                    engine.poll_maintenance(PollBudget::default())
                })) {
                    Ok(result) => result,
                    Err(_) => {
                        // Device polling and other outer layers panic Also enter the unified failure protocol first,Can't leave healthy global actions blocking closure.
                        engine.failed.store(true, Ordering::SeqCst);
                        engine.poll_maintenance(PollBudget::default())
                    }
                };
                if collect(control)? {
                    if let Err(error) = result {
                        control.fail(error);
                    }
                    next_check = Instant::now() + policy.check_interval;
                }
                Duration::from_micros(100)
            } else if engine.failed.load(Ordering::SeqCst) {
                return Err(Error::InvalidState(
                    "engine failure,Automatic maintenance stopped",
                ));
            } else if Instant::now() < next_check {
                next_check.saturating_duration_since(Instant::now())
            } else {
                let f = engine.log.frontiers()?;
                let until = target(policy, engine.config.log.page_bytes as u64, f)?;
                let mut state = control.lock()?;
                if state.stop {
                    return Ok(());
                }
                // Linearize acceptance within the same lock as stop request;Execution of copy and lock-out I/O advance.
                let accepted = catch_unwind(AssertUnwindSafe(|| {
                    if state.reclaim {
                        engine.start_gc(f.begin).map(Task::Reclaim).map(Some)
                    } else if let Some(until) = until {
                        if state.completed == u64::MAX {
                            return Err(Error::CapacityExceeded);
                        }
                        engine
                            .start_compaction(CompactionOptions {
                                algorithm: CompactionAlgorithm::Lookup,
                                until,
                                workers: engine.config.maintenance.workers,
                                shift_begin: true,
                                checkpoint: false,
                            })
                            .map(Task::Compact)
                            .map(Some)
                    } else {
                        Ok(None)
                    }
                }));
                let accepted = match accepted {
                    Ok(result) => result,
                    Err(_) => {
                        engine.failed.store(true, Ordering::SeqCst);
                        return Err(Error::InvalidState(
                            "Automatic maintenance acceptance task panics",
                        ));
                    }
                };
                let delay = match accepted {
                    Ok(Some(task)) => {
                        state.phase = match &task {
                            Task::Compact(_) => Phase::Compacting,
                            Task::Reclaim(_) => Phase::Reclaiming,
                        };
                        state.task = Some(task);
                        Duration::from_micros(100)
                    }
                    Ok(None) => {
                        state.phase = Phase::Idle;
                        policy.check_interval
                    }
                    Err(Error::Busy | Error::RangeTruncated) => {
                        // Other maintenance during the non-acceptance period can be moved begin;Next round re-planning from actual boundaries.
                        state.phase = Phase::Scheduled;
                        policy.check_interval.min(Duration::from_millis(10))
                    }
                    Err(Error::InvalidFormat(_)) if engine.log.frontiers()?.begin != f.begin => {
                        state.phase = Phase::Scheduled;
                        policy.check_interval.min(Duration::from_millis(10))
                    }
                    Err(error) => return Err(error),
                };
                next_check = Instant::now() + delay;
                control.wake.notify_all();
                delay
            }
        };
        control.pause(duration)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_automatic_target_only_covers_complete_safe_cold_pages_above_the_threshold_and_is_limited_by_the_single_budget()
     {
        let policy = AutoCompactionPolicy {
            log_size_budget: 20000,
            trigger_fraction: 0.5,
            compact_fraction: 0.75,
            max_compacted_bytes: 10000,
            ..Default::default()
        };
        for begin in [0, 48, 4096, 8000] {
            for span in [0, 1, 9999, 10000, 40000] {
                for cold in [0, 1000, 4096, 8192, 30000] {
                    let f = Frontiers {
                        begin: LogAddress(begin),
                        tail: LogAddress(begin + span),
                        safe_head: LogAddress(begin + cold.min(span)),
                        safe_read_only: LogAddress(begin + cold.min(span)),
                        ..Default::default()
                    };
                    let planned = target(&policy, 4096, f).unwrap();
                    if span < 10000 {
                        assert!(planned.is_none());
                    }
                    if let Some(until) = planned {
                        assert!(
                            until > f.begin && until <= f.safe_head && until <= f.safe_read_only
                        );
                        assert!(until.0.is_multiple_of(4096));
                        assert!(until.0 - begin <= 10000);
                        assert!(until.0 - begin <= (span as f64 * 0.75) as u64);
                    }
                }
            }
        }
        let f = Frontiers {
            begin: LogAddress(0),
            tail: LogAddress(50000),
            safe_head: LogAddress(12000),
            safe_read_only: LogAddress(6000),
            ..Default::default()
        };
        assert_eq!(target(&policy, 4096, f).unwrap(), Some(LogAddress(4096)));
    }
    #[test]
    fn automatic_configuration_rejects_zero_budget_invalid_ratio_interval_and_upper_limit_of_less_than_one_page()
     {
        let mut config = crate::config::Config::default();
        config.validate().unwrap();
        config.maintenance.auto_compaction = true;
        assert!(config.validate().is_err());
        config.maintenance.auto_compaction_policy.log_size_budget = 1 << 30;
        config.validate().unwrap();
        for fraction in [f64::NAN, f64::INFINITY, 0.0, -0.1, 1.1] {
            let mut bad = config.clone();
            bad.maintenance.auto_compaction_policy.trigger_fraction = fraction;
            assert!(bad.validate().is_err());
            let mut bad = config.clone();
            bad.maintenance.auto_compaction_policy.compact_fraction = fraction;
            assert!(bad.validate().is_err());
        }
        for duration in [Duration::ZERO, Duration::MAX] {
            let mut bad = config.clone();
            bad.maintenance.auto_compaction_policy.check_interval = duration;
            assert!(bad.validate().is_err());
        }
        config
            .maintenance
            .auto_compaction_policy
            .max_compacted_bytes = config.log.page_bytes as u64 - 1;
        assert!(config.validate().is_err());
    }
}
