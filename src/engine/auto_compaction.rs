//! 一个有界调度线程复用压缩与 GC 票据；空闲不持有存储，停止不撤销已接受任务。
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
            .map_err(|_| Error::InvalidState("自动维护状态锁中毒"))
    }
    fn fail(&self, error: Error) {
        // 异常收尾仍保存原因；中毒本身已经意味着失败，不能恢复成功调度。
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
                .map_err(|_| Error::InvalidState("自动维护等待锁中毒"))?,
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
        // 单次等待很短，Session 等待者仍可及时刷新自己的检查点屏障。
        let duration = deadline
            .0
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(1));
        let state = self.shared.lock()?;
        drop(
            self.shared
                .wake
                .wait_timeout(state, duration)
                .map_err(|_| Error::InvalidState("自动维护等待锁中毒"))?,
        );
        Ok(())
    }
    fn status(&self) -> Result<AutoCompactionStatus, Error> {
        let mut thread = self
            .thread
            .lock()
            .map_err(|_| Error::InvalidState("自动维护线程锁中毒"))?;
        if thread.as_ref().is_some_and(|thread| thread.is_finished()) {
            if thread.take().expect("线程存在").join().is_err() {
                self.shared
                    .fail(Error::InvalidState("自动维护线程意外退出"));
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
        // 最后一个 Engine 引用可能在本线程的一步末尾释放，析构不能 join 自己。
        let _ = self.request_stop();
    }
}

/// 使用真实安全冷区和向下页对齐；预算是触发阈值，不是严格拒写上限。
fn target(
    policy: &AutoCompactionPolicy,
    page: u64,
    f: Frontiers,
) -> Result<Option<LogAddress>, Error> {
    let span = f
        .tail
        .0
        .checked_sub(f.begin.0)
        .ok_or(Error::InvalidState("日志跨度倒置"))?;
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
            .map_err(|_| Error::InvalidState("自动维护线程锁中毒"))?;
        {
            let mut state = runtime.shared.lock()?;
            if state.phase != Phase::Disabled || state.stop {
                return Err(Error::InvalidState("自动维护不能重复启动"));
            }
            state.phase = Phase::Idle;
        }
        let weak = Arc::downgrade(self);
        let control = runtime.shared.clone();
        *handle = Some(
            thread::Builder::new()
                .name("raster自动压缩".into())
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| run(weak.clone(), &control)));
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            if let Some(engine) = weak.upgrade()
                                && engine.failed.load(Ordering::SeqCst)
                            {
                                // 接受阶段也可能由设备能力查询触发失败；同步终结当时存在的手动全局动作。
                                let _ = engine.poll_maintenance(PollBudget::default());
                            }
                            control.fail(error);
                        }
                        Err(_) => {
                            if let Some(engine) = weak.upgrade() {
                                engine.failed.store(true, Ordering::SeqCst);
                                let _ = engine.poll_maintenance(PollBudget::default());
                            }
                            control.fail(Error::InvalidState("自动维护调度恐慌"));
                        }
                    }
                    // Stopped/Failed 由观察者在 join 后发布。
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
            .ok_or(Error::InvalidState("日志跨度倒置"))?;
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
        // 每一步才升级强引用，睡眠和空闲期间存储可以正常销毁。
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
                // 失败推进仍会完成失败票据并停止工作者；先收真实结果再结束线程。
                let result = match catch_unwind(AssertUnwindSafe(|| {
                    engine.poll_maintenance(PollBudget::default())
                })) {
                    Ok(result) => result,
                    Err(_) => {
                        // 设备轮询等外层 panic 也先进入统一失败协议，不能留下健康的全局动作阻碍关闭。
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
                return Err(Error::InvalidState("引擎失败，自动维护停止"));
            } else if Instant::now() < next_check {
                next_check.saturating_duration_since(Instant::now())
            } else {
                let f = engine.log.frontiers()?;
                let until = target(policy, engine.config.log.page_bytes as u64, f)?;
                let mut state = control.lock()?;
                if state.stop {
                    return Ok(());
                }
                // 与停止请求在同一锁内线性化接受；锁外执行复制和 I/O 推进。
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
                        return Err(Error::InvalidState("自动维护接受任务恐慌"));
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
                        // 未接受期间其他维护可移动 begin；下一轮从实际边界重新规划。
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
    fn 自动目标只覆盖阈值以上的完整安全冷页且受单次预算限制() {
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
    fn 自动配置拒绝零预算无效比例间隔及不足一页的上限() {
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
