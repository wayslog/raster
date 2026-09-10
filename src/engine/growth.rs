//! 在线扩容参与全局动作；迁移不跨业务回调，完成报告等待旧表安全释放。
use super::Engine;
use crate::{
    api::maintenance::{IndexGrowthReport, MaintenanceCompleter, MaintenanceTicket},
    coordination::{Action, Phase},
    epoch::DeferredAction,
    index::growth::GrowthProgress,
    schema::Schema,
    types::*,
};
use std::sync::{TryLockError, atomic::Ordering};
#[derive(Default)]
pub(crate) struct GrowthRuntime {
    job: Option<Job>,
}
struct Job {
    id: MaintenanceId,
    complete: MaintenanceCompleter<IndexGrowthReport>,
    progress: Option<GrowthProgress>,
    reclaimed: bool,
    failed: bool,
    finished: bool,
}
impl Job {
    fn step<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let state = engine.coordinator.snapshot()?;
        if state.id != Some(self.id) || state.phase == Phase::Failed {
            return Err(Error::InvalidState("扩容动作已失效或失败"));
        }
        match state.phase {
            Phase::GrowPrepare => Ok(false),
            Phase::GrowCopy => {
                let mut advanced = false;
                if self.progress.is_none() {
                    self.progress = Some(engine.index.begin_growth()?);
                    advanced = true;
                }
                // 请求按同一方向取得其中一个锁；这里全部用 try_lock，绝不等待用户回调。
                let mut gates = Vec::new();
                gates
                    .try_reserve_exact(engine.operations.len())
                    .map_err(|_| Error::OutOfMemory)?;
                for gate in &engine.operations {
                    match gate.try_lock() {
                        Ok(guard) => gates.push(guard),
                        Err(TryLockError::WouldBlock) => return Ok(advanced),
                        Err(_) => return Err(Error::InvalidState("扩容遇到业务仲裁锁中毒")),
                    }
                }
                let progress = engine.cache.with_normalized_index(&engine.index, || {
                    engine.index.grow_step(PollBudget(
                        std::num::NonZeroUsize::new(1).expect("固定预算"),
                    ))
                })?;
                self.progress = Some(progress);
                if progress.complete {
                    engine.epoch.defer(DeferredAction::ReleaseIndex(Generation(
                        progress.generation.0 - 1,
                    )))?;
                    engine.epoch.advance()?;
                    engine.coordinator.advance(self.id, Phase::GrowCopy)?;
                }
                Ok(true)
            }
            Phase::Publish => {
                let progress = self
                    .progress
                    .ok_or(Error::InvalidState("扩容缺少迁移结果"))?;
                for action in engine.epoch.collect()? {
                    match action {
                        DeferredAction::ReleaseIndex(generation)
                            if generation.0 + 1 == progress.generation.0 =>
                        {
                            engine.index.release_retired(generation)?;
                            self.reclaimed = true;
                        }
                        _ => return Err(Error::InvalidState("扩容收到未支持的 epoch 释放动作")),
                    }
                }
                if !self.reclaimed {
                    return Ok(false);
                }
                engine.coordinator.finish_action(self.id)?;
                self.complete.finish(Ok(IndexGrowthReport {
                    old_buckets: progress.old_buckets,
                    new_buckets: progress.new_buckets,
                    generation: progress.generation,
                }))?;
                self.finished = true;
                Ok(true)
            }
            _ => Err(Error::InvalidState("扩容动作处于错误阶段")),
        }
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn start_growth(&self) -> Result<MaintenanceTicket<IndexGrowthReport>, Error> {
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("存储已关闭或失败"));
        }
        let mut runtime = self.growth.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Poisoned(_) => Error::InvalidState("扩容任务锁中毒"),
        })?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        let id = self.coordinator.start_action(Action::GrowIndex)?;
        let (ticket, complete) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            complete,
            progress: None,
            reclaimed: false,
            failed: false,
            finished: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_growth(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.growth.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("扩容任务锁中毒")),
        };
        let Some(job) = &mut runtime.job else {
            return Ok((false, false));
        };
        if job.failed {
            return Ok((false, false));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self)))
            .unwrap_or(Err(Error::InvalidState("扩容推进恐慌")));
        match result {
            Err(error) => {
                job.complete.finish(Err(error))?;
                job.failed = true;
                Err(Error::InvalidState("扩容失败，详见维护报告"))
            }
            Ok(advanced) => {
                let finished = job.finished;
                if finished {
                    runtime.job = None;
                }
                Ok((advanced, finished))
            }
        }
    }
    pub(crate) fn fail_growth(&self) -> Result<(), Error> {
        let mut runtime = match self.growth.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("扩容任务锁中毒")),
        };
        if let Some(job) = &mut runtime.job
            && !job.failed
            && !job.finished
        {
            job.complete
                .finish(Err(Error::InvalidState("引擎或协调动作失败，扩容终止")))?;
            job.failed = true;
        }
        Ok(())
    }
}
