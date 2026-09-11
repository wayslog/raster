//! 逻辑截断、逐桶清理和可重试工作段删除；保留检查点材料不参与工作段回收。
use super::Engine;
use crate::{
    api::maintenance::{GcReport, MaintenanceCompleter, MaintenanceTicket, PhysicalReclamation},
    coordination::{Action, Phase},
    format::PageFrame,
    maintenance::scan::{Scan, ScanStep},
    schema::Schema,
    storage::reclaim::{SegmentCandidate, SegmentDelete},
    types::*,
};
use std::sync::{TryLockError, atomic::Ordering};
#[derive(Default)]
pub(crate) struct GcRuntime {
    job: Option<Job>,
    // 已摘除映射的删除意图跨动作保留；显式重试继续原步骤，而不重新寻找旧代次文件。
    deletion: Option<SegmentDelete>,
}
enum Stage {
    Validate,
    PublishBegin,
    Index,
    PrepareIo,
    Evict,
    Delete,
    Finish,
}
struct Job {
    id: MaintenanceId,
    target: LogAddress,
    applied_begin: LogAddress,
    stage: Stage,
    validation: Option<Scan>,
    bucket: usize,
    index_cleaned: bool,
    candidates: Option<Vec<SegmentCandidate>>,
    deleted: u64,
    deferred: bool,
    failure: Option<Error>,
    complete: MaintenanceCompleter<GcReport>,
    reported: bool,
}
impl Job {
    fn step<S: Schema>(
        &mut self,
        engine: &Engine<S>,
        deletion: &mut Option<SegmentDelete>,
    ) -> Result<bool, Error> {
        if self.failure.is_some() {
            if let Some(validation) = &mut self.validation
                && !validation.drain(&engine.storage)?
            {
                return Ok(false);
            }
            // 正常删除错误在接受完成后产生，不存在未归还缓冲；身份错误由失败关闭处理。
            if deletion.as_ref().is_some_and(SegmentDelete::has_inflight) {
                return Err(Error::InvalidState("删除失败仍存在不明在途请求"));
            }
            return self.finish(engine);
        }
        match self.stage {
            Stage::Validate => {
                if let Some(validation) = &mut self.validation {
                    match validation.step(engine)? {
                        ScanStep::End => self.validation = None,
                        ScanStep::Record(_, _) | ScanStep::Pending => return Ok(false),
                    }
                }
                self.stage = Stage::PublishBegin;
            }
            Stage::PublishBegin => {
                match engine.publish_gc_begin(self.target) {
                    Ok(()) => {}
                    Err(Error::Busy) => return Ok(false),
                    Err(error) => return Err(error),
                }
                self.applied_begin = self.target;
                engine.coordinator.advance(self.id, Phase::GcIo)?;
                self.stage = Stage::Index;
            }
            Stage::Index => {
                if self.bucket < engine.index.bucket_count()? {
                    engine.index.clean_bucket(self.bucket, self.target)?;
                    self.bucket += 1;
                } else {
                    self.index_cleaned = true;
                    engine.coordinator.advance(self.id, Phase::GcIndex)?;
                    self.stage = Stage::PrepareIo;
                }
            }
            Stage::PrepareIo => {
                let _storage = match engine.storage_progress.try_lock() {
                    Ok(guard) => guard,
                    Err(TryLockError::WouldBlock) => return Ok(false),
                    Err(_) => return Err(Error::InvalidState("GC 遇到后台日志锁中毒")),
                };
                if _storage.has_flush() {
                    return Ok(false);
                }
                engine.log.discard_prefix()?;
                self.stage = Stage::Evict;
            }
            Stage::Evict => {
                if engine.log.frontiers()?.safe_head < self.floor(engine) {
                    let progress = match engine.log.evict_next() {
                        Ok(progress) => progress,
                        Err(Error::Busy) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                    if progress.remaining != 0 {
                        self.deferred = true;
                        self.stage = Stage::Finish;
                    }
                    return Ok(progress.phase_advanced || self.deferred);
                }
                if engine.storage.device.capabilities().supports_files {
                    let boundary = PageFrame::physical_offset(
                        PageId(self.target.0 / engine.config.log.page_bytes as u64),
                        engine.config.log.page_bytes,
                    )?;
                    self.candidates = Some(engine.storage.reclaim_candidates(boundary)?);
                    self.stage = Stage::Delete;
                } else {
                    self.stage = Stage::Finish;
                }
            }
            Stage::Delete => {
                if let Some(task) = deletion {
                    let before = task.has_inflight();
                    let done = task.step(&engine.storage)?;
                    let advanced = done || before != task.has_inflight();
                    if done {
                        self.deleted =
                            self.deleted.checked_add(1).ok_or(Error::CapacityExceeded)?;
                        *deletion = None;
                    }
                    return Ok(advanced);
                }
                if let Some(candidate) = self.candidates.as_mut().expect("已枚举工作段").pop()
                {
                    match SegmentDelete::detach(
                        &engine.storage,
                        engine.io.clone(),
                        engine.id,
                        candidate,
                    ) {
                        Ok(task) => *deletion = Some(task),
                        Err(Error::Busy) => self.deferred = true,
                        Err(error) => return Err(error),
                    }
                } else {
                    self.stage = Stage::Finish;
                }
            }
            Stage::Finish => return self.finish(engine),
        }
        Ok(true)
    }
    fn floor<S: Schema>(&self, engine: &Engine<S>) -> LogAddress {
        LogAddress(
            self.target.0 / engine.config.log.page_bytes as u64
                * engine.config.log.page_bytes as u64,
        )
    }
    fn error<S: Schema>(&self, engine: &Engine<S>, cause: Error) -> Error {
        Error::GcFailed {
            begin: engine
                .log
                .frontiers()
                .map_or(self.applied_begin, |frontiers| frontiers.begin),
            index_cleaned: self.index_cleaned,
            deleted_segments: self.deleted,
            cause: Box::new(cause),
        }
    }
    fn finish<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let result = match self.failure.take() {
            Some(cause) => Err(self.error(engine, cause)),
            None => Ok(GcReport {
                begin: self.target,
                index_cleaned: self.index_cleaned,
                deleted_segments: self.deleted,
                physical: if self.deferred {
                    PhysicalReclamation::DeferredByRuntime {
                        begin: LogAddress(0),
                        end: self.target,
                    }
                } else {
                    PhysicalReclamation::Completed
                },
            }),
        };
        // 失败也释放本次动作；报告保留真实阶段，不能把跳过的清理算作成功。
        for _ in 0..2 {
            match engine.coordinator.snapshot()?.phase {
                Phase::GcIo => {
                    engine.coordinator.advance(self.id, Phase::GcIo)?;
                }
                Phase::GcIndex => {
                    engine.coordinator.advance(self.id, Phase::GcIndex)?;
                }
                _ => break,
            }
        }
        if engine.coordinator.snapshot()?.phase != Phase::Failed {
            engine.coordinator.finish_action(self.id)?;
        }
        self.complete.finish(result)?;
        self.reported = true;
        Ok(true)
    }
    fn fail<S: Schema>(&mut self, engine: &Engine<S>, cause: Error) -> Result<(), Error> {
        if !self.reported {
            let cause = self.failure.take().unwrap_or(cause);
            self.complete.finish(Err(self.error(engine, cause)))?;
            self.reported = true;
        }
        Ok(())
    }
}
impl<S: Schema> Engine<S> {
    fn publish_gc_begin(&self, begin: LogAddress) -> Result<(), Error> {
        let mut guards = Vec::new();
        guards
            .try_reserve_exact(self.operations.len())
            .map_err(|_| Error::OutOfMemory)?;
        for gate in &self.operations {
            guards.push(gate.try_lock().map_err(|error| match error {
                TryLockError::WouldBlock => Error::Busy,
                _ => Error::InvalidState("GC 遇到业务仲裁锁中毒"),
            })?);
        }
        let mut checkpoints = self.checkpoints.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            _ => Error::InvalidState("GC 遇到检查点锁中毒"),
        })?;
        self.cache.with_normalized_index(&self.index, || {
            self.log.publish_begin(begin)?;
            checkpoints.invalidate_before(begin);
            Ok(())
        })
    }
    pub(crate) fn start_gc(
        &self,
        target: LogAddress,
    ) -> Result<MaintenanceTicket<GcReport>, Error> {
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("存储已关闭或失败"));
        }
        let caps = self.storage.device.capabilities();
        if caps.supports_files && !caps.supports_directory_sync {
            return Err(Error::UnsupportedDurability);
        }
        let mut runtime = self.gc.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            _ => Error::InvalidState("GC 任务锁中毒"),
        })?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        let before = self.log.frontiers()?;
        let before = self.log.validate_scan_range(before.begin, target)?;
        let validation = if target != before.begin
            && target < before.head
            && !target.0.is_multiple_of(self.config.log.page_bytes as u64)
        {
            Some(Scan::new(
                self,
                LogAddress(
                    target.0 / self.config.log.page_bytes as u64
                        * self.config.log.page_bytes as u64,
                )
                .max(before.begin),
                target,
            )?)
        } else {
            None
        };
        let id = self.coordinator.start_action(Action::Gc)?;
        let (ticket, complete) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            target,
            applied_begin: before.begin,
            stage: Stage::Validate,
            validation,
            bucket: 0,
            index_cleaned: false,
            candidates: None,
            deleted: 0,
            deferred: false,
            failure: None,
            complete,
            reported: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_gc(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.gc.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("GC 任务锁中毒")),
        };
        let GcRuntime { job, deletion } = &mut *runtime;
        let Some(job) = job else {
            return Ok((false, false));
        };
        if job.reported {
            return Ok((false, false));
        }
        let draining = job.failure.is_some();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self, deletion)));
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                self.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("GC 推进恐慌"))
            }
        };
        let advanced = match result {
            Ok(advanced) => advanced,
            Err(error) => {
                if draining || matches!(error, Error::InvalidState(_)) {
                    self.failed.store(true, Ordering::SeqCst);
                }
                if job.failure.is_none() {
                    job.failure = Some(error);
                }
                false
            }
        };
        if self.failed.load(Ordering::SeqCst) {
            job.fail(self, Error::InvalidState("GC 期间引擎失败关闭"))?;
            return Err(Error::InvalidState("GC 失败关闭，详见维护报告"));
        }
        let finished = job.reported;
        if finished {
            runtime.job = None;
        }
        Ok((advanced, finished))
    }
    pub(crate) fn fail_gc(&self) -> Result<(), Error> {
        let mut runtime = match self.gc.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("GC 任务锁中毒")),
        };
        if let Some(job) = &mut runtime.job {
            job.fail(self, Error::InvalidState("引擎或协调动作失败，GC 终止"))?;
        }
        Ok(())
    }
    pub(crate) fn release_stopped_gc(&self) -> Result<(), Error> {
        self.fail_gc()?;
        let mut runtime = self
            .gc
            .lock()
            .map_err(|_| Error::InvalidState("GC 任务锁中毒"))?;
        runtime.job = None;
        runtime.deletion = None;
        Ok(())
    }
}
