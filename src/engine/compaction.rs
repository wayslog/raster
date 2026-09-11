//! 单存储压缩：物理扫描选候选，条件复制重新查证；普通压缩不推进 begin。
use super::{
    Engine,
    conditional_copy::{ConditionalCopy, CopyResult},
};
use crate::{
    api::maintenance::{
        CompactionAlgorithm, CompactionOptions, CompactionReport, MaintenanceCompleter,
        MaintenanceTicket,
    },
    coordination::{Action, Phase},
    format::Record,
    maintenance::scan::{Scan, ScanStep},
    schema::Schema,
    types::*,
};
use std::{
    collections::BTreeMap,
    sync::{TryLockError, atomic::Ordering},
};

#[derive(Default)]
pub(crate) struct CompactionRuntime {
    job: Option<Job>,
}
struct Job {
    id: MaintenanceId,
    options: CompactionOptions,
    scan: Scan,
    scanned: bool,
    candidates: BTreeMap<Vec<u8>, LogAddress>,
    key_bytes: usize,
    copying: Option<ConditionalCopy>,
    copied: u64,
    failure: Option<Error>,
    complete: MaintenanceCompleter<CompactionReport>,
    reported: bool,
}
impl Job {
    fn step<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        if self.failure.is_some() {
            if let Some(copy) = &mut self.copying
                && !copy.drain(&engine.storage)?
            {
                return Ok(false);
            }
            if !self.scan.drain(&engine.storage)? {
                return Ok(false);
            }
            return self.finish(engine);
        }
        if let Some(copy) = &mut self.copying {
            // 在可能发布之前验证计数，错误报告不能漏掉已生效迁移。
            let next = self.copied.checked_add(1).ok_or(Error::CapacityExceeded)?;
            match engine.conditional_copy(copy, PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            {
                Ok(CopyResult::Copied(_)) => {
                    self.copied = next;
                    self.copying = None;
                }
                Ok(CopyResult::Obsolete) => {
                    self.copying = None;
                }
                Ok(CopyResult::Retry) => return Ok(false),
                Err(error) => {
                    if copy.published_address().is_some() {
                        self.copied = next;
                    }
                    return Err(error);
                }
            }
            return Ok(true);
        }
        if self.scanned {
            if let Some((key, source)) = self.candidates.pop_first() {
                self.key_bytes -= key.len();
                self.copying = Some(engine.new_conditional_copy(self.id, source, key)?);
                return Ok(true);
            }
            return self.finish(engine);
        }
        match self.scan.step(engine)? {
            ScanStep::Pending => Ok(false),
            ScanStep::End => {
                self.scanned = true;
                Ok(true)
            }
            ScanStep::Record(address, bytes) => {
                let record = Record::decode(&bytes)?;
                if record.header.invalid {
                    return Ok(true);
                }
                if record.header.version > engine.coordinator.snapshot()?.version {
                    return Err(Error::InvalidFormat("压缩扫描记录版本超过当前版本"));
                }
                let mut key = Vec::new();
                key.try_reserve_exact(record.key.len())
                    .map_err(|_| Error::OutOfMemory)?;
                key.extend_from_slice(record.key);
                match self.options.algorithm {
                    CompactionAlgorithm::Lookup => {
                        self.copying = Some(engine.new_conditional_copy(self.id, address, key)?);
                    }
                    CompactionAlgorithm::ScanDedup => {
                        if let Some(previous) = self.candidates.get_mut(&key) {
                            *previous = address;
                        } else {
                            let bytes = self
                                .key_bytes
                                .checked_add(key.len())
                                .ok_or(Error::CapacityExceeded)?;
                            if self.candidates.len()
                                >= engine.config.maintenance.max_compaction_keys
                                || bytes > engine.config.maintenance.max_compaction_key_bytes
                            {
                                return Err(Error::CapacityExceeded);
                            }
                            self.candidates.insert(key, address);
                            self.key_bytes = bytes;
                        }
                    }
                }
                Ok(true)
            }
        }
    }
    fn finish<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let result = if let Some(cause) = self.failure.take() {
            Err(Error::CompactionFailed {
                until: self.options.until,
                copied: self.copied,
                cause: Box::new(cause),
            })
        } else {
            Ok(CompactionReport {
                until: self.options.until,
                copied: self.copied,
                gc: None,
                checkpoint: None,
            })
        };
        let state = engine.coordinator.snapshot()?;
        if state.phase != Phase::Failed {
            engine.coordinator.advance(self.id, Phase::Compacting)?;
            engine.coordinator.finish_action(self.id)?;
        }
        self.complete.finish(result)?;
        self.reported = true;
        Ok(true)
    }
    fn fail(&mut self, cause: Error) {
        if self.failure.is_none() {
            self.failure = Some(cause);
        }
    }
    fn report_failed(&mut self, cause: Error) -> Result<(), Error> {
        if !self.reported {
            self.complete.finish(Err(Error::CompactionFailed {
                until: self.options.until,
                copied: self.copied,
                cause: Box::new(self.failure.take().unwrap_or(cause)),
            }))?;
            self.reported = true;
        }
        Ok(())
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn start_compaction(
        &self,
        options: CompactionOptions,
    ) -> Result<MaintenanceTicket<CompactionReport>, Error> {
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("存储已关闭或失败"));
        }
        if options.workers == 0 {
            return Err(Error::InvalidConfig {
                field: "compaction.workers",
                reason: "压缩工作线程数必须非零",
            });
        }
        if options.workers != 1 || options.shift_begin || options.checkpoint {
            return Err(Error::unimplemented("compaction::多线程与后续动作"));
        }
        let mut runtime = self.compaction.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Poisoned(_) => Error::InvalidState("压缩任务锁中毒"),
        })?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        let begin = self.log.frontiers()?.begin;
        let scan = Scan::new(self, begin, options.until)?;
        let id = self.coordinator.start_action(Action::Compact)?;
        let (ticket, complete) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            options,
            scan,
            scanned: false,
            candidates: BTreeMap::new(),
            key_bytes: 0,
            copying: None,
            copied: 0,
            failure: None,
            complete,
            reported: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_compaction(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.compaction.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("压缩任务锁中毒")),
        };
        let Some(job) = &mut runtime.job else {
            return Ok((false, false));
        };
        if job.reported {
            return Ok((false, false));
        }
        let draining = job.failure.is_some();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self)));
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                self.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("压缩扫描或布局恐慌"))
            }
        };
        let advanced = match result {
            Ok(advanced) => advanced,
            Err(error) => {
                if draining {
                    self.failed.store(true, Ordering::SeqCst);
                }
                job.fail(error);
                false
            }
        };
        // 普通失败先排空再终结动作；恐慌由统一失败关闭路径终结报告并等待关闭归还设备资源。
        if self.failed.load(Ordering::SeqCst) {
            job.report_failed(Error::InvalidState("压缩期间引擎失败关闭"))?;
            return Err(Error::InvalidState("压缩失败关闭，详见维护报告"));
        }
        let finished = job.reported;
        if finished {
            runtime.job = None;
        }
        Ok((advanced, finished))
    }
    pub(crate) fn fail_compaction(&self) -> Result<(), Error> {
        let mut runtime = match self.compaction.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("压缩任务锁中毒")),
        };
        if let Some(job) = &mut runtime.job {
            job.report_failed(Error::InvalidState("引擎或全局动作失败，压缩终止"))?;
        }
        Ok(())
    }
    /// 仅在设备关闭已经归还全部请求后清理异常任务，不能提前释放在途段租约。
    pub(crate) fn release_stopped_compaction(&self) -> Result<(), Error> {
        self.fail_compaction()?;
        self.compaction
            .lock()
            .map_err(|_| Error::InvalidState("压缩任务锁中毒"))?
            .job = None;
        Ok(())
    }
}
