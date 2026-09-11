//! 显式检查点释放：目录独占仲裁、依赖延后、先持久化失效再删除；失败不自动重试。
use super::Engine;
use crate::{
    api::maintenance::{
        CheckpointReleaseReport, MaintenanceCompleter, MaintenanceTicket, PhysicalReclamation,
    },
    checkpoint::{
        catalog::{Catalog, CatalogOptions, CatalogRead},
        catalog_lock::CatalogLock,
    },
    coordination::{Action, Phase},
    device::*,
    schema::Schema,
    types::*,
};
use std::sync::{TryLockError, atomic::Ordering};
#[derive(Default)]
pub(crate) struct ReleaseRuntime {
    job: Option<Job>,
}
#[derive(Clone, Copy)]
enum Stage {
    Lock,
    Catalog,
    Retire,
    RetirementSync,
    Delete,
    DeleteSync,
    Unlock,
    Finish,
}
struct Job {
    id: MaintenanceId,
    mailbox: RequestId,
    token: CheckpointToken,
    stage: Stage,
    lock: CatalogLock,
    reader: Option<CatalogRead>,
    catalog: Option<Catalog>,
    pending: Option<IoId>,
    retirement: CheckpointRetirement,
    material: usize,
    deleted: u64,
    failure: Option<Error>,
    complete: MaintenanceCompleter<CheckpointReleaseReport>,
    reported: bool,
}
impl Job {
    fn route(&self) -> CompletionRoute {
        super::io_hub::CompletionHub::route(self.mailbox)
    }
    fn error(&self, cause: Error) -> Error {
        Error::CheckpointReleaseFailed {
            token: self.token,
            retirement: self.retirement,
            confirmed_absent_materials: self.deleted,
            cause: Box::new(cause),
        }
    }
    fn report<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let result = if let Some(error) = self.failure.take() {
            Err(self.error(error))
        } else {
            let catalog = self.catalog.as_ref().expect("已验证释放目录");
            Ok(CheckpointReleaseReport {
                token: self.token,
                retirement: self.retirement,
                confirmed_absent_materials: self.deleted,
                physical: if catalog.blockers.is_empty() {
                    PhysicalReclamation::Completed
                } else {
                    PhysicalReclamation::DeferredByRecoverySet {
                        blockers: catalog.blockers.clone(),
                        begin: catalog.target.begin,
                        end: catalog.target.end,
                    }
                },
            })
        };
        engine
            .coordinator
            .advance(self.id, Phase::ReclaimCheckpoint)?;
        engine.coordinator.finish_action(self.id)?;
        self.complete.finish(result)?;
        self.reported = true;
        Ok(true)
    }
    fn fail<S: Schema>(&mut self, _engine: &Engine<S>) -> Result<(), Error> {
        if !self.reported {
            let cause = self
                .failure
                .take()
                .unwrap_or(Error::InvalidState("检查点释放期间引擎失败关闭"));
            self.complete.finish(Err(self.error(cause)))?;
            self.reported = true;
        }
        Ok(())
    }
    fn progress_lock<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        if let Some(completion) = engine.io.take(self.mailbox)? {
            self.lock.accept(&engine.storage, completion)?;
            return Ok(true);
        }
        match self.lock.submit_next(&engine.storage) {
            Ok(id) => Ok(id.is_some()),
            Err(Error::Busy) => Ok(false),
            Err(error) => Err(error),
        }
    }
    fn invalidate_local<S: Schema>(&self, engine: &Engine<S>) -> Result<(), Error> {
        engine
            .checkpoints
            .lock()
            .map_err(|_| Error::InvalidState("检查点目录锁中毒"))?
            .release_token(self.token, self.retirement == CheckpointRetirement::Retired);
        Ok(())
    }
    fn metadata<S: Schema>(
        &mut self,
        engine: &Engine<S>,
        operation: IoOperation,
    ) -> Result<bool, Error> {
        if let Some(completion) = engine.io.take(self.mailbox)? {
            if self.pending != Some(completion.id)
                || completion.route != self.route()
                || completion.buffer.is_some()
            {
                return Err(Error::InvalidState("释放元数据完成身份或缓冲错误"));
            }
            self.pending = None;
            match completion.result {
                Ok(IoOutcome::Done) => {}
                Err(Error::Io(error))
                    if matches!(self.stage, Stage::Delete)
                        && error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
                _ => return Err(Error::InvalidState("释放元数据完成类型错误")),
            }
            match self.stage {
                Stage::Retire => self.stage = Stage::RetirementSync,
                Stage::RetirementSync => {
                    self.retirement = CheckpointRetirement::Retired;
                    self.invalidate_local(engine)?;
                    self.stage = Stage::Delete;
                }
                Stage::Delete => self.stage = Stage::DeleteSync,
                Stage::DeleteSync => {
                    self.deleted = self.deleted.checked_add(1).ok_or(Error::CapacityExceeded)?;
                    self.material += 1;
                    self.stage = Stage::Delete;
                }
                _ => return Err(Error::InvalidState("释放元数据阶段错误")),
            }
            return Ok(true);
        }
        if self.pending.is_some() {
            return Ok(false);
        }
        match engine.storage.device.submit(IoRequest {
            route: self.route(),
            operation,
        }) {
            Ok(id) => {
                self.pending = Some(id);
                if matches!(self.stage, Stage::Retire) {
                    self.retirement = CheckpointRetirement::PossiblyRetired;
                    self.invalidate_local(engine)?;
                }
                Ok(true)
            }
            Err(rejected) if matches!(rejected.reason, Error::Busy) => Ok(false),
            Err(rejected) => Err(rejected.reason),
        }
    }
    fn step<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        if self.failure.is_some() && !matches!(self.stage, Stage::Unlock | Stage::Finish) {
            if self.pending.is_some()
                || self.reader.as_ref().is_some_and(CatalogRead::has_resources)
            {
                return Err(Error::InvalidState("释放失败仍有未确认资源"));
            }
            self.stage = Stage::Unlock;
        }
        match self.stage {
            Stage::Lock => {
                if self.lock.take_contention() {
                    self.failure = Some(Error::Busy);
                    self.lock.cancel_unacquired()?;
                    self.stage = Stage::Finish;
                    return Ok(true);
                }
                if !self.lock.held() {
                    return self.progress_lock(engine);
                }
                self.reader = Some(CatalogRead::new(
                    &engine.storage,
                    &self.lock,
                    CatalogOptions {
                        store: engine.id,
                        target: self.token,
                        max_tokens: engine.config.maintenance.max_checkpoint_tokens,
                        max_bytes: engine.config.maintenance.max_checkpoint_catalog_bytes,
                        chunk: engine.config.log.page_bytes,
                        route: self.route(),
                    },
                )?);
                self.stage = Stage::Catalog;
            }
            Stage::Catalog => {
                let reader = self.reader.as_mut().expect("目录读取者存在");
                let advanced =
                    reader.step(&engine.storage, &self.lock, engine.io.take(self.mailbox)?)?;
                if let Some(catalog) = reader.take_result(&engine.storage, &self.lock)? {
                    self.retirement = if catalog.retired {
                        CheckpointRetirement::PossiblyRetired
                    } else {
                        CheckpointRetirement::NotAttempted
                    };
                    self.stage = if !catalog.blockers.is_empty() {
                        Stage::Unlock
                    } else if catalog.retired {
                        Stage::RetirementSync
                    } else {
                        Stage::Retire
                    };
                    self.catalog = Some(catalog);
                    self.reader = None;
                }
                return Ok(advanced);
            }
            Stage::Retire => {
                return self.metadata(
                    engine,
                    IoOperation::Rename {
                        source: engine.storage.checkpoint_path(self.token, "commit")?,
                        destination: engine
                            .storage
                            .checkpoint_path(self.token, "commit.released")?,
                    },
                );
            }
            Stage::RetirementSync | Stage::DeleteSync => {
                return self.metadata(
                    engine,
                    IoOperation::SyncDirectory(
                        engine
                            .storage
                            .checkpoint_path(self.token, "commit")?
                            .parent()
                            .expect("固定目录")
                            .to_path_buf(),
                    ),
                );
            }
            Stage::Delete => {
                let catalog = self.catalog.as_ref().expect("目录已验证");
                if let Some(material) = catalog.target.materials.get(self.material) {
                    let name = crate::storage::SegmentedStorage::checkpoint_material_name(
                        material.id,
                        material.generation,
                    );
                    return self.metadata(
                        engine,
                        IoOperation::RemoveFile(engine.storage.checkpoint_path(self.token, &name)?),
                    );
                }
                self.stage = Stage::Unlock;
            }
            Stage::Unlock => {
                if self.lock.closed() {
                    self.stage = Stage::Finish;
                } else if !self.lock.held() && self.failure.is_some() {
                    // 未取得锁且没有在途尝试才允许取消；其余错误由失败关闭保存资源。
                    if self.lock.cancel_unacquired().is_ok() {
                        self.stage = Stage::Finish;
                    } else {
                        return self.progress_lock(engine);
                    }
                } else {
                    self.lock.release()?;
                    return self.progress_lock(engine);
                }
            }
            Stage::Finish => return self.report(engine),
        }
        Ok(true)
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn start_checkpoint_release(
        &self,
        token: CheckpointToken,
    ) -> Result<MaintenanceTicket<CheckpointReleaseReport>, Error> {
        token.validate()?;
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("存储已经关闭或失败"));
        }
        let caps = self.storage.device.capabilities();
        if !caps.supports_files
            || !caps.supports_file_sync
            || !caps.supports_directory_sync
            || !caps.supports_atomic_publish
            || !caps.supports_file_locks
            || !caps.supports_directory_listing
            || caps.transfer_alignment != 1
        {
            return Err(Error::UnsupportedDurability);
        }
        let mut runtime = self.checkpoint_release.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => Error::Busy,
            _ => Error::InvalidState("检查点释放任务锁中毒"),
        })?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        let mailbox = self.io.reserve(SessionId(self.id.0))?;
        let lock = match CatalogLock::new(
            &self.storage,
            super::io_hub::CompletionHub::route(mailbox),
            FileLockMode::Exclusive,
        ) {
            Ok(lock) => lock,
            Err(error) => {
                self.io.release(mailbox)?;
                return Err(error);
            }
        };
        let id = match self.coordinator.start_action(Action::ReleaseCheckpoint) {
            Ok(id) => id,
            Err(error) => {
                self.io.release(mailbox)?;
                return Err(error);
            }
        };
        let (ticket, complete) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            mailbox,
            token,
            stage: Stage::Lock,
            lock,
            reader: None,
            catalog: None,
            pending: None,
            retirement: CheckpointRetirement::NotAttempted,
            material: 0,
            deleted: 0,
            failure: None,
            complete,
            reported: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_checkpoint_release(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.checkpoint_release.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("检查点释放任务锁中毒")),
        };
        let Some(job) = &mut runtime.job else {
            return Ok((false, false));
        };
        if job.reported {
            return Ok((false, false));
        }
        let draining = job.failure.is_some();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self)))
            .unwrap_or(Err(Error::InvalidState("检查点释放推进恐慌")));
        let advanced = match result {
            Ok(advanced) => advanced,
            Err(error) => {
                if draining
                    || matches!(error, Error::InvalidState(_))
                    || matches!(job.stage, Stage::Lock | Stage::Unlock)
                {
                    self.failed.store(true, Ordering::SeqCst);
                }
                if job.failure.is_none() {
                    job.failure = Some(error);
                }
                false
            }
        };
        if self.failed.load(Ordering::SeqCst) {
            job.fail(self)?;
            return Err(Error::InvalidState("检查点释放失败关闭，详见维护报告"));
        }
        if job.reported {
            self.io.release(job.mailbox)?;
            runtime.job = None;
            return Ok((advanced, true));
        }
        Ok((advanced, false))
    }
    pub(crate) fn fail_checkpoint_release(&self) -> Result<(), Error> {
        let mut runtime = match self.checkpoint_release.try_lock() {
            Ok(runtime) => runtime,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("检查点释放任务锁中毒")),
        };
        if let Some(job) = &mut runtime.job {
            job.fail(self)?;
        }
        Ok(())
    }
    pub(crate) fn release_stopped_checkpoint_release(&self) -> Result<(), Error> {
        self.fail_checkpoint_release()?;
        let mut runtime = self
            .checkpoint_release
            .lock()
            .map_err(|_| Error::InvalidState("检查点释放任务锁中毒"))?;
        if let Some(job) = runtime.job.take() {
            self.io.release(job.mailbox)?;
        }
        Ok(())
    }
}
