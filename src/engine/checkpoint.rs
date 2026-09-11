//! 检查点实际任务只持有材料和完成端；不持有 Engine 的 Arc，避免所有权环。
use super::{Engine, io_hub::CompletionHub};
use crate::{
    api::maintenance::{
        CheckpointKind, CheckpointReport, DurableProgress, MaintenanceCompleter, MaintenanceTicket,
    },
    checkpoint::{
        directory::{DirectoryPrepare, PreparedDirectory},
        log_material::{LogMaterialFile, LogMaterialSpec, LogMaterialWrite},
        material::{MaterialWrite, SyncedFile},
        publication::CommitPublish,
        retention::RetentionCatalog,
    },
    coordination::{Action, Phase},
    device::IoCompletion,
    format::{Kind, Manifest, Material, checksum},
    schema::{KeyCodec, Schema, ValueLayout},
    storage::SegmentedStorage,
    types::*,
};
use std::sync::{TryLockError, atomic::Ordering};

#[derive(Default)]
pub(crate) struct CheckpointRuntime {
    job: Option<Job>,
    latest_index: Option<Manifest>,
    pub(crate) retained: RetentionCatalog,
}
impl CheckpointRuntime {
    pub(crate) fn invalidate_before(&mut self, begin: LogAddress) {
        if self
            .latest_index
            .as_ref()
            .is_some_and(|index| index.begin < begin)
        {
            self.latest_index = None;
        }
    }

    pub(crate) fn recovered(index: &Manifest, log: &Manifest) -> Result<Self, Error> {
        let mut retained = RetentionCatalog::default();
        retained.record_committed(index)?;
        if index.token != log.token {
            retained.record_committed(log)?;
        }
        Ok(Self {
            job: None,
            latest_index: Some(index.clone()),
            retained,
        })
    }
}
enum Work {
    Directory(DirectoryPrepare),
    Index(MaterialWrite),
    Log(LogMaterialWrite),
    Publish(CommitPublish),
}
enum Output {
    Directory(PreparedDirectory),
    Index(SyncedFile),
    Log(LogMaterialFile),
    Published,
}
impl Work {
    fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Error> {
        match self {
            Self::Directory(w) => w.accept(storage, completion),
            Self::Index(w) => w.accept(storage, completion),
            Self::Log(w) => w.accept(storage, completion),
            Self::Publish(w) => w.accept(storage, completion),
        }
        .map_err(|rejected| rejected.reason)
    }
    fn submit(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        match self {
            Self::Directory(w) => w.submit_next(storage),
            Self::Index(w) => w.submit_next(storage),
            Self::Log(w) => w.submit_next(storage),
            Self::Publish(w) => w.submit_next(storage),
        }
    }
    fn output(&mut self) -> Option<Result<Output, Error>> {
        match self {
            Self::Directory(w) => w.take_result().map(|r| r.map(Output::Directory)),
            Self::Index(w) => w.take_synced().map(|r| r.map(Output::Index)),
            Self::Log(w) => w.take_result().map(|r| r.map(Output::Log)),
            Self::Publish(w) => w.take_result().map(|r| r.map(|_| Output::Published)),
        }
    }
}
struct Job {
    id: MaintenanceId,
    mailbox: RequestId,
    kind: CheckpointKind,
    manifest: Manifest,
    completer: MaintenanceCompleter<CheckpointReport>,
    index: Option<Vec<u8>>,
    frozen: Option<LogAddress>,
    next_page: u64,
    directory: Option<PreparedDirectory>,
    files: Vec<SyncedFile>,
    work: Option<Work>,
    published: bool,
    finished: bool,
    failed: bool,
}
impl Job {
    fn step<S: Schema>(
        &mut self,
        engine: &Engine<S>,
        retained: &mut RetentionCatalog,
    ) -> Result<bool, Error> {
        let state = engine.coordinator.snapshot()?;
        if state.id != Some(self.id) || state.phase == Phase::Failed {
            return Err(Error::InvalidState("检查点动作已失效"));
        }
        if let Some(work) = &mut self.work {
            if let Some(completion) = engine.io.take(self.mailbox)? {
                work.accept(&engine.storage, completion)?;
            }
            if let Some(output) = work.output() {
                match output? {
                    Output::Directory(directory) => self.directory = Some(directory),
                    Output::Index(file) => self.files.push(file),
                    Output::Log(file) => {
                        self.files.push(file.file);
                        self.manifest.materials.push(file.descriptor);
                        self.next_page += 1;
                    }
                    Output::Published => self.published = true,
                }
                self.work = None;
                return Ok(true);
            }
            return match work.submit(&engine.storage) {
                Ok(id) => Ok(id.is_some()),
                Err(Error::Busy) => Ok(false),
                Err(error) => Err(error),
            };
        }
        let route = CompletionHub::route(self.mailbox);
        let chunk = engine.config.log.page_bytes;
        match state.phase {
            Phase::IndexSnapshot => {
                let before = engine.log.frontiers()?;
                let bytes = match engine.snapshot_index() {
                    Ok(bytes) => bytes,
                    Err(Error::Busy) => return Ok(false),
                    Err(error) => return Err(error),
                };
                let after = engine.log.frontiers()?;
                self.manifest.begin = before.begin;
                // 开发期重放覆盖整个保留日志，包含快照前已预留但稍后发布的旧请求。
                self.manifest.replay_from = before.begin;
                self.manifest.end = after.tail;
                self.manifest.materials.push(Material {
                    id: 0,
                    generation: Generation(0),
                    kind: Kind::Index,
                    begin: before.begin,
                    end: after.tail,
                    bytes: bytes.len() as u64,
                    checksum: checksum(&bytes),
                });
                self.index = Some(bytes);
                engine.coordinator.advance(self.id, Phase::IndexSnapshot)?;
                Ok(true)
            }
            Phase::WaitFlush => {
                if self.manifest.kind != Kind::Index {
                    // 与普通后台边界推进互斥，防止只读目标被并发推进后产生过期请求。
                    let _storage = match engine.storage_progress.try_lock() {
                        Ok(guard) => guard,
                        Err(TryLockError::WouldBlock) => return Ok(false),
                        Err(_) => return Err(Error::InvalidState("后台日志推进锁中毒")),
                    };
                    if self.frozen.is_none() {
                        let cuts = match engine.coordinator.cuts(self.id) {
                            Ok(cuts) => cuts,
                            Err(Error::Busy) => return Ok(false),
                            Err(error) => return Err(error),
                        };
                        if cuts.iter().any(|cut| cut.old_pending != 0) {
                            return Ok(false);
                        }
                        self.manifest.session_progress = cuts
                            .into_iter()
                            .filter_map(|cut| cut.last_accepted.map(|serial| (cut.session, serial)))
                            .collect();
                        self.frozen = Some(match engine.log.pad_tail() {
                            Ok(end) => end,
                            Err(Error::Busy) => return Ok(false),
                            Err(error) => return Err(error),
                        });
                        self.manifest.end = self.frozen.expect("已固定尾部");
                        if self.manifest.begin < engine.log.frontiers()?.begin {
                            return Err(Error::RangeTruncated);
                        }
                        for material in &mut self.manifest.materials {
                            material.end = self.manifest.end;
                        }
                        self.next_page = self.manifest.begin.0 / chunk as u64;
                        let pages = self.manifest.end.0 / chunk as u64 - self.next_page;
                        let count = usize::try_from(pages).map_err(|_| Error::CapacityExceeded)?;
                        if count
                            .checked_add(self.manifest.materials.len())
                            .is_none_or(|n| n > crate::format::MAX_MANIFEST_ITEMS)
                        {
                            return Err(Error::CapacityExceeded);
                        }
                        self.manifest
                            .materials
                            .try_reserve(count)
                            .map_err(|_| Error::OutOfMemory)?;
                        self.files
                            .try_reserve(count + 1)
                            .map_err(|_| Error::OutOfMemory)?;
                    }
                    let end = self.frozen.expect("已固定尾部");
                    let frontiers = engine.log.frontiers()?;
                    if frontiers.safe_read_only < end {
                        match engine.log.advance_read_only(end.max(frontiers.read_only)) {
                            Ok(()) => {}
                            Err(Error::Busy) => return Ok(false),
                            Err(error) => return Err(error),
                        }
                    }
                    if engine.log.frontiers()?.flushed_until < end {
                        return Ok(false);
                    }
                }
                if self.directory.is_none() {
                    self.work = Some(Work::Directory(DirectoryPrepare::new(
                        &engine.storage,
                        engine.id,
                        self.manifest.token,
                        route,
                    )?));
                } else if let Some(bytes) = self.index.take() {
                    self.files.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                    let name = SegmentedStorage::checkpoint_material_name(0, Generation(0));
                    self.work = Some(Work::Index(MaterialWrite::new(
                        &engine.storage,
                        self.manifest.token,
                        &name,
                        bytes,
                        chunk,
                        route,
                    )?));
                } else if self.manifest.kind != Kind::Index
                    && self.next_page < self.manifest.end.0 / chunk as u64
                {
                    self.work = Some(Work::Log(LogMaterialWrite::new(
                        &engine.storage,
                        &engine.log,
                        LogMaterialSpec {
                            token: self.manifest.token,
                            page: PageId(self.next_page),
                            id: self
                                .next_page
                                .checked_add(1)
                                .ok_or(Error::CapacityExceeded)?,
                            chunk,
                            route,
                        },
                    )?));
                } else {
                    let work = CommitPublish::new(
                        &engine.storage,
                        self.directory.take().expect("已预留目录"),
                        self.manifest.clone(),
                        std::mem::take(&mut self.files),
                        chunk,
                        route,
                    )?;
                    engine.coordinator.advance(self.id, Phase::WaitFlush)?;
                    self.work = Some(Work::Publish(work));
                }
                Ok(true)
            }
            Phase::Publish if self.published => {
                let report = CheckpointReport {
                    kind: self.kind,
                    token: self.manifest.token,
                    version: self.manifest.version,
                    begin: self.manifest.begin,
                    end: self.manifest.end,
                    sessions: self
                        .manifest
                        .session_progress
                        .iter()
                        .map(|(session, serial)| DurableProgress {
                            session: *session,
                            serial: *serial,
                            version: self.manifest.version,
                        })
                        .collect(),
                };
                retained.record_committed(&self.manifest)?;
                engine.coordinator.finish_action(self.id)?;
                self.completer.finish(Ok(report))?;
                self.finished = true;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn start_checkpoint(
        &self,
        kind: CheckpointKind,
    ) -> Result<MaintenanceTicket<CheckpointReport>, Error> {
        let mut runtime = self.checkpoints.try_lock().map_err(lock_error)?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("引擎已失败或关闭"));
        }
        let caps = self.storage.device.capabilities();
        if !caps.supports_files
            || !caps.supports_file_sync
            || !caps.supports_directory_sync
            || !caps.supports_atomic_publish
            || caps.transfer_alignment != 1
        {
            return Err(Error::UnsupportedDurability);
        }
        let token = CheckpointToken::generate()?;
        let current = self.coordinator.snapshot()?;
        let (kind_on_disk, action) = match kind {
            CheckpointKind::Full => (Kind::Full, Action::CheckpointFull),
            CheckpointKind::Index => (Kind::Index, Action::CheckpointIndex),
            CheckpointKind::Log => (Kind::Log, Action::CheckpointLog),
        };
        let mut manifest = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Manifest {
            store: self.id,
            token,
            kind: kind_on_disk,
            base_index: token,
            key_format: self.schema.key_codec().format_id(),
            value_format: self.schema.value_layout().format_id(),
            hash: self.schema.key_codec().hash_descriptor(),
            version: current.version,
            begin: LogAddress(0),
            end: LogAddress(0),
            replay_from: LogAddress(0),
            session_progress: vec![],
            materials: vec![],
        }))
        .map_err(|_| {
            self.failed.store(true, Ordering::SeqCst);
            Error::InvalidState("检查点语义描述恐慌")
        })?;
        if kind_on_disk == Kind::Log {
            let base = runtime
                .latest_index
                .as_ref()
                .ok_or(Error::InvalidState("日志检查点需要已成功提交的索引检查点"))?;
            if base.key_format != manifest.key_format
                || base.value_format != manifest.value_format
                || base.hash != manifest.hash
            {
                return Err(Error::InvalidFormat("检查点语义描述发生变化"));
            }
            manifest.base_index = base.token;
            manifest.begin = base.begin;
            manifest.replay_from = base.replay_from;
        }
        let mailbox = self.io.reserve(SessionId(self.id.0))?;
        let id = match self.coordinator.start_action(action) {
            Ok(id) => id,
            Err(error) => {
                self.io.release(mailbox)?;
                return Err(error);
            }
        };
        let (ticket, completer) = MaintenanceTicket::pair(self.id, id);
        runtime.job = Some(Job {
            id,
            mailbox,
            kind,
            manifest,
            completer,
            index: None,
            frozen: None,
            next_page: 0,
            directory: None,
            files: vec![],
            work: None,
            published: false,
            finished: false,
            failed: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_checkpoint(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.checkpoints.try_lock() {
            Ok(r) => r,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("检查点任务锁中毒")),
        };
        let CheckpointRuntime { job, retained, .. } = &mut *runtime;
        let Some(job) = job else {
            return Ok((false, false));
        };
        if job.failed {
            return Ok((false, false));
        }
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(self, retained)))
                .unwrap_or(Err(Error::InvalidState("检查点推进恐慌")));
        match result {
            Err(error) => {
                job.completer.finish(Err(error))?;
                job.failed = true;
                Err(Error::InvalidState("检查点任务失败，详见维护报告"))
            }
            Ok(progress) => {
                let finished = job.finished;
                if finished {
                    self.io.release(job.mailbox)?;
                    if job.manifest.kind != Kind::Log {
                        runtime.latest_index = Some(job.manifest.clone());
                    }
                    runtime.job = None;
                }
                Ok((progress, finished))
            }
        }
    }
    pub(crate) fn fail_checkpoint(&self) -> Result<(), Error> {
        let mut runtime = match self.checkpoints.try_lock() {
            Ok(r) => r,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(_) => return Err(Error::InvalidState("检查点任务锁中毒")),
        };
        if let Some(job) = &mut runtime.job
            && !job.failed
            && !job.finished
        {
            job.completer
                .finish(Err(Error::InvalidState("引擎或协调动作失败，检查点终止")))?;
            job.failed = true;
        }
        Ok(())
    }
    /// 仅在设备 shutdown 确认所有 I/O 已归还后解除失败任务与邮箱。
    pub(crate) fn release_stopped_checkpoint(&self) -> Result<(), Error> {
        let mut runtime = self
            .checkpoints
            .lock()
            .map_err(|_| Error::InvalidState("检查点任务锁中毒"))?;
        if let Some(job) = runtime.job.take() {
            if !job.finished && !job.failed {
                job.completer
                    .finish(Err(Error::InvalidState("设备关闭，检查点终止")))?;
            }
            self.io.release(job.mailbox)?;
        }
        Ok(())
    }
}
fn lock_error<T>(error: TryLockError<T>) -> Error {
    match error {
        TryLockError::WouldBlock => Error::Busy,
        TryLockError::Poisoned(_) => Error::InvalidState("检查点任务锁中毒"),
    }
}
