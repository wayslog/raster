//! Checkpoint actual tasks only hold materials and completion end;Not held Engine of Arc,Avoid ownership rings.
use super::{Engine, io_hub::CompletionHub};
use crate::{
    api::maintenance::{
        CheckpointKind, CheckpointReport, DurableProgress, MaintenanceCompleter, MaintenanceTicket,
    },
    checkpoint::{
        catalog_lock::CatalogLock,
        directory::{DirectoryPrepare, PreparedDirectory},
        log_material::{LogMaterialFile, LogMaterialSpec, LogMaterialWrite},
        manifest_read::ManifestRead,
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
    pub(crate) fn release_token(&mut self, token: CheckpointToken, confirmed: bool) {
        if self
            .latest_index
            .as_ref()
            .is_some_and(|index| index.token == token)
        {
            self.latest_index = None;
        }
        if confirmed {
            self.retained.retire(token);
        }
    }
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
    BaseIndex(ManifestRead),
}
enum Output {
    Directory(PreparedDirectory),
    Index(SyncedFile),
    Log(LogMaterialFile),
    Published,
    BaseIndex(Manifest),
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
            Self::BaseIndex(w) => w.accept(storage, completion),
        }
        .map_err(|rejected| rejected.reason)
    }
    fn submit(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        match self {
            Self::Directory(w) => w.submit_next(storage),
            Self::Index(w) => w.submit_next(storage),
            Self::Log(w) => w.submit_next(storage),
            Self::Publish(w) => w.submit_next(storage),
            Self::BaseIndex(w) => w.submit_next(storage),
        }
    }
    fn output(&mut self) -> Option<Result<Output, Error>> {
        match self {
            Self::Directory(w) => w.take_result().map(|r| r.map(Output::Directory)),
            Self::Index(w) => w.take_synced().map(|r| r.map(Output::Index)),
            Self::Log(w) => w.take_result().map(|r| r.map(Output::Log)),
            Self::Publish(w) => w.take_result().map(|r| r.map(|_| Output::Published)),
            Self::BaseIndex(w) => w.take_result().map(|r| r.map(Output::BaseIndex)),
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
    catalog_lock: Option<CatalogLock>,
    base_validated: bool,
}
impl Job {
    fn progress_lock<S: Schema>(&mut self, engine: &Engine<S>) -> Result<bool, Error> {
        let lock = self.catalog_lock.as_mut().expect("Directory lock created");
        if let Some(completion) = engine.io.take(self.mailbox)? {
            lock.accept(&engine.storage, completion)?;
            return Ok(true);
        }
        match lock.submit_next(&engine.storage) {
            Ok(id) => Ok(id.is_some()),
            Err(Error::Busy) => Ok(false),
            Err(error) => Err(error),
        }
    }
    fn step<S: Schema>(
        &mut self,
        engine: &Engine<S>,
        retained: &mut RetentionCatalog,
    ) -> Result<bool, Error> {
        let state = engine.coordinator.snapshot()?;
        if state.id != Some(self.id) || state.phase == Phase::Failed {
            return Err(Error::InvalidState("Checkpoint action has expired"));
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
                    Output::BaseIndex(index) => {
                        crate::format::match_recovery(&index, &self.manifest)?;
                        self.base_validated = true;
                    }
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
                // Development replay covers the entire retention log,Contains old requests that were reserved before the snapshot but were released later.
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
                    // Mutually exclusive with normal background boundary pushing,Prevent expired requests from being generated after read-only targets are pushed concurrently.
                    let _storage = match engine.storage_progress.try_lock() {
                        Ok(guard) => guard,
                        Err(TryLockError::WouldBlock) => return Ok(false),
                        Err(_) => {
                            return Err(Error::InvalidState(
                                "background_log_progress_lock_poisoned",
                            ));
                        }
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
                        self.manifest.end = self.frozen.expect("Tail fixed");
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
                    let end = self.frozen.expect("Tail fixed");
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
                // Namespace creation is also protected by shared locks,Exclusive enumeration on the release side will not miss existing directories.
                if self.catalog_lock.is_none() {
                    self.catalog_lock = Some(CatalogLock::new(
                        &engine.storage,
                        route,
                        crate::device::FileLockMode::Shared,
                    )?);
                }
                if !self
                    .catalog_lock
                    .as_ref()
                    .expect("Directory lock created")
                    .held()
                {
                    return self.progress_lock(engine);
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
                    if self.manifest.kind == Kind::Log && !self.base_validated {
                        self.work = Some(Work::BaseIndex(ManifestRead::new(
                            &engine.storage,
                            engine.id,
                            self.manifest.base_index,
                            route,
                            chunk,
                        )?));
                        return Ok(true);
                    }
                    let work = CommitPublish::new(
                        &engine.storage,
                        self.directory.take().expect("Catalog reserved"),
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
                let lock = self
                    .catalog_lock
                    .as_mut()
                    .expect("Publish holds directory lock");
                if !lock.closed() {
                    lock.release()?;
                    return self.progress_lock(engine);
                }
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
    pub(crate) fn checkpoint_capabilities(&self) -> Result<(), Error> {
        let caps = self.storage.device.capabilities();
        if !caps.supports_files
            || !caps.supports_file_sync
            || !caps.supports_directory_sync
            || !caps.supports_atomic_publish
            || !caps.supports_file_locks
            || caps.transfer_alignment != 1
        {
            return Err(Error::UnsupportedDurability);
        }
        Ok(())
    }
    pub(crate) fn start_checkpoint(
        &self,
        kind: CheckpointKind,
    ) -> Result<MaintenanceTicket<CheckpointReport>, Error> {
        let mut runtime = self.checkpoints.try_lock().map_err(lock_error)?;
        if runtime.job.is_some() {
            return Err(Error::Busy);
        }
        if self.failed.load(Ordering::SeqCst) || self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(Error::InvalidState("Engine has failed or shut down"));
        }
        self.checkpoint_capabilities()?;
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
            Error::InvalidState("Checkpoint semantics describe panics")
        })?;
        if kind_on_disk == Kind::Log {
            let base = runtime.latest_index.as_ref().ok_or(Error::InvalidState(
                "Log checkpoint requires successfully committed index checkpoint",
            ))?;
            if base.key_format != manifest.key_format
                || base.value_format != manifest.value_format
                || base.hash != manifest.hash
            {
                return Err(Error::InvalidFormat(
                    "Checkpoint semantic description changes",
                ));
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
            catalog_lock: None,
            base_validated: false,
        });
        Ok(ticket)
    }
    pub(crate) fn progress_checkpoint(&self) -> Result<(bool, bool), Error> {
        let mut runtime = match self.checkpoints.try_lock() {
            Ok(r) => r,
            Err(TryLockError::WouldBlock) => return Ok((false, false)),
            Err(_) => return Err(Error::InvalidState("checkpoint_task_lock_poisoned")),
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
                .unwrap_or(Err(Error::InvalidState("Checkpoint Advance Panic")));
        match result {
            Err(error) => {
                job.completer.finish(Err(error))?;
                job.failed = true;
                Err(Error::InvalidState(
                    "Checkpoint task failed,see_maintenance_report",
                ))
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
            Err(_) => return Err(Error::InvalidState("checkpoint_task_lock_poisoned")),
        };
        if let Some(job) = &mut runtime.job
            && !job.failed
            && !job.finished
        {
            job.completer.finish(Err(Error::InvalidState(
                "Engine or coordination action failed,Checkpoint terminated",
            )))?;
            job.failed = true;
        }
        Ok(())
    }
    /// only on device shutdown Confirm all I/O Release failed tasks and mailboxes after returning them.
    pub(crate) fn release_stopped_checkpoint(&self) -> Result<(), Error> {
        let mut runtime = self
            .checkpoints
            .lock()
            .map_err(|_| Error::InvalidState("checkpoint_task_lock_poisoned"))?;
        if let Some(job) = runtime.job.take() {
            if !job.finished && !job.failed {
                job.completer.finish(Err(Error::InvalidState(
                    "Device shuts down,Checkpoint terminated",
                )))?;
            }
            self.io.release(job.mailbox)?;
        }
        Ok(())
    }
}
fn lock_error<T>(error: TryLockError<T>) -> Error {
    match error {
        TryLockError::WouldBlock => Error::Busy,
        TryLockError::Poisoned(_) => Error::InvalidState("checkpoint_task_lock_poisoned"),
    }
}
