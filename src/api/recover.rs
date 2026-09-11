//! 同步恢复驱动只操作新设备与独立工作日志；全部成功后才发布实例。
use super::{
    maintenance::{DurableProgress, RecoveryReport, RecoverySet},
    store::RasterKV,
};
use crate::{
    checkpoint::{
        catalog_lock::CatalogLock,
        manifest_read::ManifestRead,
        read::{MaterialRead, ReadSpec},
        recovery::{RecoveryPlan, ValidatedMaterial},
        replay::{Replay, ReplayOptions},
    },
    config::Config,
    device::*,
    engine::{Engine, io_hub::CompletionHub},
    format::{Kind, Manifest, PageFrame},
    schema::{Schema, SharedValue},
    storage::{SegmentedStorage, open::SegmentOpen, transfer::SegmentTransfer},
    types::*,
};
use std::{sync::Arc, time::Instant};
const ROUTE: CompletionRoute = CompletionRoute(0);
trait Task {
    type Output;
    fn result(&mut self) -> Option<Result<Self::Output, Error>>;
    fn submit(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error>;
    fn accept(&mut self, storage: &SegmentedStorage, completion: IoCompletion)
    -> Result<(), Error>;
}
macro_rules! task {
    ($ty:ty,$output:ty) => {
        impl Task for $ty {
            type Output = $output;
            fn result(&mut self) -> Option<Result<Self::Output, Error>> {
                self.take_result()
            }
            fn submit(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
                self.submit_next(storage)
            }
            fn accept(
                &mut self,
                storage: &SegmentedStorage,
                completion: IoCompletion,
            ) -> Result<(), Error> {
                self.accept(storage, completion).map_err(|r| r.reason)
            }
        }
    };
}
task!(ManifestRead, Manifest);
task!(MaterialRead, Vec<u8>);
task!(SegmentOpen, FileId);
task!(SegmentTransfer, ());
fn completion(
    storage: &SegmentedStorage,
    id: IoId,
    deadline: Deadline,
) -> Result<IoCompletion, Error> {
    loop {
        if deadline.expired() {
            return Err(Error::DeadlineExceeded);
        }
        let mut out = Vec::new();
        storage.device.poll(PollBudget::default(), &mut out)?;
        if !out.is_empty() {
            if out.len() != 1 || out[0].id != id || out[0].route != ROUTE {
                return Err(Error::InvalidState("恢复设备返回未匹配完成"));
            }
            return Ok(out.pop().expect("只有一个完成"));
        }
        std::thread::yield_now();
    }
}
fn drive<T: Task>(
    storage: &SegmentedStorage,
    task: &mut T,
    deadline: Deadline,
) -> Result<T::Output, Error> {
    loop {
        if let Some(result) = task.result() {
            return result;
        }
        if deadline.expired() {
            return Err(Error::DeadlineExceeded);
        }
        match task.submit(storage) {
            Ok(Some(id)) => task.accept(storage, completion(storage, id, deadline)?)?,
            Ok(None) | Err(Error::Busy) => std::thread::yield_now(),
            Err(error) => return Err(error),
        }
    }
}
fn execute(
    storage: &SegmentedStorage,
    operation: IoOperation,
    deadline: Deadline,
) -> Result<(), Error> {
    let mut request = IoRequest {
        route: ROUTE,
        operation,
    };
    let id = loop {
        if deadline.expired() {
            return Err(Error::DeadlineExceeded);
        }
        match storage.device.submit(request) {
            Ok(id) => break id,
            Err(rejected) if matches!(rejected.reason, Error::Busy) => {
                request = rejected.request;
                std::thread::yield_now();
            }
            Err(rejected) => return Err(rejected.reason),
        }
    };
    let done = completion(storage, id, deadline)?;
    if done.buffer.is_some() {
        return Err(Error::InvalidState("恢复元数据操作返回意外缓冲"));
    }
    match done.result? {
        IoOutcome::Done => Ok(()),
        _ => Err(Error::InvalidState("恢复元数据完成类型错误")),
    }
}
fn drive_catalog_lock(
    storage: &SegmentedStorage,
    lock: &mut CatalogLock,
    releasing: bool,
    deadline: Deadline,
) -> Result<(), Error> {
    loop {
        if (releasing && lock.closed()) || (!releasing && lock.held()) {
            return Ok(());
        }
        if deadline.expired() {
            return Err(Error::DeadlineExceeded);
        }
        match lock.submit_next(storage) {
            Ok(Some(id)) => lock.accept(storage, completion(storage, id, deadline)?)?,
            Ok(None) | Err(Error::Busy) => std::thread::yield_now(),
            Err(error) => return Err(error),
        }
    }
}
fn read_material(
    storage: &SegmentedStorage,
    token: CheckpointToken,
    material: &crate::format::Material,
    config: &Config,
    deadline: Deadline,
) -> Result<Vec<u8>, Error> {
    let name = SegmentedStorage::checkpoint_material_name(material.id, material.generation);
    let limit = if material.kind == Kind::Index {
        config.recovery.max_index_bytes
    } else {
        PageFrame::encoded_size(config.log.page_bytes)?
    };
    let mut read = MaterialRead::new(
        storage,
        ReadSpec {
            token,
            name: &name,
            bytes: material.bytes,
            limit,
            chunk: config.log.page_bytes,
            route: ROUTE,
        },
    )?;
    let bytes = drive(storage, &mut read, deadline)?;
    material.verify(&bytes)?;
    Ok(bytes)
}
fn write_page(
    storage: &SegmentedStorage,
    page: PageId,
    page_bytes: usize,
    bytes: Vec<u8>,
    deadline: Deadline,
) -> Result<(), Error> {
    let mut transfer =
        SegmentTransfer::write(PageFrame::physical_offset(page, page_bytes)?, bytes, ROUTE)?;
    loop {
        if let Some(result) = transfer.take_result() {
            return result;
        }
        if deadline.expired() {
            return Err(Error::DeadlineExceeded);
        }
        if let Some(address) = transfer.next_address()? {
            match storage.resolve(address) {
                Ok(_) => {}
                Err(Error::RangeTruncated) => {
                    let number = address.0 / storage.segment_bytes;
                    let mut open = SegmentOpen::new(
                        storage,
                        number,
                        storage.generation(number)?,
                        true,
                        ROUTE,
                    )?;
                    drive(storage, &mut open, deadline)?;
                }
                Err(error) => return Err(error),
            }
        }
        match transfer.submit_next(storage) {
            Ok(Some(id)) => transfer
                .accept(storage, completion(storage, id, deadline)?)
                .map_err(|r| r.reason)?,
            Ok(None) | Err(Error::Busy) => std::thread::yield_now(),
            Err(error) => return Err(error),
        }
    }
}
pub(crate) fn recover<S: Schema>(
    config: Config,
    schema: S,
    factory: Box<dyn DeviceFactory>,
    set: RecoverySet,
) -> Result<(RasterKV<S>, RecoveryReport), Error> {
    config.validate()?;
    set.store.validate()?;
    set.index.validate()?;
    set.log.validate()?;
    if config.maintenance.auto_compaction || config.storage.pre_allocate_log {
        return Err(Error::unimplemented("engine::高级配置"));
    }
    let deadline = Deadline(
        Instant::now()
            .checked_add(config.recovery.timeout)
            .ok_or(Error::CapacityExceeded)?,
    );
    let device: Arc<dyn Device> = Arc::from(factory.open(DeviceOpenOptions {
        root: config.storage.root.clone(),
        create_new: false,
    })?);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build(config, schema, device.clone(), set, deadline)
    }))
    .unwrap_or(Err(Error::InvalidState("恢复过程恐慌")));
    if result.is_err() {
        // 失败不发布引擎；设备拥有在途缓冲，先请求排空，最后一个 Arc 的析构负责兜底。
        let _ = device.shutdown(Deadline(
            Instant::now() + std::time::Duration::from_secs(30),
        ));
    }
    result
}
fn build<S: Schema>(
    config: Config,
    schema: S,
    device: Arc<dyn Device>,
    set: RecoverySet,
    deadline: Deadline,
) -> Result<(RasterKV<S>, RecoveryReport), Error> {
    let caps = device.capabilities();
    if !caps.supports_files
        || !caps.supports_file_sync
        || !caps.supports_directory_sync
        || !caps.supports_atomic_publish
        || !caps.supports_file_locks
    {
        return Err(Error::UnsupportedDurability);
    }
    let storage = SegmentedStorage::recovered(
        device,
        config.storage.root.clone(),
        config.storage.segment_bytes,
        CheckpointToken::generate()?,
    )?;
    let mut catalog_lock = CatalogLock::new(&storage, ROUTE, FileLockMode::Shared)?;
    drive_catalog_lock(&storage, &mut catalog_lock, false, deadline)?;
    let index_manifest = drive(
        &storage,
        &mut ManifestRead::new(&storage, set.store, set.index, ROUTE, config.log.page_bytes)?,
        deadline,
    )?;
    let log_manifest = if set.index == set.log {
        index_manifest.clone()
    } else {
        drive(
            &storage,
            &mut ManifestRead::new(&storage, set.store, set.log, ROUTE, config.log.page_bytes)?,
            deadline,
        )?
    };
    let schema = Arc::new(schema);
    let mut plan = RecoveryPlan::new(&set, index_manifest, log_manifest, &*schema, &config)?;
    let log = crate::log::HybridLog::from_checkpoint(
        config.log.clone(),
        Arc::new(SharedValue(schema.clone())),
        plan.log().begin,
        plan.log().end,
    )?;
    let requests: Vec<_> = plan
        .requests()
        .map(|(token, material)| (token, material.clone()))
        .collect();
    let mut source_index = None;
    for (token, material) in requests {
        let bytes = read_material(&storage, token, &material, &config, deadline)?;
        match plan.verify_material(token, material.id, &bytes, &*schema)? {
            ValidatedMaterial::Index(image) if token == set.index => source_index = Some(image),
            ValidatedMaterial::Index(_) => {}
            ValidatedMaterial::Log(frame) => {
                for (_, record) in frame.records()? {
                    if !record.header.invalid && !record.header.tombstone {
                        drop(log.decode_temporary(record.value)?);
                    }
                }
            }
        }
    }
    if plan.remaining() != 0 {
        return Err(Error::InvalidState("恢复材料尚未全部验证"));
    }
    let source_index = source_index.ok_or(Error::InvalidFormat("恢复集合缺少索引材料"))?;
    let mut replay = Replay::new(ReplayOptions {
        begin: plan.log().begin,
        end: plan.log().end,
        page_bytes: config.log.page_bytes,
        version: plan.log().version,
        buckets: config.index.buckets,
        generation: source_index.generation,
        max_records: config.recovery.max_records,
    })?;
    for material in plan.log().materials.iter().filter(|m| m.kind == Kind::Log) {
        let bytes = read_material(&storage, set.log, material, &config, deadline)?;
        let rewritten = replay.page(&bytes, schema.key_codec())?;
        write_page(
            &storage,
            material.begin.page_offset(config.log.page_bytes as u64)?.0,
            config.log.page_bytes,
            rewritten,
            deadline,
        )?;
    }
    for entry in source_index.entries {
        replay.resolve_head(entry.address, entry.tag)?;
    }
    let mut index = crate::index::MemIndex::new(config.index.clone())?;
    index.restore(replay.index()?)?;
    let coordinator = crate::coordination::Coordinator::from_checkpoint(
        config.session.max_sessions,
        plan.log().version,
        &plan.log().session_progress,
    )?;
    let files = storage.bound_files()?;
    for file in &files {
        execute(
            &storage,
            IoOperation::SyncFile {
                file: *file,
                metadata: true,
            },
            deadline,
        )?;
    }
    if !files.is_empty() {
        execute(
            &storage,
            IoOperation::SyncDirectory(storage.segment_directory()),
            deadline,
        )?;
        execute(
            &storage,
            IoOperation::SyncDirectory(std::path::PathBuf::new()),
            deadline,
        )?;
    }
    let report = RecoveryReport {
        set: set.clone(),
        version: plan.log().version,
        sessions: plan
            .log()
            .session_progress
            .iter()
            .map(|&(session, serial)| DurableProgress {
                session,
                serial,
                version: plan.log().version,
            })
            .collect(),
    };
    let io_capacity = config.io_capacity()?;
    catalog_lock.release()?;
    drive_catalog_lock(&storage, &mut catalog_lock, true, deadline)?;
    let engine = Engine {
        id: set.store,
        scans: Default::default(),
        compaction: std::sync::Mutex::new(Default::default()),
        gc: std::sync::Mutex::new(Default::default()),
        checkpoint_release: std::sync::Mutex::new(Default::default()),
        io: Arc::new(CompletionHub::new(set.store, io_capacity)?),
        growth: std::sync::Mutex::new(Default::default()),
        checkpoints: std::sync::Mutex::new(
            crate::engine::checkpoint::CheckpointRuntime::recovered(plan.index(), plan.log())?,
        ),
        storage_progress: std::sync::Mutex::new(Default::default()),
        schema,
        index,
        log,
        coordinator,
        storage,
        epoch: crate::epoch::EpochManager::new()?,
        cache: crate::cache::ReadCache::new(config.cache.clone()),
        config,
        shutdown_state: crate::sync::Mutex::new(false),
        version_permits: Default::default(),
        operations: (0..64).map(|_| crate::sync::Mutex::new(())).collect(),
        failed: false.into(),
        shutdown_requested: false.into(),
    };
    Ok((
        RasterKV {
            inner: Arc::new(engine),
        },
        report,
    ))
}
