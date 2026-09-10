//! 使用真实文件后端注入拒绝/完成错误；格式层固定样例不冒充引擎快照。
use super::{
    directory::{DirectoryPrepare, PreparedDirectory},
    material::{MaterialWrite, SyncedFile},
    publication::{CommitPublish, PublishedCommit},
};
use crate::{
    device::*,
    format::{Commit, Manifest},
    storage::SegmentedStorage,
    types::*,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
#[derive(Clone, Copy)]
enum FaultMode {
    Busy,
    Reject,
    Complete,
    Short,
}
#[derive(Default)]
struct Trace {
    files: BTreeMap<u64, String>,
    pending: BTreeMap<u64, (String, Option<String>, Option<FaultMode>)>,
    events: Vec<String>,
    fault: Option<(String, FaultMode)>,
}
struct Controlled {
    inner: Box<dyn Device>,
    trace: Mutex<Trace>,
}
impl Device for Controlled {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let mut trace = self.trace.lock().unwrap();
        let file_name = |file: &FileId| trace.files.get(&file.slot).cloned().unwrap_or_default();
        let (tag, opened) = match &request.operation {
            IoOperation::CreateDirectory(path) => (
                format!("mkdir:{}", path.file_name().unwrap().to_string_lossy()),
                None,
            ),
            IoOperation::SyncDirectory(path) => (
                format!(
                    "syncdir:{}",
                    if path.as_os_str().is_empty() {
                        String::from(".")
                    } else {
                        path.file_name().unwrap().to_string_lossy().into_owned()
                    }
                ),
                None,
            ),
            IoOperation::Open { path, .. } => {
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                (format!("open:{name}"), Some(name))
            }
            IoOperation::Write { file, .. } => (format!("write:{}", file_name(file)), None),
            IoOperation::Read { file, .. } => (format!("read:{}", file_name(file)), None),
            IoOperation::SyncFile { file, .. } => (format!("sync:{}", file_name(file)), None),
            IoOperation::Close(file) => (format!("close:{}", file_name(file)), None),
            IoOperation::Rename { .. } => (String::from("rename"), None),
            _ => panic!("发布任务不应执行该操作"),
        };
        trace.events.push(tag.clone());
        let fault = if trace
            .fault
            .as_ref()
            .is_some_and(|(expected, _)| expected == &tag)
        {
            trace.fault.take().map(|(_, mode)| mode)
        } else {
            None
        };
        if matches!(fault, Some(FaultMode::Busy)) {
            return Err(RejectedIo {
                request,
                reason: Error::Busy,
            });
        }
        if matches!(fault, Some(FaultMode::Reject)) {
            return Err(RejectedIo {
                request,
                reason: std::io::Error::other(format!("注入拒绝 {tag}")).into(),
            });
        }
        let id = self.inner.submit(request)?;
        trace.pending.insert(id.0, (tag, opened, fault));
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
        let begin = out.len();
        self.inner.poll(budget, out)?;
        let mut trace = self.trace.lock().unwrap();
        for completion in &mut out[begin..] {
            let (tag, opened, fail) = trace.pending.remove(&completion.id.0).unwrap();
            if let (Some(name), Ok(IoOutcome::Opened(file))) = (opened, &completion.result) {
                trace.files.insert(file.slot, name);
            }
            if matches!(fail, Some(FaultMode::Short))
                && let Ok(IoOutcome::Transferred(n)) = &mut completion.result
            {
                *n = (*n).min(1);
            }
            if matches!(fail, Some(FaultMode::Complete)) {
                completion.result =
                    Err(std::io::Error::other(format!("注入完成失败 {tag}")).into());
            }
        }
        Ok(())
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    storage: SegmentedStorage,
    device: Arc<Controlled>,
    root: Directory,
}
impl Fixture {
    fn new() -> Self {
        Self::with_segment_bytes(4096)
    }
    fn with_segment_bytes(segment_bytes: u64) -> Self {
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-publish-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let inner = crate::device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }
        .open(DeviceOpenOptions {
            root: root.0.clone(),
            create_new: true,
        })
        .unwrap();
        let device = Arc::new(Controlled {
            inner,
            trace: Mutex::new(Trace::default()),
        });
        let storage = SegmentedStorage::new(device.clone(), root.0.clone(), segment_bytes).unwrap();
        Self {
            storage,
            device,
            root,
        }
    }
    fn reset(&self, fault: Option<(String, FaultMode)>) {
        let mut trace = self.device.trace.lock().unwrap();
        trace.events.clear();
        trace.fault = fault;
    }
    fn completion(&self) -> IoCompletion {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "原生发布 I/O 超时");
            let mut out = vec![];
            self.device.poll(PollBudget::default(), &mut out).unwrap();
            if !out.is_empty() {
                assert_eq!(out.len(), 1);
                return out.pop().unwrap();
            }
            std::thread::yield_now();
        }
    }
    fn read(&self, name: &str) -> Vec<u8> {
        std::fs::read(
            self.root.0.join(
                self.storage
                    .checkpoint_path(CheckpointToken([2; 16]), name)
                    .unwrap(),
            ),
        )
        .unwrap()
    }
}
trait Task {
    type Output;
    fn result(&mut self) -> Option<Result<Self::Output, Error>>;
    fn submit(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error>;
    fn accept_one(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Error>;
}
macro_rules! task {
    ($type:ty,$output:ty,$take:ident) => {
        impl Task for $type {
            type Output = $output;
            fn result(&mut self) -> Option<Result<Self::Output, Error>> {
                self.$take()
            }
            fn submit(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
                self.submit_next(storage)
            }
            fn accept_one(
                &mut self,
                storage: &SegmentedStorage,
                completion: IoCompletion,
            ) -> Result<(), Error> {
                self.accept(storage, completion).map_err(|r| r.reason)
            }
        }
    };
}
task!(DirectoryPrepare, PreparedDirectory, take_result);
task!(MaterialWrite, SyncedFile, take_synced);
task!(CommitPublish, PublishedCommit, take_result);
task!(super::manifest_read::ManifestRead, Manifest, take_result);
task!(super::read::MaterialRead, Vec<u8>, take_result);
task!(
    super::log_material::LogMaterialWrite,
    super::log_material::LogMaterialFile,
    take_result
);
fn drive<T: Task>(fixture: &Fixture, task: &mut T) -> Result<T::Output, Error> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "发布任务没有终结");
        if let Some(result) = task.result() {
            return result;
        }
        match task.submit(&fixture.storage) {
            Ok(Some(_)) => task.accept_one(&fixture.storage, fixture.completion())?,
            Ok(None) | Err(Error::Busy) => std::thread::yield_now(),
            Err(error) => return Err(error),
        }
    }
}
fn hex(text: &str) -> Vec<u8> {
    let text = text.trim();
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn manifest() -> Manifest {
    Manifest::decode(&hex(include_str!("../../tests/fixtures/p1-manifest.hex"))).unwrap()
}
fn prepare(fixture: &Fixture) -> Result<PreparedDirectory, Error> {
    drive(
        fixture,
        &mut DirectoryPrepare::new(
            &fixture.storage,
            StoreId([1; 16]),
            CheckpointToken([2; 16]),
            CompletionRoute(1),
        )?,
    )
}
fn materials(fixture: &Fixture, manifest: &Manifest) -> Vec<SyncedFile> {
    manifest
        .materials
        .iter()
        .zip([b"index".as_slice(), b"data".as_slice()])
        .map(|(material, bytes)| {
            let name = SegmentedStorage::checkpoint_material_name(material.id, material.generation);
            drive(
                fixture,
                &mut MaterialWrite::new(
                    &fixture.storage,
                    manifest.token,
                    &name,
                    bytes.to_vec(),
                    1024,
                    CompletionRoute(2),
                )
                .unwrap(),
            )
            .unwrap()
        })
        .collect()
}
#[test]
fn 目录预留同步父目录且已预留的令牌不能复用() {
    let fixture = Fixture::new();
    let _directory = prepare(&fixture).unwrap();
    let token = "02".repeat(16);
    assert_eq!(
        fixture.device.trace.lock().unwrap().events,
        vec![
            "mkdir:checkpoints".to_string(),
            "syncdir:.".into(),
            format!("mkdir:{token}"),
            "open:owner".into(),
            "write:owner".into(),
            "sync:owner".into(),
            "close:owner".into(),
            format!("syncdir:{token}"),
            "syncdir:checkpoints".into()
        ]
    );
    let owner = fixture.read("owner");
    assert_eq!(&owner[..8], b"RCLM\x01\x00\x00\x00");
    assert_eq!(owner.len(), 44);
    assert!(
        matches!(prepare(&fixture),Err(Error::Io(error)) if error.kind()==std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(fixture.read("owner"), owner);
}
#[test]
fn 原生发布写清单和提交后重命名且最后同步完成才成功() {
    let fixture = Fixture::new();
    let directory = prepare(&fixture).unwrap();
    let manifest = manifest();
    let files = materials(&fixture, &manifest);
    fixture.reset(None);
    let mut task = CommitPublish::new(
        &fixture.storage,
        directory,
        manifest.clone(),
        files,
        1024,
        CompletionRoute(3),
    )
    .unwrap();
    loop {
        assert!(task.take_result().is_none());
        if task.submit_next(&fixture.storage).unwrap().is_none() {
            continue;
        }
        let last = fixture
            .device
            .trace
            .lock()
            .unwrap()
            .events
            .last()
            .unwrap()
            .clone();
        let completion = fixture.completion();
        if last.starts_with("syncdir:") {
            assert!(task.may_be_visible());
            assert!(task.take_result().is_none());
            task.accept(&fixture.storage, completion)
                .map_err(|r| r.reason)
                .unwrap();
            break;
        }
        task.accept(&fixture.storage, completion)
            .map_err(|r| r.reason)
            .unwrap();
    }
    let report = task.take_result().unwrap().unwrap();
    assert_eq!(report.token, manifest.token);
    assert!(!task.has_resources());
    assert!(task.take_result().is_none());
    let bytes = fixture.read("manifest");
    assert_eq!(
        Commit::decode(&fixture.read("commit"))
            .unwrap()
            .verify(&bytes)
            .unwrap(),
        manifest
    );
    assert_eq!(
        fixture.read("commit"),
        hex(include_str!("../../tests/fixtures/p1-commit.hex"))
    );
    assert_eq!(
        fixture.device.trace.lock().unwrap().events,
        vec![
            "open:manifest".to_string(),
            "write:manifest".into(),
            "sync:manifest".into(),
            "close:manifest".into(),
            "open:commit.pending".into(),
            "write:commit.pending".into(),
            "sync:commit.pending".into(),
            "close:commit.pending".into(),
            "rename".into(),
            format!("syncdir:{}", "02".repeat(16))
        ]
    );
    assert!(matches!(prepare(&fixture), Err(Error::Io(_))));
    assert_eq!(fixture.read("manifest"), bytes);
}
#[test]
fn 目录创建或同步失败不产生预留凭据() {
    for tag in [
        "mkdir:checkpoints".to_string(),
        "syncdir:.".into(),
        format!("mkdir:{}", "02".repeat(16)),
        "open:owner".into(),
        "write:owner".into(),
        "sync:owner".into(),
        format!("syncdir:{}", "02".repeat(16)),
        "syncdir:checkpoints".into(),
    ] {
        let fixture = Fixture::new();
        fixture.reset(Some((tag, FaultMode::Reject)));
        assert!(prepare(&fixture).is_err());
        assert!(
            !fixture
                .root
                .0
                .join(
                    fixture
                        .storage
                        .checkpoint_path(CheckpointToken([2; 16]), "commit")
                        .unwrap()
                )
                .exists()
        );
    }
}
#[test]
fn 发布各阶段错误不报告成功且重命名后失败保留可能可见状态() {
    for tag in [
        "open:manifest".to_string(),
        "write:manifest".into(),
        "sync:manifest".into(),
        "close:manifest".into(),
        "open:commit.pending".into(),
        "write:commit.pending".into(),
        "sync:commit.pending".into(),
        "close:commit.pending".into(),
        "rename".into(),
        format!("syncdir:{}", "02".repeat(16)),
    ] {
        let fixture = Fixture::new();
        let directory = prepare(&fixture).unwrap();
        let manifest = manifest();
        let files = materials(&fixture, &manifest);
        fixture.reset(Some((tag.clone(), FaultMode::Reject)));
        let mut task = CommitPublish::new(
            &fixture.storage,
            directory,
            manifest,
            files,
            1024,
            CompletionRoute(3),
        )
        .unwrap();
        assert!(drive(&fixture, &mut task).is_err());
        assert!(task.take_result().is_none());
        assert_eq!(task.may_be_visible(), tag.starts_with("syncdir:"));
    }
    let fixture = Fixture::new();
    let directory = prepare(&fixture).unwrap();
    let manifest = manifest();
    let files = materials(&fixture, &manifest);
    fixture.reset(Some((
        format!("syncdir:{}", "02".repeat(16)),
        FaultMode::Complete,
    )));
    let mut task = CommitPublish::new(
        &fixture.storage,
        directory,
        manifest,
        files,
        1024,
        CompletionRoute(3),
    )
    .unwrap();
    assert!(drive(&fixture, &mut task).is_err());
    assert!(task.may_be_visible());
    assert!(!fixture.read("commit").is_empty());
}
#[test]
fn 缺失或错误材料凭据在写清单前拒绝() {
    for bad_digest in [false, true] {
        let fixture = Fixture::new();
        let directory = prepare(&fixture).unwrap();
        let mut manifest = manifest();
        let mut files = materials(&fixture, &manifest);
        fixture.reset(None);
        if bad_digest {
            manifest.materials[0].checksum ^= 1;
        } else {
            files.pop();
        }
        assert!(
            CommitPublish::new(
                &fixture.storage,
                directory,
                manifest,
                files,
                1024,
                CompletionRoute(3)
            )
            .is_err()
        );
        assert!(fixture.device.trace.lock().unwrap().events.is_empty());
    }
}

#[test]
fn 其他存储令牌名称或清单身份不能冒用同步材料凭据() {
    for case in 0..4 {
        let fixture = Fixture::new();
        let directory = prepare(&fixture).unwrap();
        let mut manifest = manifest();
        let mut files = materials(&fixture, &manifest);
        match case {
            0 => {
                let other = Fixture::new();
                let _ = prepare(&other).unwrap();
                files[0] = materials(&other, &manifest).remove(0);
            }
            1 => {
                let token = CheckpointToken([4; 16]);
                let _ = drive(
                    &fixture,
                    &mut DirectoryPrepare::new(
                        &fixture.storage,
                        manifest.store,
                        token,
                        CompletionRoute(4),
                    )
                    .unwrap(),
                )
                .unwrap();
                let name = SegmentedStorage::checkpoint_material_name(
                    manifest.materials[0].id,
                    manifest.materials[0].generation,
                );
                files[0] = drive(
                    &fixture,
                    &mut MaterialWrite::new(
                        &fixture.storage,
                        token,
                        &name,
                        b"index".to_vec(),
                        1024,
                        CompletionRoute(4),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            2 => {
                files[0] = drive(
                    &fixture,
                    &mut MaterialWrite::new(
                        &fixture.storage,
                        manifest.token,
                        "unexpected",
                        b"index".to_vec(),
                        1024,
                        CompletionRoute(4),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            _ => manifest.store = StoreId([9; 16]),
        }
        fixture.reset(None);
        assert!(
            CommitPublish::new(
                &fixture.storage,
                directory,
                manifest,
                files,
                1024,
                CompletionRoute(3)
            )
            .is_err()
        );
        assert!(fixture.device.trace.lock().unwrap().events.is_empty());
    }
}
#[test]
fn 目录及发布的错误身份和重复完成不推动当前状态() {
    let fixture = Fixture::new();
    let other =
        SegmentedStorage::new(fixture.device.clone(), fixture.root.0.clone(), 4096).unwrap();
    let mut directory = DirectoryPrepare::new(
        &fixture.storage,
        StoreId([1; 16]),
        CheckpointToken([2; 16]),
        CompletionRoute(1),
    )
    .unwrap();
    directory.submit_next(&fixture.storage).unwrap();
    let mut completion = fixture.completion();
    let first = completion.id;
    completion.route = CompletionRoute(99);
    let mut completion = directory
        .accept(&fixture.storage, completion)
        .unwrap_err()
        .request;
    completion.route = CompletionRoute(1);
    let completion = directory.accept(&other, completion).unwrap_err().request;
    directory
        .accept(&fixture.storage, completion)
        .map_err(|r| r.reason)
        .unwrap();
    assert!(
        directory
            .accept(
                &fixture.storage,
                IoCompletion {
                    id: first,
                    route: CompletionRoute(1),
                    result: Ok(IoOutcome::Done),
                    buffer: None
                }
            )
            .is_err()
    );
    let directory = drive(&fixture, &mut directory).unwrap();
    let manifest = manifest();
    let files = materials(&fixture, &manifest);
    fixture.reset(None);
    let mut task = CommitPublish::new(
        &fixture.storage,
        directory,
        manifest,
        files,
        1024,
        CompletionRoute(3),
    )
    .unwrap();
    let mut completion = loop {
        if task.submit_next(&fixture.storage).unwrap().is_none() {
            continue;
        }
        let completion = fixture.completion();
        if fixture
            .device
            .trace
            .lock()
            .unwrap()
            .events
            .last()
            .is_some_and(|tag| tag == "rename")
        {
            break completion;
        }
        task.accept(&fixture.storage, completion)
            .map_err(|r| r.reason)
            .unwrap();
    };
    let rename = completion.id;
    completion.route = CompletionRoute(99);
    let mut completion = task
        .accept(&fixture.storage, completion)
        .unwrap_err()
        .request;
    completion.route = CompletionRoute(3);
    let completion = task.accept(&other, completion).unwrap_err().request;
    task.accept(&fixture.storage, completion)
        .map_err(|r| r.reason)
        .unwrap();
    assert!(
        task.accept(
            &fixture.storage,
            IoCompletion {
                id: rename,
                route: CompletionRoute(3),
                result: Ok(IoOutcome::Done),
                buffer: None
            }
        )
        .is_err()
    );
    assert!(task.take_result().is_none());
    assert!(drive(&fixture, &mut task).is_ok());
}
#[test]
fn 繁忙拒绝可重试且重命名未被接受时不标记可见() {
    let fixture = Fixture::new();
    fixture.reset(Some(("mkdir:checkpoints".into(), FaultMode::Busy)));
    let mut prepare = DirectoryPrepare::new(
        &fixture.storage,
        StoreId([1; 16]),
        CheckpointToken([2; 16]),
        CompletionRoute(1),
    )
    .unwrap();
    assert!(matches!(
        prepare.submit_next(&fixture.storage),
        Err(Error::Busy)
    ));
    assert!(!prepare.has_resources());
    let directory = drive(&fixture, &mut prepare).unwrap();
    let manifest = manifest();
    let files = materials(&fixture, &manifest);
    fixture.reset(Some(("rename".into(), FaultMode::Busy)));
    let mut task = CommitPublish::new(
        &fixture.storage,
        directory,
        manifest,
        files,
        1024,
        CompletionRoute(3),
    )
    .unwrap();
    loop {
        match task.submit_next(&fixture.storage) {
            Err(Error::Busy) => break,
            Ok(Some(_)) => task
                .accept(&fixture.storage, fixture.completion())
                .map_err(|r| r.reason)
                .unwrap(),
            Ok(None) => {}
            Err(error) => panic!("意外拒绝：{error}"),
        }
    }
    assert!(!task.may_be_visible());
    assert!(task.take_result().is_none());
    assert!(drive(&fixture, &mut task).is_ok());
    assert!(task.may_be_visible());
    assert_eq!(
        fixture
            .device
            .trace
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|tag| tag.as_str() == "rename")
            .count(),
        2
    );
    let fixture = Fixture::new();
    fixture.reset(Some(("syncdir:.".into(), FaultMode::Complete)));
    assert!(self::prepare(&fixture).is_err());
}

#[test]
fn 真实索引与日志记录导出材料后可同步发布并精确读取() {
    use crate::{
        config::{IndexConfig, LogConfig},
        index::{IndexHead, MemIndex, PublishResult},
        log::HybridLog,
        schema::{
            KeyCodec,
            builtin::{AtomicU64Value, U64Key},
        },
    };
    let index = MemIndex::new(IndexConfig { buckets: 2 }).unwrap();
    let log = HybridLog::new(
        LogConfig {
            page_bytes: 512,
            memory_pages: 2,
            mutable_fraction: 0.5,
        },
        Arc::new(AtomicU64Value),
    )
    .unwrap();
    for key in [0_u64, 1, u64::MAX] {
        let hash = U64Key.hash(&key);
        let expected = index.prepare(hash).unwrap();
        let previous = match expected.head {
            IndexHead::Log(address) => Some(address),
            IndexHead::Empty => None,
            IndexHead::Cache(_) => panic!("此测试没有缓存"),
        };
        let address = log
            .finish_initialization(
                log.reserve_record(&key.to_le_bytes(), previous, key)
                    .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            index.compare_publish(expected, IndexHead::Log(address)),
            Ok(PublishResult::Published)
        ));
    }
    let bytes = index.snapshot().unwrap().encode().unwrap();
    let fixture = Fixture::new();
    let directory = prepare(&fixture).unwrap();
    let mut manifest = manifest();
    manifest.kind = manifest.materials[0].kind;
    manifest.materials.truncate(1);
    manifest.session_progress.clear();
    manifest.key_format = U64Key.format_id();
    manifest.value_format = AtomicU64Value.format_id();
    manifest.hash = U64Key.hash_descriptor();
    manifest.version = CheckpointVersion(0);
    manifest.begin = LogAddress(0);
    manifest.end = log.frontiers().unwrap().tail;
    manifest.replay_from = manifest.end;
    let material = &mut manifest.materials[0];
    material.begin = manifest.begin;
    material.end = manifest.end;
    material.bytes = bytes.len() as u64;
    material.checksum = crate::format::checksum(&bytes);
    let name = SegmentedStorage::checkpoint_material_name(material.id, material.generation);
    let receipt = drive(
        &fixture,
        &mut MaterialWrite::new(
            &fixture.storage,
            manifest.token,
            &name,
            bytes.clone(),
            17,
            CompletionRoute(2),
        )
        .unwrap(),
    )
    .unwrap();
    drive(
        &fixture,
        &mut CommitPublish::new(
            &fixture.storage,
            directory,
            manifest.clone(),
            vec![receipt],
            19,
            CompletionRoute(3),
        )
        .unwrap(),
    )
    .unwrap();
    let loaded_manifest = Commit::decode(&fixture.read("commit"))
        .unwrap()
        .verify(&fixture.read("manifest"))
        .unwrap();
    assert_eq!(loaded_manifest, manifest);
    let loaded = fixture.read(&name);
    assert_eq!(loaded, bytes);
    loaded_manifest.materials[0].verify(&loaded).unwrap();
    let snapshot = crate::format::IndexSnapshot::decode(&loaded).unwrap();
    assert_eq!(snapshot.buckets, 2);
    assert_eq!(snapshot.entries.len(), 3);
    for entry in snapshot.entries {
        log.lease(entry.address)
            .unwrap()
            .read(|value| value)
            .unwrap();
    }
    assert!(loaded_manifest.session_progress.is_empty());
}

fn frozen_log(fixture: &Fixture) -> crate::log::HybridLog<crate::schema::builtin::AtomicU64Value> {
    use crate::{config::LogConfig, log::HybridLog, schema::builtin::AtomicU64Value};
    fixture
        .storage
        .device
        .submit(IoRequest {
            route: CompletionRoute(90),
            operation: IoOperation::CreateDirectory("segments".into()),
        })
        .unwrap();
    fixture.completion().result.unwrap();
    let log = HybridLog::new(
        LogConfig {
            page_bytes: 256,
            memory_pages: 2,
            mutable_fraction: 0.5,
        },
        Arc::new(AtomicU64Value),
    )
    .unwrap();
    for value in [11, 22] {
        log.finish_initialization(
            log.reserve_record(&[value as u8], None, value)
                .unwrap()
                .with_version(CheckpointVersion(value)),
        )
        .unwrap();
    }
    let end = log.pad_tail().unwrap();
    log.advance_read_only(end).unwrap();
    let mut flush = log
        .begin_flush(&fixture.storage, CompletionRoute(91), CheckpointVersion(22))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        if log.finish_flush(&fixture.storage, &mut flush).unwrap() {
            break;
        }
        if flush.submit_next(&fixture.storage).unwrap().is_some() {
            flush
                .accept(&fixture.storage, fixture.completion())
                .unwrap();
        }
    }
    log
}
fn log_material_task(
    fixture: &Fixture,
    log: &crate::log::HybridLog<crate::schema::builtin::AtomicU64Value>,
) -> super::log_material::LogMaterialWrite {
    super::log_material::LogMaterialWrite::new(
        &fixture.storage,
        log,
        super::log_material::LogMaterialSpec {
            token: CheckpointToken([2; 16]),
            page: PageId(0),
            id: 33,
            chunk: 17,
            route: CompletionRoute(92),
        },
    )
    .unwrap()
}
#[test]
fn 原生日志材料复制保留混合版本和页填充并取得同步凭据() {
    // 292 字节页帧跨越两个 256 字节物理段。
    let fixture = Fixture::with_segment_bytes(256);
    let log = frozen_log(&fixture);
    let _directory = prepare(&fixture).unwrap();
    let expected = log
        .encode_page(PageId(0), CheckpointVersion(22))
        .unwrap()
        .bytes;
    // 移除驻留记录并释放页后，材料仍须从已写日志读出，不能依赖内存页。
    let frame = crate::format::PageFrame::decode(&expected, PageId(0), 256).unwrap();
    for (address, _) in frame.records().unwrap() {
        log.retire(address).unwrap();
    }
    log.release_page(PageId(0), Generation(0)).unwrap();
    let mut task = log_material_task(&fixture, &log);
    assert!(!task.has_resources());
    let material = drive(&fixture, &mut task).unwrap();
    assert!(!task.has_resources());
    assert!(task.take_result().is_none());
    assert!(task.submit_next(&fixture.storage).unwrap().is_none());
    assert_eq!(material.descriptor.begin, LogAddress(0));
    assert_eq!(material.descriptor.end, LogAddress(256));
    assert_eq!(material.descriptor.kind, crate::format::Kind::Log);
    let bytes = fixture.read(&material.file.name);
    assert_eq!(bytes, expected);
    material.descriptor.verify(&bytes).unwrap();
    assert_eq!(material.file.digest.bytes, material.descriptor.bytes);
    let frame = crate::format::PageFrame::decode(&bytes, PageId(0), 256).unwrap();
    let records = frame.records().unwrap();
    assert_eq!(
        records
            .iter()
            .map(|(_, r)| r.header.version.0)
            .collect::<Vec<_>>(),
        vec![11, 22]
    );
}
#[test]
fn 日志材料读写失败不返回凭据且忙拒绝可继续() {
    for (event, mode, success) in [
        (
            "read:0000000000000000-0000000000000000.log",
            FaultMode::Busy,
            true,
        ),
        (
            "read:0000000000000000-0000000000000000.log",
            FaultMode::Reject,
            false,
        ),
        (
            "read:0000000000000000-0000000000000000.log",
            FaultMode::Complete,
            false,
        ),
        (
            "write:0000000000000021-0000000000000000.material",
            FaultMode::Reject,
            false,
        ),
        (
            "sync:0000000000000021-0000000000000000.material",
            FaultMode::Complete,
            false,
        ),
    ] {
        let fixture = Fixture::new();
        let log = frozen_log(&fixture);
        let _directory = prepare(&fixture).unwrap();
        let mut task = log_material_task(&fixture, &log);
        fixture.reset(Some((event.into(), mode)));
        assert_eq!(drive(&fixture, &mut task).is_ok(), success, "{event}");
        assert!(!task.has_resources());
        assert!(task.take_result().is_none());
        assert!(task.submit_next(&fixture.storage).unwrap().is_none());
    }
}
#[test]
fn 日志材料错误路由和存储不会消费在途读取() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let log = frozen_log(&fixture);
    let _directory = prepare(&fixture).unwrap();
    let mut task = log_material_task(&fixture, &log);
    assert!(task.submit_next(&other.storage).is_err());
    let id = task.submit_next(&fixture.storage).unwrap().unwrap();
    assert!(task.has_resources());
    assert!(task.submit_next(&fixture.storage).unwrap().is_none());
    let completion = fixture.completion();
    assert_eq!(completion.id, id);
    let mut completion = task.accept(&other.storage, completion).unwrap_err().request;
    completion.route = CompletionRoute(123);
    let mut completion = task
        .accept(&fixture.storage, completion)
        .unwrap_err()
        .request;
    assert!(task.has_resources());
    completion.route = CompletionRoute(92);
    task.accept(&fixture.storage, completion).unwrap();
    assert!(!task.has_resources());
    drive(&fixture, &mut task).unwrap();
}

#[test]
fn 日志材料拒绝帧损坏及外壳校验正确但内部记录损坏() {
    for repair_frame in [false, true] {
        let fixture = Fixture::new();
        let log = frozen_log(&fixture);
        let _directory = prepare(&fixture).unwrap();
        let mut bytes = log
            .encode_page(PageId(0), CheckpointVersion(22))
            .unwrap()
            .bytes;
        bytes[32] ^= 1;
        if repair_frame {
            let n = bytes.len() - 4;
            let crc = crate::format::checksum(&bytes[..n]);
            bytes[n..].copy_from_slice(&crc.to_le_bytes());
        }
        std::fs::write(
            fixture
                .root
                .0
                .join(fixture.storage.segment_path(0, Generation(0))),
            bytes,
        )
        .unwrap();
        let mut task = log_material_task(&fixture, &log);
        fixture.reset(None);
        assert!(drive(&fixture, &mut task).is_err());
        assert!(!task.has_resources());
        assert!(
            fixture
                .device
                .trace
                .lock()
                .unwrap()
                .events
                .iter()
                .all(|event| !event.starts_with("open:"))
        );
    }
}
#[test]
fn 日志材料拒绝尚未冻结或尚未写完的源页() {
    use crate::{config::LogConfig, log::HybridLog, schema::builtin::AtomicU64Value};
    let fixture = Fixture::new();
    let log = HybridLog::new(
        LogConfig {
            page_bytes: 256,
            memory_pages: 2,
            mutable_fraction: 0.5,
        },
        Arc::new(AtomicU64Value),
    )
    .unwrap();
    assert!(matches!(
        log.checkpoint_page(PageId(0), CompletionRoute(1)),
        Err(Error::Busy)
    ));
    log.finish_initialization(log.reserve_record(b"key", None, 1).unwrap())
        .unwrap();
    let end = log.pad_tail().unwrap();
    assert!(matches!(
        log.checkpoint_page(PageId(0), CompletionRoute(1)),
        Err(Error::Busy)
    ));
    log.advance_read_only(end).unwrap();
    assert!(matches!(
        log.checkpoint_page(PageId(0), CompletionRoute(1)),
        Err(Error::Busy)
    ));
    assert!(
        log.checkpoint_page(PageId(u64::MAX), CompletionRoute(1))
            .is_err()
    );
    assert!(fixture.device.trace.lock().unwrap().events.is_empty());
}

fn recovery_file(fixture: &Fixture, bytes: &[u8]) {
    let _directory = prepare(fixture).unwrap();
    drive(
        fixture,
        &mut MaterialWrite::new(
            &fixture.storage,
            CheckpointToken([2; 16]),
            "sample",
            bytes.to_vec(),
            17,
            CompletionRoute(93),
        )
        .unwrap(),
    )
    .unwrap();
}
fn read_task(fixture: &Fixture, bytes: u64) -> super::read::MaterialRead {
    super::read::MaterialRead::new(
        &fixture.storage,
        super::read::ReadSpec {
            token: CheckpointToken([2; 16]),
            name: "sample",
            bytes,
            limit: 4096,
            chunk: 17,
            route: CompletionRoute(94),
        },
    )
    .unwrap()
}
#[test]
fn 恢复文件分块与短读精确返回且空文件必须通过结束探测() {
    for payload in [
        vec![],
        (0..1047).map(|i| (i % 251) as u8).collect::<Vec<_>>(),
    ] {
        let fixture = Fixture::new();
        recovery_file(&fixture, &payload);
        let mut task = read_task(&fixture, payload.len() as u64);
        fixture.reset(Some(("read:sample".into(), FaultMode::Short)));
        assert_eq!(drive(&fixture, &mut task).unwrap(), payload);
        assert!(!task.has_resources());
        assert!(task.take_result().is_none());
        assert!(task.submit_next(&fixture.storage).unwrap().is_none());
        assert_eq!(
            fixture.device.trace.lock().unwrap().events.last().unwrap(),
            "close:sample"
        );
        assert_eq!(fixture.read("sample"), payload);
    }
}
#[test]
fn 恢复读取拒绝截断尾随和超限声明且不创建缺失文件() {
    for expected in [0, 4, 6] {
        let fixture = Fixture::new();
        recovery_file(&fixture, b"abcde");
        let mut task = read_task(&fixture, expected);
        assert!(drive(&fixture, &mut task).is_err());
        assert!(!task.has_resources());
        assert_eq!(fixture.read("sample"), b"abcde");
    }
    let fixture = Fixture::new();
    for (bytes, limit) in [(4097, 4096), (u64::MAX, usize::MAX)] {
        assert!(
            super::read::MaterialRead::new(
                &fixture.storage,
                super::read::ReadSpec {
                    token: CheckpointToken([2; 16]),
                    name: "sample",
                    bytes,
                    limit,
                    chunk: 17,
                    route: CompletionRoute(94),
                }
            )
            .is_err()
        );
    }
    assert!(fixture.device.trace.lock().unwrap().events.is_empty());
    let _directory = prepare(&fixture).unwrap();
    let mut task = read_task(&fixture, 5);
    assert!(
        matches!(drive(&fixture, &mut task), Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound)
    );
    assert!(!task.has_resources());
    assert!(
        !fixture
            .root
            .0
            .join(
                fixture
                    .storage
                    .checkpoint_path(CheckpointToken([2; 16]), "sample")
                    .unwrap()
            )
            .exists()
    );
}
#[test]
fn 恢复读取错误路由不消费请求且关闭失败保留资源状态() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    recovery_file(&fixture, b"abcde");
    let mut task = read_task(&fixture, 5);
    assert!(task.submit_next(&other.storage).is_err());
    task.submit_next(&fixture.storage).unwrap().unwrap();
    let completion = fixture.completion();
    let mut completion = task.accept(&other.storage, completion).unwrap_err().request;
    completion.route = CompletionRoute(999);
    let mut completion = task
        .accept(&fixture.storage, completion)
        .unwrap_err()
        .request;
    assert!(task.has_resources());
    completion.route = CompletionRoute(94);
    task.accept(&fixture.storage, completion).unwrap();
    fixture.reset(Some(("close:sample".into(), FaultMode::Reject)));
    assert!(drive(&fixture, &mut task).is_err());
    assert!(task.has_resources());
    assert!(task.take_result().is_none());
    assert!(task.submit_next(&fixture.storage).unwrap().is_none());
}
#[test]
fn 恢复文件读失败仍关闭且忙拒绝不终结() {
    for mode in [FaultMode::Busy, FaultMode::Reject, FaultMode::Complete] {
        let fixture = Fixture::new();
        recovery_file(&fixture, b"abcde");
        let mut task = read_task(&fixture, 5);
        fixture.reset(Some(("read:sample".into(), mode)));
        assert_eq!(
            drive(&fixture, &mut task).is_ok(),
            matches!(mode, FaultMode::Busy)
        );
        assert!(!task.has_resources());
        assert_eq!(
            fixture.device.trace.lock().unwrap().events.last().unwrap(),
            "close:sample"
        );
    }
}

fn published_fixture(fixture: &Fixture) -> Manifest {
    let directory = prepare(fixture).unwrap();
    let manifest = manifest();
    let files = materials(fixture, &manifest);
    drive(
        fixture,
        &mut CommitPublish::new(
            &fixture.storage,
            directory,
            manifest.clone(),
            files,
            17,
            CompletionRoute(95),
        )
        .unwrap(),
    )
    .unwrap();
    manifest
}
fn manifest_reader(fixture: &Fixture, store: StoreId) -> super::manifest_read::ManifestRead {
    super::manifest_read::ManifestRead::new(
        &fixture.storage,
        store,
        CheckpointToken([2; 16]),
        CompletionRoute(96),
        17,
    )
    .unwrap()
}
#[test]
fn 恢复清单先验证提交再读取且只在关闭后返回完整描述() {
    let fixture = Fixture::new();
    let expected = published_fixture(&fixture);
    fixture.reset(None);
    let mut read = manifest_reader(&fixture, expected.store);
    assert_eq!(drive(&fixture, &mut read).unwrap(), expected);
    assert!(!read.has_resources());
    assert!(read.take_result().is_none());
    let trace = fixture.device.trace.lock().unwrap();
    let close_commit = trace
        .events
        .iter()
        .position(|e| e == "close:commit")
        .unwrap();
    let open_manifest = trace
        .events
        .iter()
        .position(|e| e == "open:manifest")
        .unwrap();
    assert!(close_commit < open_manifest);
    assert_eq!(trace.events.last().unwrap(), "close:manifest");
}
#[test]
fn 恢复拒绝错身份损坏清单和未发布提交() {
    let fixture = Fixture::new();
    let expected = published_fixture(&fixture);
    fixture.reset(None);
    assert!(drive(&fixture, &mut manifest_reader(&fixture, StoreId([9; 16]))).is_err());
    assert!(
        !fixture
            .device
            .trace
            .lock()
            .unwrap()
            .events
            .contains(&"open:manifest".to_owned())
    );
    let path = fixture.root.0.join(
        fixture
            .storage
            .checkpoint_path(expected.token, "manifest")
            .unwrap(),
    );
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[20] ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(drive(&fixture, &mut manifest_reader(&fixture, expected.store)).is_err());

    let fixture = Fixture::new();
    let directory = prepare(&fixture).unwrap();
    let expected = manifest();
    let files = materials(&fixture, &expected);
    fixture.reset(Some(("rename".into(), FaultMode::Reject)));
    assert!(
        drive(
            &fixture,
            &mut CommitPublish::new(
                &fixture.storage,
                directory,
                expected.clone(),
                files,
                17,
                CompletionRoute(95)
            )
            .unwrap()
        )
        .is_err()
    );
    assert!(!fixture.read("commit.pending").is_empty());
    assert!(drive(&fixture, &mut manifest_reader(&fixture, expected.store)).is_err());
}
#[test]
fn 恢复清单解析阶段仍拒绝其他存储且损坏提交不会触发清单分配() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let expected = published_fixture(&fixture);
    let mut read = manifest_reader(&fixture, expected.store);
    assert!(read.submit_next(&other.storage).is_err());
    assert_eq!(drive(&fixture, &mut read).unwrap(), expected);
    assert!(read.submit_next(&other.storage).is_err());
    let path = fixture.root.0.join(
        fixture
            .storage
            .checkpoint_path(expected.token, "commit")
            .unwrap(),
    );
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
    let crc = crate::format::checksum(&bytes[..52]);
    bytes[52..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).unwrap();
    fixture.reset(None);
    assert!(drive(&fixture, &mut manifest_reader(&fixture, expected.store)).is_err());
    assert!(
        !fixture
            .device
            .trace
            .lock()
            .unwrap()
            .events
            .contains(&"open:manifest".to_owned())
    );
}

#[test]
fn 冷日志继续追加和刷盘不覆盖已安装的旧页帧() {
    use crate::{config::LogConfig, log::HybridLog, schema::builtin::AtomicU64Value};
    let fixture = Fixture::new();
    let old = frozen_log(&fixture);
    let path = fixture
        .root
        .0
        .join(fixture.storage.segment_path(0, Generation(0)));
    let before = std::fs::read(&path).unwrap();
    drop(old);
    let log = HybridLog::from_checkpoint(
        LogConfig {
            page_bytes: 256,
            memory_pages: 2,
            mutable_fraction: 0.5,
        },
        Arc::new(AtomicU64Value),
        LogAddress(0),
        LogAddress(256),
    )
    .unwrap();
    let next = log
        .finish_initialization(
            log.reserve_record(b"new", Some(LogAddress(0)), 29)
                .unwrap()
                .with_version(CheckpointVersion(23)),
        )
        .unwrap();
    assert_eq!(next, LogAddress(256));
    let end = log.pad_tail().unwrap();
    log.advance_read_only(end).unwrap();
    let mut flush = log
        .begin_flush(&fixture.storage, CompletionRoute(97), CheckpointVersion(23))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !log.finish_flush(&fixture.storage, &mut flush).unwrap() {
        assert!(Instant::now() < deadline);
        if flush.submit_next(&fixture.storage).unwrap().is_some() {
            flush
                .accept(&fixture.storage, fixture.completion())
                .unwrap();
        }
    }
    let after = std::fs::read(path).unwrap();
    assert_eq!(&after[..before.len()], before);
    let frame = crate::format::PageFrame::decode(&after[before.len()..], PageId(1), 256).unwrap();
    let records = frame.records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, next);
    assert_eq!(records[0].1.header.previous, Some(LogAddress(0)));
    assert_eq!(records[0].1.header.version, CheckpointVersion(23));
    assert_eq!(records[0].1.value, 29u64.to_le_bytes());
    assert_eq!(log.frontiers().unwrap().flushed_until, LogAddress(512));
}
