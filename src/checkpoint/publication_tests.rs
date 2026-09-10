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
}
#[derive(Default)]
struct Trace {
    files: BTreeMap<u64, String>,
    pending: BTreeMap<u64, (String, Option<String>, bool)>,
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
        trace.pending.insert(
            id.0,
            (tag, opened, matches!(fault, Some(FaultMode::Complete))),
        );
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
            if fail {
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
        let storage = SegmentedStorage::new(device.clone(), root.0.clone(), 4096).unwrap();
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
