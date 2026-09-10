//! 单个不可覆盖材料文件的写入与文件同步；目录同步和提交发布由上层负责。
use crate::{device::*, storage::SegmentedStorage, types::*};
use std::{path::PathBuf, sync::Arc};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Open,
    Write,
    Sync,
    Close,
    Done,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WrittenMaterial {
    pub bytes: u64,
    pub checksum: u32,
}
/// 只有成功写入、同步并关闭后才能从材料任务取得的文件凭据。
pub(crate) struct SyncedFile {
    pub(super) owner: Arc<()>,
    pub(super) token: CheckpointToken,
    pub(super) name: String,
    pub(super) digest: WrittenMaterial,
}
pub(crate) struct MaterialWrite {
    owner: Arc<()>,
    path: PathBuf,
    token: CheckpointToken,
    name: String,
    route: CompletionRoute,
    bytes: Vec<u8>,
    chunk: usize,
    cursor: usize,
    file: Option<FileId>,
    pending: Option<(IoId, usize)>,
    stage: Stage,
    receipt: WrittenMaterial,
    failure: Option<Error>,
    result: Option<Result<WrittenMaterial, Error>>,
}
impl MaterialWrite {
    /// 父目录须先由检查点任务创建；这里只建立新文件，不能覆盖既有材料。
    pub fn new(
        storage: &SegmentedStorage,
        token: CheckpointToken,
        name: &str,
        bytes: Vec<u8>,
        chunk: usize,
        route: CompletionRoute,
    ) -> Result<Self, Error> {
        let caps = storage.device.capabilities();
        if !caps.supports_files || !caps.supports_file_sync || caps.transfer_alignment != 1 {
            return Err(Error::UnsupportedDurability);
        }
        if chunk == 0 || !caps.memory_alignment.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "checkpoint.chunk",
                reason: "块大小须非零且设备内存对齐有效",
            });
        }
        let path = storage.checkpoint_path(token, name)?;
        let receipt = WrittenMaterial {
            bytes: u64::try_from(bytes.len()).map_err(|_| Error::CapacityExceeded)?,
            checksum: crate::format::checksum(&bytes),
        };
        Ok(Self {
            owner: storage.identity.clone(),
            path,
            token,
            name: name.to_owned(),
            route,
            bytes,
            chunk,
            cursor: 0,
            file: None,
            pending: None,
            stage: Stage::Open,
            receipt,
            failure: None,
            result: None,
        })
    }
    fn check_owner(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if Arc::ptr_eq(&self.owner, &storage.identity) {
            Ok(())
        } else {
            Err(Error::InvalidState("材料写入属于其他存储"))
        }
    }
    fn fail(&mut self, error: Error) {
        self.failure.get_or_insert(error);
        if self.file.is_some() && self.stage != Stage::Close {
            self.stage = Stage::Close;
        } else {
            self.stage = Stage::Done;
            self.result = Some(Err(self.failure.take().expect("保存首个错误")));
        }
    }
    /// Busy 可以稍后重试；其他拒绝转入关闭清理，由调用者继续驱动至结果终结。
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        self.check_owner(storage)?;
        if self.pending.is_some() || self.stage == Stage::Done {
            return Ok(None);
        }
        let mut length = 0;
        let operation = match self.stage {
            Stage::Open => IoOperation::Open {
                path: self.path.clone(),
                create_new: true,
            },
            Stage::Write => {
                length = self.chunk.min(self.bytes.len() - self.cursor);
                let mut buffer = match AlignedBuffer::new_zeroed(
                    length,
                    storage.device.capabilities().memory_alignment,
                ) {
                    Ok(buffer) => buffer,
                    Err(error) => {
                        self.fail(error);
                        return Ok(None);
                    }
                };
                buffer
                    .as_mut_slice()
                    .copy_from_slice(&self.bytes[self.cursor..self.cursor + length]);
                IoOperation::Write {
                    file: self.file.expect("材料已打开"),
                    offset: self.cursor as u64,
                    buffer,
                }
            }
            Stage::Sync => IoOperation::SyncFile {
                file: self.file.expect("材料已打开"),
                metadata: true,
            },
            Stage::Close => IoOperation::Close(self.file.expect("关闭时保留句柄")),
            Stage::Done => return Ok(None),
        };
        match storage.device.submit(IoRequest {
            route: self.route,
            operation,
        }) {
            Ok(id) => {
                self.pending = Some((id, length));
                Ok(Some(id))
            }
            Err(rejected) => match rejected.reason {
                Error::Busy => Err(Error::Busy),
                error => {
                    self.fail(error);
                    Ok(None)
                }
            },
        }
    }
    #[allow(clippy::result_large_err, reason = "错误身份必须归还原完成与缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if self.check_owner(storage).is_err()
            || self.pending.is_none_or(|(id, _)| id != completion.id)
            || completion.route != self.route
        {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("材料完成身份不匹配"),
            });
        }
        let (_, length) = self.pending.take().expect("在途身份已匹配");
        match (self.stage, completion.result) {
            (Stage::Open, Ok(IoOutcome::Opened(file))) => {
                self.file = Some(file);
                if completion.buffer.is_some() {
                    self.fail(Error::InvalidState("打开材料返回了意外缓冲"));
                } else {
                    self.stage = if self.bytes.is_empty() {
                        Stage::Sync
                    } else {
                        Stage::Write
                    };
                }
            }
            (Stage::Write, Ok(IoOutcome::Transferred(written))) => {
                if written == 0 {
                    self.fail(Error::Io(std::io::ErrorKind::WriteZero.into()));
                } else if written > length
                    || completion.buffer.as_ref().is_none_or(|b| b.len() != length)
                {
                    self.fail(Error::InvalidState("材料短写长度或缓冲不匹配"));
                } else {
                    self.cursor += written;
                    if self.cursor == self.bytes.len() {
                        self.stage = Stage::Sync;
                    }
                }
            }
            (Stage::Sync, Ok(IoOutcome::Done)) if completion.buffer.is_none() => {
                self.stage = Stage::Close
            }
            (Stage::Close, Ok(IoOutcome::Done)) if completion.buffer.is_none() => {
                self.file = None;
                self.stage = Stage::Done;
                self.result = Some(match self.failure.take() {
                    Some(error) => Err(error),
                    None => Ok(self.receipt),
                });
            }
            (_, Err(error)) => self.fail(error),
            _ => self.fail(Error::InvalidState("材料完成类型错误")),
        }
        Ok(())
    }
    pub fn take_synced(&mut self) -> Option<Result<SyncedFile, Error>> {
        self.result.take().map(|result| {
            result.map(|digest| SyncedFile {
                owner: self.owner.clone(),
                token: self.token,
                name: self.name.clone(),
                digest,
            })
        })
    }
    pub fn take_result(&mut self) -> Option<Result<WrittenMaterial, Error>> {
        self.result.take()
    }
    /// Drop 不发起阻塞 I/O；拥有者须保留任务至完成，或由设备 shutdown 接管资源。
    pub fn has_resources(&self) -> bool {
        self.pending.is_some() || self.file.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    #[derive(Default)]
    struct State {
        next: u64,
        queued: Option<(IoId, IoRequest)>,
        events: Vec<&'static str>,
        bytes: Vec<u8>,
        open: bool,
        short: Option<usize>,
        fail: Option<&'static str>,
        reject: bool,
    }
    // 仅模拟完成顺序和错误，不提供持久化证据；另有真实文件后端验收。
    #[derive(Default)]
    struct ScriptDevice(Mutex<State>);
    impl Device for ScriptDevice {
        fn capabilities(&self) -> DeviceCapabilities {
            DeviceCapabilities {
                supports_files: true,
                memory_alignment: 8,
                transfer_alignment: 1,
                supports_file_sync: true,
                supports_directory_sync: false,
                supports_atomic_publish: false,
            }
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            let mut state = self.0.lock().unwrap();
            if state.reject || state.queued.is_some() {
                state.reject = false;
                return Err(RejectedIo {
                    request,
                    reason: Error::Busy,
                });
            }
            let id = IoId(state.next);
            state.next += 1;
            state.queued = Some((id, request));
            Ok(id)
        }
        fn poll(&self, _budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
            let mut state = self.0.lock().unwrap();
            let Some((id, request)) = state.queued.take() else {
                return Ok(());
            };
            let mut buffer = None;
            let kind = match &request.operation {
                IoOperation::Open { .. } => "open",
                IoOperation::Write { .. } => "write",
                IoOperation::SyncFile { .. } => "sync",
                IoOperation::Close(_) => "close",
                _ => panic!("材料写入不应提交其他操作"),
            };
            state.events.push(kind);
            let fail = state.fail == Some(kind);
            let result = match request.operation {
                IoOperation::Open { create_new, .. } => {
                    assert!(create_new);
                    if fail {
                        Err(Error::Codec(kind))
                    } else {
                        assert!(!state.open);
                        state.open = true;
                        Ok(IoOutcome::Opened(FileId {
                            slot: 0,
                            generation: Generation(0),
                        }))
                    }
                }
                IoOperation::Write {
                    offset,
                    buffer: bytes,
                    ..
                } => {
                    let n = state.short.unwrap_or(bytes.len()).min(bytes.len());
                    let result = if fail {
                        Err(Error::Codec(kind))
                    } else {
                        let start = offset as usize;
                        state.bytes.resize(start + n, 0);
                        state.bytes[start..start + n].copy_from_slice(&bytes.as_slice()[..n]);
                        Ok(IoOutcome::Transferred(n))
                    };
                    buffer = Some(bytes);
                    result
                }
                IoOperation::SyncFile { metadata, .. } => {
                    assert!(metadata);
                    if fail {
                        Err(Error::Codec(kind))
                    } else {
                        Ok(IoOutcome::Done)
                    }
                }
                IoOperation::Close(_) => {
                    if fail {
                        Err(Error::Codec(kind))
                    } else {
                        state.open = false;
                        Ok(IoOutcome::Done)
                    }
                }
                _ => unreachable!(),
            };
            out.push(IoCompletion {
                id,
                route: request.route,
                result,
                buffer,
            });
            Ok(())
        }
        fn shutdown(&self, _: Deadline) -> Result<(), Error> {
            Ok(())
        }
    }
    fn fixture() -> (Arc<ScriptDevice>, SegmentedStorage, MaterialWrite) {
        let device = Arc::new(ScriptDevice::default());
        let storage = SegmentedStorage::new(device.clone(), PathBuf::new(), 4096).unwrap();
        let task = MaterialWrite::new(
            &storage,
            CheckpointToken([1; 16]),
            "material-0",
            b"123456789".to_vec(),
            4,
            CompletionRoute(7),
        )
        .unwrap();
        (device, storage, task)
    }
    fn one(device: &dyn Device) -> IoCompletion {
        let mut out = vec![];
        device.poll(PollBudget::default(), &mut out).unwrap();
        assert_eq!(out.len(), 1);
        out.pop().unwrap()
    }
    fn run(storage: &SegmentedStorage, task: &mut MaterialWrite) -> Result<WrittenMaterial, Error> {
        for _ in 0..100 {
            if let Some(result) = task.take_result() {
                return result;
            }
            match task.submit_next(storage) {
                Ok(Some(_)) => task
                    .accept(storage, one(&*storage.device))
                    .map_err(|r| r.reason)
                    .unwrap(),
                Ok(None) | Err(Error::Busy) => {}
                Err(error) => panic!("任务推进失败：{error}"),
            }
        }
        panic!("材料任务没有终结")
    }
    #[test]
    fn 短写从实际位置继续且只在同步关闭后返回固定校验值() {
        let (device, storage, mut task) = fixture();
        device.0.lock().unwrap().short = Some(2);
        let receipt = run(&storage, &mut task).unwrap();
        assert_eq!(
            receipt,
            WrittenMaterial {
                bytes: 9,
                checksum: 0xe306_9283
            }
        );
        let state = device.0.lock().unwrap();
        assert_eq!(state.bytes, b"123456789");
        assert_eq!(
            state.events,
            vec![
                "open", "write", "write", "write", "write", "write", "sync", "close"
            ]
        );
        assert!(!state.open);
        assert!(!task.has_resources());
        assert!(task.take_result().is_none());
        assert!(task.submit_next(&storage).unwrap().is_none());
    }
    #[test]
    fn 各阶段失败不会自动重试或伪造成功且写同步失败会关闭() {
        for stage in ["open", "write", "sync", "close"] {
            let (device, storage, mut task) = fixture();
            device.0.lock().unwrap().fail = Some(stage);
            assert!(matches!(run(&storage,&mut task),Err(Error::Codec(actual)) if actual==stage));
            let state = device.0.lock().unwrap();
            assert_eq!(
                state.events.iter().filter(|event| **event == stage).count(),
                1
            );
            if stage == "write" || stage == "sync" {
                assert_eq!(state.events.last(), Some(&"close"));
                assert!(!state.open);
            }
            assert_eq!(task.has_resources(), stage == "close");
            assert!(task.take_result().is_none());
        }
    }
    #[test]
    fn 零写终结为错误且繁忙拒绝保持尚未接受的状态() {
        let (device, storage, mut task) = fixture();
        device.0.lock().unwrap().reject = true;
        assert!(matches!(task.submit_next(&storage), Err(Error::Busy)));
        assert!(!task.has_resources());
        assert!(device.0.lock().unwrap().events.is_empty());
        device.0.lock().unwrap().short = Some(0);
        assert!(
            matches!(run(&storage,&mut task),Err(Error::Io(error)) if error.kind()==std::io::ErrorKind::WriteZero)
        );
        assert_eq!(
            device.0.lock().unwrap().events,
            vec!["open", "write", "close"]
        );
        assert!(!task.has_resources());
    }
    #[test]
    fn 错路由与错存储完成归还原缓冲且重复完成不推进() {
        let (device, storage, mut task) = fixture();
        task.submit_next(&storage).unwrap();
        task.accept(&storage, one(&*device))
            .map_err(|r| r.reason)
            .unwrap();
        task.submit_next(&storage).unwrap();
        let mut completion = one(&*device);
        let id = completion.id;
        completion.route = CompletionRoute(8);
        let mut completion = task.accept(&storage, completion).unwrap_err().request;
        assert_eq!(completion.buffer.as_ref().unwrap().as_slice(), b"1234");
        completion.route = CompletionRoute(7);
        let other = SegmentedStorage::new(device.clone(), PathBuf::new(), 4096).unwrap();
        let completion = task.accept(&other, completion).unwrap_err().request;
        task.accept(&storage, completion)
            .map_err(|r| r.reason)
            .unwrap();
        assert!(
            task.accept(
                &storage,
                IoCompletion {
                    id,
                    route: CompletionRoute(7),
                    result: Ok(IoOutcome::Done),
                    buffer: None
                }
            )
            .is_err()
        );
        assert_eq!(run(&storage, &mut task).unwrap().bytes, 9);
        assert_eq!(device.0.lock().unwrap().bytes, b"123456789");
    }
    #[test]
    fn 空文件仍同步且不支持同步的设备在接受前拒绝() {
        let (device, storage, _) = fixture();
        let mut task = MaterialWrite::new(
            &storage,
            CheckpointToken([1; 16]),
            "empty",
            vec![],
            4,
            CompletionRoute(7),
        )
        .unwrap();
        assert_eq!(
            run(&storage, &mut task).unwrap(),
            WrittenMaterial {
                bytes: 0,
                checksum: 0
            }
        );
        assert_eq!(
            device.0.lock().unwrap().events,
            vec!["open", "sync", "close"]
        );
        let memory = SegmentedStorage::new(
            Arc::new(crate::device::memory::MemoryDevice::new(4, 64).unwrap()),
            PathBuf::new(),
            4096,
        )
        .unwrap();
        assert!(matches!(
            MaterialWrite::new(
                &memory,
                CheckpointToken([1; 16]),
                "file",
                vec![1],
                4,
                CompletionRoute(7)
            ),
            Err(Error::UnsupportedDurability)
        ));
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn 原生材料分块同步关闭后可重读且重复新建不覆盖() {
        struct Directory(PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        fn ready(device: &dyn Device) -> IoCompletion {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                assert!(std::time::Instant::now() < deadline, "原生材料 I/O 超时");
                let mut out = vec![];
                device.poll(PollBudget::default(), &mut out).unwrap();
                if !out.is_empty() {
                    assert_eq!(out.len(), 1);
                    return out.pop().unwrap();
                }
                std::thread::yield_now();
            }
        }
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-material-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let device = crate::device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }
        .open(DeviceOpenOptions {
            root: root.0.clone(),
            create_new: true,
        })
        .unwrap();
        let storage = SegmentedStorage::new(Arc::from(device), root.0.clone(), 4096).unwrap();
        let token = CheckpointToken([1; 16]);
        let path = storage.checkpoint_path(token, "material-0").unwrap();
        for directory in [
            PathBuf::from("checkpoints"),
            path.parent().unwrap().to_path_buf(),
        ] {
            storage
                .device
                .submit(IoRequest {
                    route: CompletionRoute(99),
                    operation: IoOperation::CreateDirectory(directory),
                })
                .unwrap();
            ready(&*storage.device).result.unwrap();
        }
        let data = (0..97).map(|n| ((n * 37) % 256) as u8).collect::<Vec<_>>();
        for duplicate in [false, true] {
            let mut task = MaterialWrite::new(
                &storage,
                token,
                "material-0",
                if duplicate {
                    b"replacement".to_vec()
                } else {
                    data.clone()
                },
                7,
                CompletionRoute(1),
            )
            .unwrap();
            let result = loop {
                if let Some(result) = task.take_result() {
                    break result;
                }
                match task.submit_next(&storage) {
                    Ok(Some(_)) => task
                        .accept(&storage, ready(&*storage.device))
                        .map_err(|r| r.reason)
                        .unwrap(),
                    Ok(None) | Err(Error::Busy) => std::thread::yield_now(),
                    Err(error) => panic!("材料推进错误：{error}"),
                }
            };
            if duplicate {
                assert!(
                    matches!(result,Err(Error::Io(error)) if error.kind()==std::io::ErrorKind::AlreadyExists)
                );
            } else {
                assert_eq!(result.unwrap().bytes, 97);
            }
            assert!(!task.has_resources());
            assert_eq!(std::fs::read(root.0.join(&path)).unwrap(), data);
        }
        storage
            .device
            .shutdown(Deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            ))
            .unwrap();
    }
}
