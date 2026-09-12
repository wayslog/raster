//! Writing of a single non-overwriteable material file is synchronized with the file;Directory synchronization and submission publishing are the responsibility of the upper layer.
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
/// Only successful writes,File credentials obtained from material tasks only after synchronization and closing.
pub(crate) struct SyncedFile {
    pub(super) owner: Arc<crate::sync::InstanceId>,
    pub(super) token: CheckpointToken,
    pub(super) name: String,
    pub(super) digest: WrittenMaterial,
}
pub(crate) struct MaterialWrite {
    owner: Arc<crate::sync::InstanceId>,
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
    /// The parent directory must first be created by the checkpoint task;Only new files will be created here,Cannot cover existing materials.
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
                reason: "Block size must be non-zero and device memory alignment must be valid",
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
            Err(Error::InvalidState(
                "Material writing belongs to other storage",
            ))
        }
    }
    fn fail(&mut self, error: Error) {
        self.failure.get_or_insert(error);
        if self.file.is_some() && self.stage != Stage::Close {
            self.stage = Stage::Close;
        } else {
            self.stage = Stage::Done;
            self.result = Some(Err(self.failure.take().expect("Save first error")));
        }
    }
    /// Busy You can try again later;Other rejections transfer to close cleanup,Continue to be driven by the caller until the result is completed.
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
                    file: self.file.expect("Material has been opened"),
                    offset: self.cursor as u64,
                    buffer,
                }
            }
            Stage::Sync => IoOperation::SyncFile {
                file: self.file.expect("Material has been opened"),
                metadata: true,
            },
            Stage::Close => IoOperation::Close(self.file.expect("Keep handle when closing")),
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
    #[allow(
        clippy::result_large_err,
        reason = "Misidentification must be returned to original completion with buffering"
    )]
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
                reason: Error::InvalidState("Material completion identity mismatch"),
            });
        }
        let (_, length) = self.pending.take().expect("In-transit identity matched");
        match (self.stage, completion.result) {
            (Stage::Open, Ok(IoOutcome::Opened(file))) => {
                self.file = Some(file);
                if completion.buffer.is_some() {
                    self.fail(Error::InvalidState(
                        "Opening material returned an unexpected buffer",
                    ));
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
                    self.fail(Error::InvalidState(
                        "Material short length or buffer mismatch",
                    ));
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
            _ => self.fail(Error::InvalidState("Wrong material completion type")),
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
    /// Drop Do not initiate blocking I/O;The owner must keep the task until it is completed,or by device shutdown Take over resources.
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
    // Only simulation of completion order and errors,No evidence of persistence is provided;There is also real file back-end acceptance.
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
                supports_directory_listing: false,
                supports_file_locks: false,
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
                _ => panic!("Material writing should not be submitted for other operations"),
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
        let storage = SegmentedStorage::new(device.clone(), 4096).unwrap();
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
                Err(error) => panic!("Task advancement failed:{error}"),
            }
        }
        panic!("The material task is not over")
    }
    #[test]
    fn the_short_write_continues_from_the_actual_position_and_returns_a_fixed_checksum_only_after_synchronization_is_turned_off()
     {
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
    fn failures_in_each_stage_will_not_be_automatically_retried_or_faked_successfully_and_write_synchronization_failure_will_be_closed()
     {
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
    fn zero_writes_terminate_as_errors_and_busy_rejections_remain_unaccepted() {
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
    fn wrong_routing_and_wrong_storage_completion_are_restored_to_the_original_buffer_and_repeated_completion_is_not_advanced()
     {
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
        let other = SegmentedStorage::new(device.clone(), 4096).unwrap();
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
    fn empty_files_are_still_synced_and_devices_that_dont_support_sync_are_rejected_before_accepting()
     {
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
    fn original_materials_can_be_re_read_after_block_synchronization_is_turned_off_and_repeated_creation_will_not_overwrite_them()
     {
        struct Directory(PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        fn ready(device: &dyn Device) -> IoCompletion {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "virgin materials I/O timeout"
                );
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
        let storage = SegmentedStorage::new(Arc::from(device), 4096).unwrap();
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
                    Err(error) => panic!("Material advance error:{error}"),
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
