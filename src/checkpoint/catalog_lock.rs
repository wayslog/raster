//! 检查点目录的跨实例仲裁；每次动作独立打开锁文件，关闭完成后才解除持有。
use crate::{device::*, storage::SegmentedStorage, types::*};
use std::sync::Arc;
#[derive(Clone, Copy)]
enum Phase {
    Acquire,
    Held,
    Close,
    Closed,
    Failed,
}
pub(crate) struct CatalogLock {
    owner: Arc<()>,
    route: CompletionRoute,
    mode: FileLockMode,
    phase: Phase,
    pending: Option<IoId>,
    file: Option<FileId>,
}
impl CatalogLock {
    pub fn new(
        storage: &SegmentedStorage,
        route: CompletionRoute,
        mode: FileLockMode,
    ) -> Result<Self, Error> {
        if !storage.device.capabilities().supports_file_locks {
            return Err(Error::UnsupportedDurability);
        }
        Ok(Self {
            owner: storage.identity.clone(),
            route,
            mode,
            phase: Phase::Acquire,
            pending: None,
            file: None,
        })
    }
    pub fn held(&self) -> bool {
        matches!(self.phase, Phase::Held)
    }
    pub fn closed(&self) -> bool {
        matches!(self.phase, Phase::Closed)
    }
    pub fn release(&mut self) -> Result<(), Error> {
        match self.phase {
            Phase::Held => {
                self.phase = Phase::Close;
                Ok(())
            }
            Phase::Close | Phase::Closed => Ok(()),
            _ => Err(Error::InvalidState("尚未取得或已经失败的目录锁不能释放")),
        }
    }
    fn check_owner(&self, storage: &SegmentedStorage) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState("目录锁属于其他存储"));
        }
        Ok(())
    }
    pub fn submit_next(&mut self, storage: &SegmentedStorage) -> Result<Option<IoId>, Error> {
        self.check_owner(storage)?;
        if self.pending.is_some() {
            return Ok(None);
        }
        let operation = match self.phase {
            Phase::Acquire => IoOperation::TryLock {
                path: "checkpoint.lock".into(),
                mode: self.mode,
            },
            Phase::Close => IoOperation::Close(self.file.expect("目录锁持有句柄")),
            Phase::Held | Phase::Closed => return Ok(None),
            Phase::Failed => return Err(Error::InvalidState("目录锁已经失败")),
        };
        match storage.device.submit(IoRequest {
            route: self.route,
            operation,
        }) {
            Ok(id) => {
                self.pending = Some(id);
                Ok(Some(id))
            }
            Err(rejected) => {
                if !matches!(rejected.reason, Error::Busy) {
                    self.phase = Phase::Failed;
                }
                Err(rejected.reason)
            }
        }
    }
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Error> {
        self.check_owner(storage)?;
        if self.pending != Some(completion.id)
            || self.route != completion.route
            || completion.buffer.is_some()
        {
            return Err(Error::InvalidState("目录锁完成身份或缓冲错误"));
        }
        self.pending = None;
        match (self.phase, completion.result) {
            (Phase::Acquire, Ok(IoOutcome::Locked(file))) => {
                self.file = Some(file);
                self.phase = Phase::Held;
            }
            (Phase::Acquire, Err(Error::Busy)) => {}
            (Phase::Close, Ok(IoOutcome::Done) | Err(Error::RangeTruncated)) => {
                self.file = None;
                self.phase = Phase::Closed;
            }
            (_, Err(error)) => {
                self.phase = Phase::Failed;
                return Err(error);
            }
            _ => {
                self.phase = Phase::Failed;
                return Err(Error::InvalidState("目录锁完成类型错误"));
            }
        }
        Ok(())
    }
}
// Drop 不提交 I/O。失败任务必须由拥有者保留至设备 shutdown；设备文件表持有未确认句柄。
