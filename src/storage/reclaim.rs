//! 工作段回收意图：先摘除受保护映射，再关闭、删除并同步目录；失败保留当前步骤。
use super::*;
use crate::{device::*, engine::io_hub::CompletionHub};
#[derive(Clone, Debug)]
pub(crate) struct SegmentCandidate {
    pub number: u64,
    pub generation: Generation,
}
impl SegmentedStorage {
    /// boundary 为第一个必须保留的物理字节；只枚举当前实例仍绑定的完整旧段。
    pub fn reclaim_candidates(&self, boundary: u64) -> Result<Vec<SegmentCandidate>, Error> {
        let segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("段映射锁中毒"))?;
        let limit = boundary / self.segment_bytes;
        let mut result = Vec::new();
        for (&number, binding) in segments.range(..limit) {
            if binding.file.is_some() {
                result.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                result.push(SegmentCandidate {
                    number,
                    generation: binding.generation,
                });
            }
        }
        Ok(result)
    }
}
#[derive(Clone, Copy)]
enum Stage {
    Close,
    Remove,
    Sync,
    Done,
}
pub(crate) struct SegmentDelete {
    owner: Arc<()>,
    hub: Arc<CompletionHub>,
    id: RequestId,
    file: FileId,
    path: PathBuf,
    directory: PathBuf,
    stage: Stage,
    inflight: Option<IoId>,
}
impl SegmentDelete {
    /// 所有可能失败的路由及路径准备均先于映射失效；失效后任务持有旧句柄与旧代次路径。
    pub fn detach(
        storage: &SegmentedStorage,
        hub: Arc<CompletionHub>,
        store: StoreId,
        candidate: SegmentCandidate,
    ) -> Result<Self, Error> {
        let path = storage.segment_path(candidate.number, candidate.generation);
        let directory = storage.segment_directory();
        let id = hub.reserve(SessionId(store.0))?;
        let file = match storage.invalidate(candidate.number, candidate.generation) {
            Ok((file, _)) => file,
            Err(error) => {
                hub.release(id)?;
                return Err(error);
            }
        };
        Ok(Self {
            owner: storage.identity.clone(),
            hub,
            id,
            file,
            path,
            directory,
            stage: Stage::Close,
            inflight: None,
        })
    }
    pub fn has_inflight(&self) -> bool {
        self.inflight.is_some()
    }
    /// false 表示仍待提交或完成，true 只在删除目录同步成功之后返回。
    pub fn step(&mut self, storage: &SegmentedStorage) -> Result<bool, Error> {
        if !Arc::ptr_eq(&self.owner, &storage.identity) {
            return Err(Error::InvalidState("删除任务属于其他存储"));
        }
        if let Some(completion) = self.hub.take(self.id)? {
            if Some(completion.id) != self.inflight
                || completion.route != CompletionHub::route(self.id)
                || completion.buffer.is_some()
            {
                return Err(Error::InvalidState("删除完成身份或缓冲类型错误"));
            }
            self.inflight = None;
            let done = match completion.result {
                Ok(IoOutcome::Done) => true,
                // 独占摘除之后旧句柄已无其他使用者；已关闭或已删除可安全接续下一步。
                Err(Error::RangeTruncated) if matches!(self.stage, Stage::Close) => true,
                Err(Error::Io(ref error))
                    if matches!(self.stage, Stage::Remove)
                        && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    true
                }
                Err(error) => return Err(error),
                _ => return Err(Error::InvalidState("删除完成结果类型错误")),
            };
            if done {
                self.stage = match self.stage {
                    Stage::Close => Stage::Remove,
                    Stage::Remove => Stage::Sync,
                    Stage::Sync => Stage::Done,
                    Stage::Done => return Err(Error::InvalidState("删除重复完成")),
                };
            }
            return Ok(matches!(self.stage, Stage::Done));
        }
        if self.inflight.is_some() {
            return Ok(false);
        }
        let operation = match self.stage {
            Stage::Close => IoOperation::Close(self.file),
            Stage::Remove => IoOperation::RemoveFile(self.path.clone()),
            Stage::Sync => IoOperation::SyncDirectory(self.directory.clone()),
            Stage::Done => return Ok(true),
        };
        match storage.device.submit(IoRequest {
            route: CompletionHub::route(self.id),
            operation,
        }) {
            Ok(id) => self.inflight = Some(id),
            Err(rejected) if matches!(rejected.reason, Error::Busy) => {}
            Err(rejected) => return Err(rejected.reason),
        }
        Ok(false)
    }
}
impl Drop for SegmentDelete {
    fn drop(&mut self) {
        let _ = self.hub.release(self.id);
    }
}
