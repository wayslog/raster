//! 压缩共用的条件复制：查证最新源，在同步源许可内复制，等待 I/O 时只保留拥有型状态。
use super::{Engine, io_hub::CompletionHub, version_permit::VersionPermit};
use crate::{
    coordination::{Action, Phase},
    device::IoCompletion,
    format::Record,
    index::{EntrySnapshot, IndexHead, PublishResult},
    log::lookup::{LogLookup, LookupStep},
    schema::{Schema, ValueLayout, key::decode_canonical},
    storage::SegmentedStorage,
    types::*,
};
use std::sync::{Arc, TryLockError, atomic::Ordering};

pub(crate) struct ConditionalCopy {
    hub: Arc<CompletionHub>,
    monitor: super::metrics::Monitor,
    id: Option<RequestId>,
    action: MaintenanceId,
    version: CheckpointVersion,
    source: LogAddress,
    key: Vec<u8>,
    hash: KeyHash,
    permit: Option<VersionPermit>,
    lookup: Option<(EntrySnapshot, LogLookup)>,
    ended: bool,
    published: Option<LogAddress>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CopyResult {
    Copied(LogAddress),
    Obsolete,
    Retry,
}
impl ConditionalCopy {
    pub fn has_inflight(&self) -> bool {
        self.lookup
            .as_ref()
            .is_some_and(|(_, lookup)| lookup.has_inflight())
    }
    pub fn published_address(&self) -> Option<LogAddress> {
        self.published
    }
    #[allow(clippy::result_large_err, reason = "错误路由原样归还拥有缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        let Some((_, lookup)) = self.lookup.as_mut() else {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("条件复制未等待磁盘查询"),
            });
        };
        lookup.accept(storage, completion)
    }
    fn release(&mut self) -> Result<(), Error> {
        if self.has_inflight() {
            return Err(Error::Busy);
        }
        self.lookup = None;
        self.permit = None;
        self.ended = true;
        if let Some(id) = self.id.take() {
            self.hub.release(id)?;
        }
        Ok(())
    }
    /// 失败收尾不提交新读取；调度器须先归还已有完成，再结束所属全局动作。
    pub fn drain(&mut self, storage: &SegmentedStorage) -> Result<bool, Error> {
        if let Some(id) = self.id
            && let Some(completion) = self.hub.take(id)?
        {
            self.accept(storage, completion)
                .map_err(|rejected| rejected.reason)?;
        }
        if self.has_inflight() {
            return Ok(false);
        }
        let io = self.id.and_then(|id| self.hub.completion_count(id).ok());
        self.release()?;
        self.monitor.finish(super::metrics::Completed::Aborted, io);
        Ok(true)
    }
}
impl Drop for ConditionalCopy {
    fn drop(&mut self) {
        // 正常调度先 drain；异常销毁仅注销历史邮箱，设备仍拥有已接受的缓冲。
        if let Some(id) = self.id.take() {
            let io = if self.has_inflight() {
                None
            } else {
                self.hub.completion_count(id).ok()
            };
            self.monitor
                .finish(super::metrics::Completed::Failed(Effect::Unknown), io);
            let _ = self.hub.release(id);
        }
    }
}
impl<S: Schema> Engine<S> {
    pub(crate) fn new_conditional_copy(
        &self,
        action: MaintenanceId,
        source: LogAddress,
        key: Vec<u8>,
    ) -> Result<ConditionalCopy, Error> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let state = self.coordinator.snapshot()?;
            if self.failed.load(Ordering::SeqCst)
                || self.shutdown_requested.load(Ordering::SeqCst)
                || state.id != Some(action)
                || state.action != Some(Action::Compact)
                || state.phase != Phase::Compacting
            {
                return Err(Error::InvalidState("条件复制需要有效的压缩动作"));
            }
            source.validate()?;
            let frontiers = self.log.frontiers()?;
            if source < frontiers.begin {
                return Err(Error::RangeTruncated);
            }
            if source >= frontiers.tail {
                return Err(Error::InvalidFormat("压缩源超出日志范围"));
            }
            let (_, hash) =
                decode_canonical(self.schema.key_codec(), &key).inspect_err(|error| {
                    // 键适配器在自己的边界把恐慌转换为 InvalidState；仍须向引擎传播失败关闭。
                    if matches!(error, Error::InvalidState(_)) {
                        self.failed.store(true, Ordering::SeqCst);
                    }
                })?;
            let permit = self.version_permits.reserve(hash, state.version)?;
            let id = self.io.reserve(SessionId(self.id.0))?;
            Ok(ConditionalCopy {
                hub: self.io.clone(),
                monitor: self.metrics.accept(super::metrics::Kind::Copy),
                id: Some(id),
                action,
                version: state.version,
                source,
                key,
                hash,
                permit: Some(permit),
                lookup: None,
                ended: false,
                published: None,
            })
        }));
        match result {
            Ok(result) => result,
            Err(_) => {
                self.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("条件复制键准备恐慌"))
            }
        }
    }
    pub(crate) fn conditional_copy(
        &self,
        request: &mut ConditionalCopy,
        budget: PollBudget,
    ) -> Result<CopyResult, Error> {
        if !Arc::ptr_eq(&self.io, &request.hub) {
            return Err(Error::InvalidState("条件复制属于其他引擎实例"));
        }
        if request.ended {
            return Err(Error::InvalidState("条件复制已经终结"));
        }
        let state = self.coordinator.snapshot()?;
        if self.failed.load(Ordering::SeqCst)
            || state.id != Some(request.action)
            || state.action != Some(Action::Compact)
            || state.phase != Phase::Compacting
            || state.version != request.version
        {
            return Err(Error::InvalidState("条件复制动作或版本已失效"));
        }
        if !request
            .permit
            .as_ref()
            .ok_or(Error::InvalidState("条件复制缺少版本许可"))?
            .ready()?
        {
            request.monitor.pending();
            return Ok(CopyResult::Retry);
        }
        let _gate =
            match self.operations[request.hash.0 as usize % self.operations.len()].try_lock() {
                Ok(gate) => gate,
                Err(TryLockError::WouldBlock) => {
                    request.monitor.pending();
                    return Ok(CopyResult::Retry);
                }
                Err(_) => return Err(Error::InvalidState("条件复制遇到业务仲裁锁中毒")),
            };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.copy_step(request, budget)
        }));
        let mut result = match result {
            Ok(result) => result,
            Err(_) => {
                self.failed.store(true, Ordering::SeqCst);
                Err(Error::InvalidState("条件复制布局或发布恐慌"))
            }
        };
        if matches!(result, Ok(CopyResult::Retry)) {
            request.monitor.pending();
        } else {
            request.ended = true;
            let io = if request.has_inflight() {
                None
            } else {
                request
                    .id
                    .and_then(|id| request.hub.completion_count(id).ok())
            };
            if !request.has_inflight()
                && let Err(error) = request.release()
            {
                result = Err(error);
            }
            let summary = match &result {
                Ok(CopyResult::Copied(_)) => super::metrics::Completed::Success,
                Ok(CopyResult::Obsolete) => super::metrics::Completed::NotFound,
                Err(_) => super::metrics::Completed::Failed(if request.published.is_some() {
                    Effect::Applied
                } else {
                    Effect::NotApplied
                }),
                Ok(CopyResult::Retry) => unreachable!("重试已单独处理"),
            };
            request.monitor.finish(summary, io);
        }
        result
    }
    fn copy_step(
        &self,
        request: &mut ConditionalCopy,
        budget: PollBudget,
    ) -> Result<CopyResult, Error> {
        let id = request
            .id
            .ok_or(Error::InvalidState("条件复制缺少完成路由"))?;
        if let Some(completion) = self.io.take(id)? {
            request
                .accept(&self.storage, completion)
                .map_err(|rejected| rejected.reason)?;
        }
        if request.lookup.is_none() {
            let resolved = self.resolve_index(request.hash, &request.key)?;
            let mut key = Vec::new();
            key.try_reserve_exact(request.key.len())
                .map_err(|_| Error::OutOfMemory)?;
            key.extend_from_slice(&request.key);
            let lookup = self.log.lookup_metadata(
                &self.storage,
                key,
                resolved.head,
                CompletionHub::route(id),
            )?;
            request.lookup = Some((resolved.entry, lookup));
        }
        let (expected, lookup) = request.lookup.as_mut().expect("查询已创建");
        let step = lookup.step(&self.log, &self.storage, budget)?;
        if matches!(step, LookupStep::AwaitingIo | LookupStep::Continue) {
            return Ok(CopyResult::Retry);
        }
        if !matches!(
            step,
            LookupStep::Present | LookupStep::Tombstone | LookupStep::Missing
        ) {
            return Err(Error::InvalidState("条件复制元数据查询返回了活跃值"));
        }
        let current = self.resolve_index(request.hash, &request.key)?;
        if current.entry != *expected {
            request.lookup = None;
            return Ok(CopyResult::Retry);
        }
        if lookup.matched_address() != Some(request.source) {
            return Ok(CopyResult::Obsolete);
        }
        let (_, lookup) = request.lookup.take().expect("终结查询存在");
        let mut publishing = false;
        let result = lookup.with_matched_record(&self.log, &self.storage, |bytes| {
            publishing = true;
            self.publish_copy(request, current.entry, current.head, bytes)
        });
        match result {
            // 仅源许可争用可重试；专家布局返回 Busy 仍是本次复制的终结错误。
            Err(Error::Busy) if !publishing => Ok(CopyResult::Retry),
            Err(Error::RangeTruncated)
                if !publishing && request.source >= self.log.frontiers()?.begin =>
            {
                Ok(CopyResult::Retry)
            }
            result => result,
        }
    }
    fn publish_copy(
        &self,
        request: &mut ConditionalCopy,
        entry: EntrySnapshot,
        head: Option<LogAddress>,
        bytes: &[u8],
    ) -> Result<CopyResult, Error> {
        let record = Record::decode(bytes)?;
        if record.key != request.key
            || record.header.version > request.version
            || record.header.invalid
            || record
                .header
                .previous
                .is_some_and(|previous| previous >= request.source)
        {
            return Err(Error::InvalidFormat("条件复制源记录无效"));
        }
        let reservation = if record.header.tombstone {
            match self.log.reserve_tombstone(&request.key, head) {
                Ok(reservation) => reservation,
                Err(Error::CapacityExceeded)
                    if self.storage.device.capabilities().supports_files =>
                {
                    return Ok(CopyResult::Retry);
                }
                Err(error) => return Err(error),
            }
        } else {
            let value = self.schema.value_layout().decode_owned(record.value)?;
            let plan = self.schema.value_layout().plan(&value)?.validate()?;
            self.log.record_fits(request.key.len(), plan)?;
            let allocation = match self.log.allocate_record(&request.key, head, plan) {
                Ok(allocation) => allocation,
                Err(Error::CapacityExceeded)
                    if self.storage.device.capabilities().supports_files =>
                {
                    return Ok(CopyResult::Retry);
                }
                Err(error) => return Err(error),
            };
            allocation.initialize(value)?
        };
        self.log.with_initialization(
            reservation.with_version(request.version),
            |address| match self.index.compare_publish(entry, IndexHead::Log(address)) {
                Ok(PublishResult::Published) => {
                    request.published = Some(address);
                    Ok(CopyResult::Copied(address))
                }
                Ok(PublishResult::Conflict(_)) => {
                    self.log.retire(address)?;
                    request.monitor.invalidate();
                    Ok(CopyResult::Retry)
                }
                Err(error) => {
                    self.log.retire(address)?;
                    request.monitor.invalidate();
                    Err(error)
                }
            },
        )
    }
}
