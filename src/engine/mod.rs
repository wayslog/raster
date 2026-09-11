//! 四操作、挂起任务、持久化和维护的编排中心。
pub(crate) mod auto_compaction;
mod cache;
pub(crate) mod checkpoint;
pub(crate) mod checkpoint_release;
pub(crate) mod compaction;
mod conditional_copy;
mod delete;
pub(crate) mod gc;
pub(crate) mod growth;
pub(crate) mod io_hub;
mod maintenance;
pub(crate) mod metrics;
mod observe;
mod pending;
mod progress;
mod read;
mod rmw;
pub(crate) mod scan;
#[cfg(test)]
pub(crate) mod session_actor;
mod storage_progress;
pub(crate) mod thread_sessions;
mod upsert;
mod version_permit;

use crate::{
    cache::ReadCache, config::Config, coordination::Coordinator, epoch::EpochManager,
    index::MemIndex, log::HybridLog, schema::Schema, storage::SegmentedStorage, types::*,
};
pub(crate) use pending::SessionRuntime;

pub(crate) struct Engine<S: Schema> {
    pub id: StoreId,
    pub metrics: std::sync::Arc<metrics::Metrics>,
    pub thread_sessions: std::sync::Arc<thread_sessions::ThreadSessions>,
    pub io: std::sync::Arc<io_hub::CompletionHub>,
    pub scans: scan::ScanRegistry,
    pub auto_compaction: auto_compaction::AutoCompactionRuntime,
    pub compaction: std::sync::Mutex<compaction::CompactionRuntime>,
    pub gc: std::sync::Mutex<gc::GcRuntime>,
    pub checkpoint_release: std::sync::Mutex<checkpoint_release::ReleaseRuntime>,
    pub growth: std::sync::Mutex<growth::GrowthRuntime>,
    pub checkpoints: std::sync::Mutex<checkpoint::CheckpointRuntime>,
    pub storage_progress: std::sync::Mutex<storage_progress::StorageProgress>,
    pub version_permits: version_permit::VersionPermits,
    pub operations: Vec<crate::sync::Mutex<()>>,
    pub failed: std::sync::atomic::AtomicBool,
    pub shutdown_requested: std::sync::atomic::AtomicBool,
    pub schema: std::sync::Arc<S>,
    pub config: Config,
    pub index: MemIndex,
    pub log: HybridLog<crate::schema::SharedValue<S>>,
    pub shutdown_state: crate::sync::Mutex<bool>,
    pub cache: ReadCache,
    pub epoch: EpochManager,
    pub coordinator: Coordinator,
    pub storage: SegmentedStorage,
}
impl<S: Schema> Engine<S> {
    fn prepare<O: crate::api::operation::Keyed<S>>(
        &self,
        session: &SessionRuntime,
        serial: Serial,
        request: &O,
    ) -> Result<(KeyHash, Vec<u8>), Error> {
        use crate::schema::KeyCodec;
        if session.closing || self.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("会话关闭或引擎失败"));
        }
        // 活跃身份只有当前会话能够提交；创建/续接读取登记进度，admit 成功后同步本地值。
        // 此处仅提前拒绝非法序号；最终接受仍检查全局身份、版本及序号。
        if session
            .current
            .last_accepted
            .is_some_and(|last| serial <= last)
        {
            return Err(Error::InvalidState("操作序号必须严格递增"));
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let codec = self.schema.key_codec();
            let key = request.key();
            let hash = codec.hash(key);
            let len = codec.encoded_len(key)? as usize;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(len)
                .map_err(|_| Error::OutOfMemory)?;
            bytes.resize(len, 0);
            codec.encode(key, &mut bytes)?;
            Ok((hash, bytes))
        })) {
            Ok(result) => result,
            Err(_) => {
                self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(Error::InvalidState("请求键或编码器恐慌"))
            }
        }
    }
    fn finish_request<R, T>(
        &self,
        request: R,
        result: crate::api::completion::OperationResult<T>,
        effect: Effect,
    ) -> crate::api::completion::OperationResult<T> {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(request))).is_err() {
            self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
            // 旧输出也可能有用户析构，仍在边界内释放。
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result)));
            return Err(OperationError {
                cause: Error::InvalidState("请求析构恐慌"),
                effect,
            });
        }
        result
    }
    fn admit(&self, session: &mut SessionRuntime, serial: Serial) -> Result<(), Error> {
        if session.closing || self.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("会话关闭或引擎失败"));
        }
        self.coordinator
            .accept_serial(session.id, serial, session.current.version)?;
        session.current.last_accepted = Some(serial);
        Ok(())
    }
    fn head(entry: crate::index::EntrySnapshot) -> Result<Option<LogAddress>, Error> {
        match entry.head {
            crate::index::IndexHead::Empty => Ok(None),
            crate::index::IndexHead::Log(address) => Ok(Some(address)),
            crate::index::IndexHead::Cache(_) => Err(Error::InvalidState("缓存路径尚未接通")),
        }
    }
}

#[cfg(test)]
mod value_contention_tests;

#[cfg(test)]
mod frozen_tests;
#[cfg(test)]
mod pending_read_tests;

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod checkpoint_tests;

impl<S: Schema> Engine<S> {
    /// Ready 直接移交拥有型结果；仍以相同终结和 I/O 采样口径记录统计。
    fn record_ready<T: 'static>(
        &self,
        monitor: &mut metrics::Monitor,
        id: RequestId,
        result: &crate::api::completion::OperationResult<T>,
    ) {
        let io = if monitor.sampled() {
            self.io.completion_count(id).ok()
        } else {
            Some(0)
        };
        monitor.finish(metrics::Completed::result(result), io);
    }
    fn complete_tracked<T: 'static>(
        &self,
        monitor: &mut metrics::Monitor,
        id: RequestId,
        complete: &crate::api::completion::Completer<T>,
        result: crate::api::completion::OperationResult<T>,
    ) {
        let summary = metrics::Completed::result(&result);
        let io = if monitor.sampled() {
            self.io.completion_count(id).ok()
        } else {
            Some(0)
        };
        let delivered =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| complete.finish(result)));
        if matches!(delivered, Ok(Ok(()))) {
            monitor.finish(summary, io);
        } else {
            self.failed.store(true, std::sync::atomic::Ordering::SeqCst);
            monitor.finish(metrics::Completed::Failed(Effect::Unknown), io);
        }
    }
}
