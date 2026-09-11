//! 创建和恢复完成全部组件后才发布实例；持久会话须显式继续。
use super::{
    maintenance::{Maintenance, RecoveryReport, RecoverySet},
    scan::{RecordScanner, ScanOptions},
    session::{Session, SessionOptions},
};
use crate::{
    config::Config, device::DeviceFactory, diagnostics::Diagnostics, engine::Engine,
    schema::Schema, types::*,
};
use std::sync::Arc;

/// 共享的键空间、索引、混合日志和维护历史；Clone 共享同一实例。
/// 每个线程通过自己的 Session 执行业务。恢复与创建入口见 Builder。
pub struct RasterKV<S: Schema> {
    pub(crate) inner: Arc<Engine<S>>,
}
impl<S: Schema> Clone for RasterKV<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
/// 收集配置与真实设备工厂；只有全部资源准备或恢复成功才返回 RasterKV。
pub struct Builder<S: Schema> {
    schema: S,
    config: Config,
    device: Option<Box<dyn DeviceFactory>>,
}
/// 可续跑的本线程会话及检查点持久化进度；新序号必须大于会话当前 last_accepted。
/// 同实例关闭后再次续接时，当前接受进度可能已经超过检查点的 progress.serial。
pub struct ResumedSession<S: Schema> {
    pub session: Session<S>,
    pub progress: super::maintenance::DurableProgress,
}
#[derive(Debug)]
pub struct ShutdownReport {
    pub device_drained: bool,
}

impl<S: Schema> RasterKV<S> {
    /// 持久存储身份，用于组装恢复集；恢复后保持不变。
    pub fn id(&self) -> StoreId {
        self.inner.id
    }

    pub fn builder(schema: S) -> Builder<S> {
        Builder {
            schema,
            config: Config::default(),
            device: None,
        }
    }
    /// 在调用线程注册会话；同一线程与存储只能有一个活跃会话。
    /// 显式身份重复、容量不足或实例失败时拒绝，恢复身份使用 continue_session。
    pub fn start_session(&self, options: SessionOptions) -> Result<Session<S>, Error> {
        if self.inner.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("引擎已失败关闭"));
        }
        let thread_session = self.inner.thread_sessions.enter()?;
        let id = match options.id {
            Some(id) => id,
            None => SessionId::generate()?,
        };
        let version = self.inner.coordinator.enroll(id)?;
        let participant = match self.inner.epoch.register() {
            Ok(id) => id,
            Err(error) => {
                let _ = self.inner.coordinator.leave(id);
                return Err(error);
            }
        };
        let last_accepted = self.inner.coordinator.last_accepted(id)?;
        Ok(Session {
            thread_session: Some(thread_session),
            engine: self.inner.clone(),
            id,
            participant: Some(participant),
            runtime: crate::engine::SessionRuntime::new(id, last_accepted, version),
            local: std::marker::PhantomData,
        })
    }
    /// 续接恢复报告中的身份；未知、仍活跃或同线程已有会话时拒绝。
    /// 关闭后可再次续接，last_accepted 保留本实例已经接受的最新序号。
    pub fn continue_session(&self, id: SessionId) -> Result<ResumedSession<S>, Error> {
        if self.inner.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("引擎已失败关闭"));
        }
        let thread_session = self.inner.thread_sessions.enter()?;
        let (version, serial, durable_version) = self.inner.coordinator.resume(id)?;
        let participant = match self.inner.epoch.register() {
            Ok(participant) => participant,
            Err(error) => {
                let _ = self.inner.coordinator.leave(id);
                return Err(error);
            }
        };
        let last = self.inner.coordinator.last_accepted(id)?;
        Ok(ResumedSession {
            session: Session {
                thread_session: Some(thread_session),
                engine: self.inner.clone(),
                id,
                participant: Some(participant),
                runtime: crate::engine::SessionRuntime::new(id, last, version),
                local: std::marker::PhantomData,
            },
            progress: super::maintenance::DurableProgress {
                session: id,
                serial,
                version: durable_version,
            },
        })
    }
    pub fn maintenance(&self) -> Maintenance<S> {
        Maintenance {
            inner: Arc::clone(&self.inner),
        }
    }
    /// 读取真实组件状态；并发字段不构成事务快照，跨度与桶占用不等于有效键数。
    pub fn diagnostics(&self) -> Result<Diagnostics, Error> {
        let frontiers = self.inner.log.frontiers()?;
        let (table, growing_index, migrated_buckets, retired_index_retained) =
            self.inner.index.diagnostics()?;
        let (active_requests, pending_requests) = self.inner.metrics.activity()?;
        let (log_resident_pages, log_allocated_bytes) = self.inner.log.memory_usage()?;
        let auto_compaction = self.inner.auto_compaction_status()?;
        let active_session_ids = self.inner.coordinator.active_session_ids()?;
        Ok(Diagnostics {
            log_span_bytes: frontiers
                .tail
                .0
                .checked_sub(frontiers.begin.0)
                .ok_or(Error::InvalidState("日志跨度倒置"))?,
            begin: frontiers.begin,
            tail: frontiers.tail,
            active_sessions: active_session_ids.len(),
            active_session_ids,
            active_requests,
            pending_requests,
            cached_bytes: self.inner.cache.allocated_bytes(),
            cache_reserved_bytes: self.inner.cache.reserved_bytes(),
            log_resident_pages,
            log_allocated_bytes,
            auto_compaction_scheduled: matches!(
                auto_compaction.phase,
                super::maintenance::AutoCompactionPhase::Scheduled
                    | super::maintenance::AutoCompactionPhase::Compacting
                    | super::maintenance::AutoCompactionPhase::Reclaiming
            ) || auto_compaction.active.is_some(),
            auto_compaction,
            table_generation: table.generation,
            bucket_distribution: table.bucket_distribution,
            growing_index,
            migrated_buckets,
            retired_index_retained,
            failed: self.inner.failed.load(std::sync::atomic::Ordering::SeqCst),
            shutdown_requested: self
                .inner
                .shutdown_requested
                .load(std::sync::atomic::Ordering::SeqCst),
        })
    }
    /// 启用后续请求和内部事件采样，不清空已有计数。
    pub fn enable_stats_collection(&self) {
        self.inner.metrics.enable(true);
    }
    /// 停止新请求采样；已有采样请求记录到终结，实时资源诊断保持启用。
    pub fn disable_stats_collection(&self) {
        self.inner.metrics.enable(false);
    }
    /// 返回拥有型历史计数，创建或恢复新实例时归零，不进入检查点。
    pub fn statistics(&self) -> crate::diagnostics::Statistics {
        self.inner.metrics.snapshot()
    }
    /// 调用者选择输出位置；本库不安装全局日志订阅器。
    pub fn write_statistics(&self, output: &mut impl std::io::Write) -> Result<(), Error> {
        write!(output, "{}", self.statistics()).map_err(Error::Io)
    }
    pub fn scan(&self, options: ScanOptions) -> Result<RecordScanner<S>, Error> {
        RecordScanner::open(self.inner.clone(), options)
    }
    /// 先停止并排空自动维护；活跃会话、扫描或手动任务仍返回 Busy，可推进后重试。
    /// 停止并排空自动维护及设备；成功不自动创建检查点。
    /// 活跃会话或手动维护导致 Busy；diagnostics 提供活跃会话身份列表。
    /// 应先关闭会话并推进原维护票据；超时可续调用。
    pub fn shutdown(&self, deadline: Deadline) -> Result<ShutdownReport, Error> {
        let mut done = self
            .inner
            .shutdown_state
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => Error::Busy,
                std::sync::TryLockError::Poisoned(_) => Error::InvalidState("关闭锁中毒"),
            })?;
        if !*done {
            // 先禁止自动接受并排空已接受任务；即使稍后因活跃会话返回 Busy，停止请求仍有效。
            self.inner.auto_compaction.request_stop()?;
            self.maintenance().wait_auto_compaction(deadline)?;
            // 注册扫描与关闭共享关闭锁；活跃扫描立即拒绝，已放弃扫描按截止时间排空。
            let scan_failure = match self.inner.drain_scans(deadline) {
                Ok(()) => None,
                Err(error @ (Error::Busy | Error::DeadlineExceeded)) => return Err(error),
                Err(error) => {
                    self.inner
                        .failed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    Some(error)
                }
            };
            // 复合任务在两个全局动作之间也仍未终结，不能让关闭越过该间隙。
            let compaction = self
                .inner
                .compaction
                .try_lock()
                .map_err(|error| match error {
                    std::sync::TryLockError::WouldBlock => Error::Busy,
                    _ => Error::InvalidState("关闭遇到压缩任务锁中毒"),
                })?;
            if compaction.is_active()
                && !self.inner.failed.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Error::Busy);
            }
            self.inner.coordinator.shutdown()?;
            self.inner
                .shutdown_requested
                .store(true, std::sync::atomic::Ordering::SeqCst);
            drop(compaction);
            self.inner.stop_compaction_workers(deadline)?;
            let failure = match self.inner.drain_storage(deadline) {
                Ok(()) => scan_failure,
                Err(Error::DeadlineExceeded) => return Err(Error::DeadlineExceeded),
                Err(error) => {
                    self.inner
                        .failed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    Some(error)
                }
            };
            self.inner.storage.device.shutdown(deadline)?;
            self.inner.release_stopped_scans()?;
            self.inner.release_stopped_storage()?;
            self.inner.release_stopped_checkpoint()?;
            self.inner.release_stopped_compaction()?;
            self.inner.release_stopped_gc()?;
            self.inner.release_stopped_checkpoint_release()?;
            *done = true;
            if let Some(error) = failure {
                return Err(error);
            }
        }
        Ok(ShutdownReport {
            device_drained: true,
        })
    }
}
impl<S: Schema> Builder<S> {
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }
    pub fn device(mut self, device: Box<dyn DeviceFactory>) -> Self {
        self.device = Some(device);
        self
    }
    /// 创建全新存储。必须提供设备工厂；文件根目录中已有存储材料时拒绝覆盖。
    /// 配置与内存准备在设备打开前校验，设备/格式错误原样返回。
    pub fn create(self) -> Result<RasterKV<S>, Error> {
        self.config.validate()?;
        if self.device.is_none() {
            return Err(Error::InvalidConfig {
                field: "device",
                reason: "必须提供设备工厂",
            });
        }
        let id = StoreId::generate()?;
        let io_capacity = self.config.io_capacity()?;
        let io = Arc::new(crate::engine::io_hub::CompletionHub::new(id, io_capacity)?);
        let schema = Arc::new(self.schema);
        let metrics = Arc::new(crate::engine::metrics::Metrics::new(
            self.config.statistics.enabled,
        ));
        let mut index = crate::index::MemIndex::new(self.config.index.clone())?;
        index.set_metrics(metrics.clone());
        let mut log = crate::log::HybridLog::new(
            self.config.log.clone(),
            Arc::new(crate::schema::SharedValue(schema.clone())),
        )?;
        if self.config.storage.pre_allocate_log {
            log.preallocate()?;
        }
        let epoch = crate::epoch::EpochManager::new()?;
        let coordinator = crate::coordination::Coordinator::new(self.config.session.max_sessions)?;
        let mut cache = crate::cache::ReadCache::new(self.config.cache.clone());
        cache.set_metrics(metrics.clone());
        cache.preallocate()?;
        let device =
            self.device
                .expect("设备工厂已检查")
                .open(crate::device::DeviceOpenOptions {
                    root: self.config.storage.root.clone(),
                    create_new: true,
                })?;
        let storage = crate::storage::SegmentedStorage::new(
            Arc::from(device),
            self.config.storage.root.clone(),
            self.config.storage.segment_bytes,
        )?;
        let store = RasterKV {
            inner: Arc::new(Engine {
                id,
                thread_sessions: Arc::new(crate::engine::thread_sessions::ThreadSessions::new(
                    self.config.session.max_sessions,
                )),
                metrics,
                io,
                scans: Default::default(),
                auto_compaction: Default::default(),
                compaction: std::sync::Mutex::new(Default::default()),
                gc: std::sync::Mutex::new(Default::default()),
                checkpoint_release: std::sync::Mutex::new(Default::default()),
                growth: std::sync::Mutex::new(Default::default()),
                checkpoints: std::sync::Mutex::new(Default::default()),
                storage_progress: std::sync::Mutex::new(Default::default()),
                schema,
                config: self.config,
                index,
                log,
                epoch,
                coordinator,
                storage,
                cache,
                shutdown_state: crate::sync::Mutex::new(false),
                version_permits: Default::default(),
                operations: (0..64).map(|_| crate::sync::Mutex::new(())).collect(),
                failed: std::sync::atomic::AtomicBool::new(false),
                shutdown_requested: std::sync::atomic::AtomicBool::new(false),
            }),
        };
        store.inner.start_auto_compaction()?;
        Ok(store)
    }

    /// 校验配对检查点、Schema 身份和材料后恢复到独立工作目录，成功才发布实例。
    /// 桶数须与索引材料一致；恢复报告中的身份经 continue_session 才成为活跃会话。
    /// 仅索引检查点不形成完整恢复集；不会读取 C++ 的磁盘字节格式。
    pub fn recover(self, set: RecoverySet) -> Result<(RasterKV<S>, RecoveryReport), Error> {
        self.config.validate()?;
        if self.device.is_none() {
            return Err(Error::InvalidConfig {
                field: "device",
                reason: "必须提供设备工厂",
            });
        }
        super::recover::recover(
            self.config,
            self.schema,
            self.device.expect("设备工厂已检查"),
            set,
        )
    }
}
