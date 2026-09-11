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
pub struct Builder<S: Schema> {
    schema: S,
    config: Config,
    device: Option<Box<dyn DeviceFactory>>,
}
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
    pub fn start_session(&self, options: SessionOptions) -> Result<Session<S>, Error> {
        if self.inner.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("引擎已失败关闭"));
        }
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
            engine: self.inner.clone(),
            id,
            participant: Some(participant),
            runtime: crate::engine::SessionRuntime::new(id, last_accepted, version),
            local: std::marker::PhantomData,
        })
    }
    pub fn continue_session(&self, id: SessionId) -> Result<ResumedSession<S>, Error> {
        if self.inner.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("引擎已失败关闭"));
        }
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
    pub fn diagnostics(&self) -> Result<Diagnostics, Error> {
        Err(Error::unimplemented("diagnostics::snapshot"))
    }
    pub fn scan(&self, options: ScanOptions) -> Result<RecordScanner<S>, Error> {
        RecordScanner::open(self.inner.clone(), options)
    }
    /// 活跃会话或扫描使关闭立即返回 Busy；已放弃扫描的在途读取按截止时间排空。
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
    pub fn create(self) -> Result<RasterKV<S>, Error> {
        self.config.validate()?;
        if self.device.is_none() {
            return Err(Error::InvalidConfig {
                field: "device",
                reason: "必须提供设备工厂",
            });
        }
        if self.config.maintenance.auto_compaction || self.config.storage.pre_allocate_log {
            return Err(Error::NotImplemented {
                module: "engine::高级配置",
            });
        }
        let id = StoreId::generate()?;
        let io_capacity = self.config.io_capacity()?;
        let io = Arc::new(crate::engine::io_hub::CompletionHub::new(id, io_capacity)?);
        let schema = Arc::new(self.schema);
        let index = crate::index::MemIndex::new(self.config.index.clone())?;
        let log = crate::log::HybridLog::new(
            self.config.log.clone(),
            Arc::new(crate::schema::SharedValue(schema.clone())),
        )?;
        let epoch = crate::epoch::EpochManager::new()?;
        let coordinator = crate::coordination::Coordinator::new(self.config.session.max_sessions)?;
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
        let cache = crate::cache::ReadCache::new(self.config.cache.clone());
        Ok(RasterKV {
            inner: Arc::new(Engine {
                id,
                io,
                scans: Default::default(),
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
        })
    }

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
