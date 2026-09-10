//! 创建完成内存组件与设备初始化后返回；恢复仍待检查点协议接入。
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
        self.inner.coordinator.enroll(id)?;
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
            runtime: crate::engine::SessionRuntime::new(id, last_accepted),
            local: std::marker::PhantomData,
        })
    }
    pub fn continue_session(&self, _id: SessionId) -> Result<ResumedSession<S>, Error> {
        Err(Error::unimplemented("coordination::continue_session"))
    }
    pub fn maintenance(&self) -> Maintenance<S> {
        Maintenance {
            inner: Arc::clone(&self.inner),
        }
    }
    pub fn diagnostics(&self) -> Result<Diagnostics, Error> {
        Err(Error::unimplemented("diagnostics::snapshot"))
    }
    pub fn scan(&self, _options: ScanOptions) -> Result<RecordScanner<S>, Error> {
        Err(Error::unimplemented("scan::open"))
    }
    /// 立即拒绝仍有活跃会话的关闭，不等待本线程自己的 Session。
    pub fn shutdown(&self, deadline: Deadline) -> Result<ShutdownReport, Error> {
        let mut done = self
            .inner
            .shutdown_state
            .lock()
            .map_err(|_| Error::InvalidState("关闭锁中毒"))?;
        if !*done {
            self.inner.coordinator.shutdown()?;
            self.inner.storage.device.shutdown(deadline)?;
            *done = true;
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
        if self.config.cache.enabled
            || self.config.maintenance.auto_compaction
            || self.config.storage.pre_allocate_log
        {
            return Err(Error::NotImplemented {
                module: "engine::高级配置",
            });
        }
        let id = StoreId::generate()?;
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
                schema,
                config: self.config,
                index,
                log,
                epoch,
                coordinator,
                storage,
                cache,
                shutdown_state: crate::sync::Mutex::new(false),
                operations: (0..64).map(|_| crate::sync::Mutex::new(())).collect(),
                failed: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }

    pub fn recover(self, _set: RecoverySet) -> Result<(RasterKV<S>, RecoveryReport), Error> {
        self.config.validate()?;
        if self.device.is_none() {
            return Err(Error::InvalidConfig {
                field: "device",
                reason: "必须提供设备工厂",
            });
        }
        Err(Error::unimplemented("checkpoint::recover"))
    }
}
