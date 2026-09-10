//! 创建/恢复只在引擎完全就绪后返回实例；骨架不会创建文件或伪造就绪状态。
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
    pub fn start_session(&self, _options: SessionOptions) -> Result<Session<S>, Error> {
        Err(Error::unimplemented("coordination::start_session"))
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
    /// 未来须立即报告活跃会话，不能阻塞等待本线程自己的 Session。
    pub fn shutdown(&self, _deadline: Deadline) -> Result<ShutdownReport, Error> {
        Err(Error::unimplemented("engine::shutdown"))
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
        let _schema = self.schema;
        Err(Error::unimplemented("engine::create"))
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
