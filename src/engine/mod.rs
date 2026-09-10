//! 内存四操作编排中心；磁盘 Pending 与持久化路径分阶段实现。
mod conditional_copy;
mod delete;
mod pending;
mod progress;
mod read;
mod rmw;
mod upsert;

use crate::{
    cache::ReadCache, config::Config, coordination::Coordinator, epoch::EpochManager,
    index::MemIndex, log::HybridLog, schema::Schema, storage::SegmentedStorage, types::*,
};
pub(crate) use pending::SessionRuntime;

pub(crate) struct Engine<S: Schema> {
    pub id: StoreId,
    pub operations: Vec<crate::sync::Mutex<()>>,
    pub failed: std::sync::atomic::AtomicBool,
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
    pub(crate) fn not_ready<T>(&self, module: &'static str) -> Result<T, Error> {
        Err(Error::unimplemented(module))
    }
}

impl<S: Schema> Engine<S> {
    fn admit(&self, session: &mut SessionRuntime, serial: Serial) -> Result<(), Error> {
        if session.closing || self.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("会话关闭或引擎失败"));
        }
        self.coordinator.accept_serial(session.id, serial)?;
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
