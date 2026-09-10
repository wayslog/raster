//! 四种操作的编排中心；当前只拒绝请求，不消耗序号或调用用户函数。
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
