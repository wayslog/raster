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
        if self
            .coordinator
            .last_accepted(session.id)?
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
