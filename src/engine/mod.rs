//! four operations,pending tasks,Orchestration center for persistence and maintenance.
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
            return Err(Error::InvalidState("Session closed or engine failed"));
        }
        // Active identities can only be submitted by the current session;create/Continue to read registration progress,admit Synchronize local value after success.
        // Only illegal serial numbers are rejected in advance here;Final acceptance still checks global identity,Version and serial number.
        if session
            .current
            .last_accepted
            .is_some_and(|last| serial <= last)
        {
            return Err(Error::InvalidState(
                "operation_serial_must_increase_strictly",
            ));
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
                Err(Error::InvalidState("request key or encoder panicked"))
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
            // Old output may also have user destruction,still released within borders.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result)));
            return Err(OperationError {
                cause: Error::InvalidState("Request destructor panic"),
                effect,
            });
        }
        result
    }
    fn admit(&self, session: &mut SessionRuntime, serial: Serial) -> Result<(), Error> {
        if session.closing || self.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("Session closed or engine failed"));
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
            crate::index::IndexHead::Cache(_) => {
                Err(Error::InvalidState("The cache path is not connected yet"))
            }
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
    /// Ready Direct handover of owned results;Still ends up the same and I/O Sampling caliber record statistics.
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
