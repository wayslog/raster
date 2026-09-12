//! Release the instance only after all components have been created and restored.;Persistent sessions must be explicitly continued.
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

/// shared keyspace,Index,Mixed logs and maintenance history;Clone Share the same instance.
/// Each thread passes its own Session Execute business.Restoration and creation portal see Builder.
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
/// Collect configuration and real device factory;Return only if all resources are successfully prepared or restored. RasterKV.
pub struct Builder<S: Schema> {
    schema: S,
    config: Config,
    device: Option<Box<dyn DeviceFactory>>,
}
/// Resumable thread session and checkpoint persistence progress;The new sequence number must be greater than the current session number last_accepted.
/// When the same instance is closed and then connected again,The current acceptance progress may have exceeded the checkpoint progress.serial.
pub struct ResumedSession<S: Schema> {
    pub session: Session<S>,
    pub progress: super::maintenance::DurableProgress,
}
#[derive(Debug)]
pub struct ShutdownReport {
    pub device_drained: bool,
}

impl<S: Schema> RasterKV<S> {
    /// Persistent storage of identities,Used to assemble recovery sets;Remain unchanged after restore.
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
    /// Register the session on the calling thread;There can only be one active session for the same thread and storage.
    /// explicit identity duplication,Reject on insufficient capacity or instance failure,Restoration of identity continue_session.
    pub fn start_session(&self, options: SessionOptions) -> Result<Session<S>, Error> {
        if self.inner.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("engine_failed_closed"));
        }
        let thread_session = self.inner.thread_sessions.enter()?;
        let id = match options.id {
            Some(id) => id,
            None => SessionId::generate()?,
        };
        let registered = self.inner.coordinator.enroll_registered(id)?;
        let participant = match self.inner.epoch.register() {
            Ok(id) => id,
            Err(error) => {
                let _ = self.inner.coordinator.leave(id);
                return Err(error);
            }
        };
        Ok(Session {
            thread_session: Some(thread_session),
            engine: self.inner.clone(),
            id,
            participant: Some(participant),
            runtime: crate::engine::SessionRuntime::new(id, registered),
            local: std::marker::PhantomData,
        })
    }
    /// Continue the identity in the recovery report;unknown,Reject if still active or if there is already a session on the same thread.
    /// Can be resumed after closing,last_accepted Keep the latest serial number accepted by this instance.
    pub fn continue_session(&self, id: SessionId) -> Result<ResumedSession<S>, Error> {
        if self.inner.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::InvalidState("engine_failed_closed"));
        }
        let thread_session = self.inner.thread_sessions.enter()?;
        let (registered, serial, durable_version) = self.inner.coordinator.resume_registered(id)?;
        let participant = match self.inner.epoch.register() {
            Ok(participant) => participant,
            Err(error) => {
                let _ = self.inner.coordinator.leave(id);
                return Err(error);
            }
        };
        Ok(ResumedSession {
            session: Session {
                thread_session: Some(thread_session),
                engine: self.inner.clone(),
                id,
                participant: Some(participant),
                runtime: crate::engine::SessionRuntime::new(id, registered),
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
    /// Read real component status;Concurrent fields do not constitute transaction snapshots,Span and bucket occupancy are not equal to the number of valid keys.
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
                .ok_or(Error::InvalidState("Log span inversion"))?,
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
    /// Enable subsequent requests and internal event sampling,Do not clear existing counts.
    pub fn enable_stats_collection(&self) {
        self.inner.metrics.enable(true);
    }
    /// Stop sampling new requests;The sampling request record has been terminated,Real-time resource diagnostics remain enabled.
    pub fn disable_stats_collection(&self) {
        self.inner.metrics.enable(false);
    }
    /// Return owned historical counters, zeroed when creating or restoring a new instance,
    /// without entering a checkpoint.
    pub fn statistics(&self) -> crate::diagnostics::Statistics {
        self.inner.metrics.snapshot()
    }
    /// Let the caller choose the output location; this library does not install a global log subscriber.
    pub fn write_statistics(&self, output: &mut impl std::io::Write) -> Result<(), Error> {
        write!(output, "{}", self.statistics()).map_err(Error::Io)
    }
    pub fn scan(&self, options: ScanOptions) -> Result<RecordScanner<S>, Error> {
        RecordScanner::open(self.inner.clone(), options)
    }
    /// Stop and drain automatic maintenance first;active session,Scan or manual tasks still return Busy,Can be pushed forward and tried again.
    /// Stop and drain automatic maintenance and equipment;Success does not automatically create a checkpoint.
    /// Active sessions or manual maintenance caused Busy;diagnostics Provides a list of active session identities.
    /// The session should be closed first and the original maintenance ticket pushed forward;Renewable call after timeout.
    pub fn shutdown(&self, deadline: Deadline) -> Result<ShutdownReport, Error> {
        let mut done = self
            .inner
            .shutdown_state
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => Error::Busy,
                std::sync::TryLockError::Poisoned(_) => Error::InvalidState("Close lock poisoning"),
            })?;
        if !*done {
            // First disable automatic acceptance and clear accepted tasks;Even if you return later with an active session Busy,Stop request still valid.
            self.inner.auto_compaction.request_stop()?;
            self.maintenance().wait_auto_compaction(deadline)?;
            // Register Scan and Close Shared Close Lock;Active scan rejected immediately,Abandoned scans are drained by deadline.
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
            // Composite tasks are not yet terminated between two global actions,Cannot allow closure to cross this gap.
            let compaction = self
                .inner
                .compaction
                .try_lock()
                .map_err(|error| match error {
                    std::sync::TryLockError::WouldBlock => Error::Busy,
                    _ => Error::InvalidState("Close encounter compression task lock poisoning"),
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
    /// Create new storage.Equipment factory must be provided;Refuse to overwrite when there is already stored material in the file root directory.
    /// Configuration and memory preparation are verified before turning on the device,Equipment/Format errors are returned as is.
    pub fn create(self) -> Result<RasterKV<S>, Error> {
        self.config.validate()?;
        if self.device.is_none() {
            return Err(Error::InvalidConfig {
                field: "device",
                reason: "Equipment factory must be provided",
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
        let device = self.device.expect("Equipment factory inspected").open(
            crate::device::DeviceOpenOptions {
                root: self.config.storage.root.clone(),
                create_new: true,
            },
        )?;
        let storage = crate::storage::SegmentedStorage::new(
            Arc::from(device),
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

    /// Verification Pairing Checkpoint,Schema Restore identities and materials to separate working directories,Publish instance only after success.
    /// The number of barrels must be consistent with the index material;Identity experience in recovery report continue_session to become an active session.
    /// Index checkpoints alone do not form a complete recovery set;Will not read C++ disk byte format.
    pub fn recover(self, set: RecoverySet) -> Result<(RasterKV<S>, RecoveryReport), Error> {
        self.config.validate()?;
        if self.device.is_none() {
            return Err(Error::InvalidConfig {
                field: "device",
                reason: "Equipment factory must be provided",
            });
        }
        super::recover::recover(
            self.config,
            self.schema,
            self.device.expect("Equipment factory inspected"),
            set,
        )
    }
}
