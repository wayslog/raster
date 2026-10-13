//! CPR checkpoint orchestration (FASTER `Checkpoint*`, `GlobalMoveToNextState`).
//!
//! Sessions drive the state machine from `refresh`; the session that finishes
//! a phase last moves the global state, and only the thread whose CAS
//! succeeds performs that transition's global work.
use super::checkpoint::{IndexMetadata, LogMetadata, SessionRecord};
use super::checkpoint_locks::CheckpointLocks;
use super::guid::Guid;
use super::kv::{Key, Value};
use super::state::{Action, Phase, SystemState};
use super::store::{FasterKv, Status};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub type IndexCallback = Box<dyn FnOnce(Status) + Send>;
/// Called once per participating session with its persisted serial number.
pub type LogCallback = Arc<dyn Fn(Status, u64) + Send + Sync>;

/// State of the checkpoint in progress. Touched only at phase changes.
#[derive(Default)]
pub(crate) struct CprRun {
    pub token: Guid,
    pub index_meta: IndexMetadata,
    pub log_meta: LogMetadata,
    pub index_callback: Option<IndexCallback>,
    pub log_callback: Option<LogCallback>,
    /// Outcome of the fuzzy index write (overflow bucket count).
    pub index_result: Option<io::Result<u64>>,
    /// Set if writing any artifact failed; callbacks then report `IoError`.
    pub failed: bool,
    pub started: Option<std::time::Instant>,
}

/// Prints phase transition times when `RASTER_CPR_TRACE` is set.
fn trace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("RASTER_CPR_TRACE").is_some())
}

struct LocksPtr(*mut CheckpointLocks);
// SAFETY: ownership moves into the epoch action that frees the table.
unsafe impl Send for LocksPtr {}
impl LocksPtr {
    /// # Safety
    /// Must be the unique owner of a table from `Box::into_raw`.
    unsafe fn free(self) {
        // SAFETY: guaranteed by the caller.
        drop(unsafe { Box::from_raw(self.0) });
    }
}

impl<K: Key, V: Value> FasterKv<K, V> {
    /// Takes a full (index + hybrid log) fold-over checkpoint. Returns its
    /// token, or `None` if another state-machine action is running or no
    /// checkpoint directory is configured. Sessions must keep refreshing.
    pub fn checkpoint(
        &self,
        index: Option<IndexCallback>,
        log: Option<LogCallback>,
    ) -> Option<Guid> {
        self.start_checkpoint(Action::CheckpointFull, index, log)
    }

    pub fn checkpoint_index(&self, index: Option<IndexCallback>) -> Option<Guid> {
        self.start_checkpoint(Action::CheckpointIndex, index, None)
    }

    pub fn checkpoint_hybrid_log(&self, log: Option<LogCallback>) -> Option<Guid> {
        self.start_checkpoint(Action::CheckpointHybridLog, None, log)
    }

    /// Current `{action, phase, version}`.
    pub fn system_state(&self) -> SystemState {
        self.state.load()
    }

    fn start_checkpoint(
        &self,
        action: Action,
        index_callback: Option<IndexCallback>,
        log_callback: Option<LogCallback>,
    ) -> Option<Guid> {
        let dirs = self.dirs.as_ref()?;
        let rest = self.state.load();
        if rest.phase != Phase::Rest || rest.action != Action::None {
            return None;
        }
        let started = SystemState { action, ..rest };
        if !self.state.compare_exchange(rest, started) {
            return None;
        }
        let token = Guid::new();
        if dirs.create(token).is_err() {
            self.state.store(rest);
            return None;
        }
        self.epoch.reset_phase_finished();
        {
            let mut run = self.cpr.lock().unwrap();
            *run = CprRun {
                token,
                index_meta: IndexMetadata {
                    version: rest.version,
                    table_size: self.index.buckets(),
                    ofb_count: 0,
                    log_begin_address: self.hlog.begin.load(),
                    checkpoint_start_address: self.hlog.tail(),
                },
                log_meta: LogMetadata {
                    version: rest.version,
                    ..Default::default()
                },
                index_callback,
                log_callback,
                started: Some(std::time::Instant::now()),
                ..Default::default()
            };
        }
        if action != Action::CheckpointIndex {
            let locks = Box::into_raw(Box::new(CheckpointLocks::new(self.index.buckets())));
            self.checkpoint_locks.store(locks, Ordering::Release);
        }
        self.state.store(started.next());
        Some(token)
    }

    /// Moves the global state from `current`; returns false if another
    /// thread already did. The winner performs the transition's global work.
    pub(crate) fn global_move_to_next_state(&self, current: SystemState) -> bool {
        let next = current.next();
        if !self.state.compare_exchange(current, next) {
            return false;
        }
        let mut run = self.cpr.lock().unwrap();
        if trace_enabled() {
            let elapsed = run.started.map(|t| t.elapsed()).unwrap_or_default();
            eprintln!("cpr {:?} -> {:?} at {elapsed:?}", current.phase, next.phase);
        }
        if next.phase == Phase::IndexCheckpoint {
            // Write the fuzzy index on this session thread, as FASTER's
            // buffered I/O does, without holding the run lock.
            let dir = self.dirs.as_ref().expect("checkpoint dir").index(run.token);
            drop(run);
            let result = self
                .index
                .write_checkpoint(&dir.join("ht.dat"), &dir.join("ofb.dat"));
            self.cpr.lock().unwrap().index_result = Some(result);
            return true;
        }
        match next.phase {
            Phase::Prepare if current.action == Action::CheckpointFull => {
                self.finish_index_checkpoint(&mut run);
            }
            Phase::GcInProgress => {
                drop(run);
                self.gc_truncate();
                return true;
            }
            Phase::GrowInProgress => {
                drop(run);
                self.grow_install();
                return true;
            }
            Phase::WaitFlush => {
                let final_address = self.hlog.tail();
                self.hlog.shift_read_only_to(final_address);
                run.log_meta.final_address = final_address;
                run.log_meta.flushed_address = final_address;
                let dir = self.dirs.as_ref().expect("checkpoint dir").log(run.token);
                run.failed |= run.log_meta.write(&dir).is_err();
            }
            Phase::Rest => {
                match current.action {
                    Action::CheckpointIndex => self.finish_index_checkpoint(&mut run),
                    Action::Gc => {
                        drop(run);
                        self.gc_complete();
                        return true;
                    }
                    Action::GrowIndex => {
                        drop(run);
                        self.grow_complete();
                        return true;
                    }
                    _ => {
                        let locks = self
                            .checkpoint_locks
                            .swap(std::ptr::null_mut(), Ordering::AcqRel);
                        let locks = LocksPtr(locks);
                        // SAFETY: no session can still use the table once every
                        // session has moved past this epoch.
                        self.epoch.bump_with_action(move || unsafe { locks.free() });
                    }
                }
                run.log_callback = None;
            }
            _ => {}
        }
        true
    }

    fn finish_index_checkpoint(&self, run: &mut CprRun) {
        match run.index_result.take() {
            Some(Ok(ofb_count)) => run.index_meta.ofb_count = ofb_count,
            _ => run.failed = true,
        }
        let dir = self.dirs.as_ref().expect("checkpoint dir").index(run.token);
        run.failed |= run.index_meta.write(&dir).is_err();
        let status = if run.failed {
            Status::IoError
        } else {
            Status::Ok
        };
        if let Some(callback) = run.index_callback.take() {
            callback(status);
        }
    }

    pub(crate) fn index_checkpoint_complete(&self) -> bool {
        let run = self.cpr.lock().unwrap();
        run.index_result.is_some()
    }

    pub(crate) fn register_session(&self, guid: Guid) {
        self.cpr.lock().unwrap().log_meta.sessions.push(guid);
    }

    pub(crate) fn log_flushed(&self) -> bool {
        let final_address = self.cpr.lock().unwrap().log_meta.final_address;
        self.hlog.flushed_until.load() >= final_address
    }

    pub(crate) fn persist_session(&self, record: SessionRecord) {
        let mut run = self.cpr.lock().unwrap();
        let dir = self.dirs.as_ref().expect("checkpoint dir").log(run.token);
        run.failed |= record.write(&dir).is_err();
    }

    pub(crate) fn persistence_callback(&self, serial: u64) {
        let (callback, status) = {
            let run = self.cpr.lock().unwrap();
            let status = if run.failed {
                Status::IoError
            } else {
                Status::Ok
            };
            (run.log_callback.clone(), status)
        };
        if let Some(callback) = callback {
            callback(status, serial);
        }
    }

    #[inline]
    pub(crate) fn locks(&self) -> Option<&CheckpointLocks> {
        // SAFETY: the table is freed only by an epoch action after every
        // session has left the checkpoint; callers are epoch-protected.
        unsafe { self.checkpoint_locks.load(Ordering::Acquire).as_ref() }
    }
}

impl<K: Key, V: Value> Drop for FasterKv<K, V> {
    fn drop(&mut self) {
        // No sessions remain (they borrow the store); run deferred actions
        // such as freeing the lock table while the log is still alive.
        self.epoch.drain_all();
        let locks = *self.checkpoint_locks.get_mut();
        if !locks.is_null() {
            // SAFETY: no sessions remain; the table is uniquely owned here.
            unsafe { LocksPtr(locks).free() };
        }
    }
}
