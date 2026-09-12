//! A set of persistent worker threads per compaction;Keep requests in transit in slots,Idle threads only hold weak storage references.
use super::{ConditionalCopy, CopyResult, Engine};
use crate::{schema::Schema, types::*};
use std::{
    sync::{
        Arc, Condvar, Mutex, TryLockError, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
#[derive(Default)]
struct Slot {
    copy: Option<ConditionalCopy>,
    copied: u64,
    accounted: bool,
}
#[derive(Default)]
struct Lane {
    slot: Mutex<Slot>,
    wake: Condvar,
}
struct Shared {
    lanes: Vec<Arc<Lane>>,
    closing: AtomicBool,
    abort: AtomicBool,
    error: Mutex<Option<Error>>,
}
impl Shared {
    fn wake(&self) {
        for lane in &self.lanes {
            lane.wake.notify_all();
        }
    }
    fn fail(&self, error: Error) {
        // The error slot only saves owned errors,No user code is executed within this lock.
        self.error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(error);
        self.abort.store(true, Ordering::SeqCst);
        self.wake();
    }
}
pub(super) struct Workers {
    shared: Arc<Shared>,
    threads: Vec<Option<JoinHandle<()>>>,
    known_copied: Vec<u64>,
    issued: u64,
}
pub(super) struct Progress {
    pub copied: u64,
    pub failure: Option<Error>,
    pub finished: bool,
}
impl Workers {
    pub fn new<S: Schema>(engine: &Arc<Engine<S>>, count: usize) -> Result<Self, Error> {
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;
        let mut threads = Vec::new();
        threads
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;
        let mut known_copied = Vec::new();
        known_copied
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;
        known_copied.resize(count, 0);
        for _ in 0..count {
            lanes.push(Arc::new(Lane::default()));
        }
        let shared = Arc::new(Shared {
            lanes,
            closing: false.into(),
            abort: false.into(),
            error: Mutex::new(None),
        });
        let mut result = Self {
            shared,
            threads,
            known_copied,
            issued: 0,
        };
        for index in 0..count {
            let weak = Arc::downgrade(engine);
            let shared = result.shared.clone();
            let lane = shared.lanes[index].clone();
            match thread::Builder::new()
                .name(format!("rastercompaction-{index}"))
                .spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run(&weak, &shared, &lane)
                    }));
                    let error = match outcome {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error),
                        Err(_) => Some(Error::InvalidState("Compaction worker thread panics")),
                    };
                    if let Some(error) = error {
                        if let Some(engine) = weak.upgrade() {
                            engine.failed.store(true, Ordering::SeqCst);
                        }
                        shared.fail(error);
                    }
                }) {
                Ok(handle) => result.threads.push(Some(handle)),
                Err(error) => {
                    result.abort();
                    // No requests have been received yet,The started thread only needs to exit idle waiting.
                    for handle in &mut result.threads {
                        if let Some(handle) = handle.take() {
                            let _ = handle.join();
                        }
                    }
                    return Err(Error::Io(error));
                }
            }
        }
        Ok(result)
    }
    #[allow(
        clippy::result_large_err,
        reason = "Return undelivered owned copy requests unchanged during busy times"
    )]
    pub fn submit(&mut self, copy: ConditionalCopy) -> Result<(), Rejected<ConditionalCopy>> {
        if self.shared.abort.load(Ordering::SeqCst) || self.shared.closing.load(Ordering::SeqCst) {
            return Err(Rejected {
                request: copy,
                reason: Error::Busy,
            });
        }
        let Some(next) = self.issued.checked_add(1) else {
            return Err(Rejected {
                request: copy,
                reason: Error::CapacityExceeded,
            });
        };
        for lane in &self.shared.lanes {
            let mut slot = match lane.slot.try_lock() {
                Ok(slot) => slot,
                Err(TryLockError::WouldBlock) => continue,
                Err(_) => {
                    return Err(Rejected {
                        request: copy,
                        reason: Error::InvalidState("Compression work tank poisoning"),
                    });
                }
            };
            if slot.copy.is_none() {
                // The worker may have exited due to the stop bit after the first check;Recheck within the slot lock to avoid orphaned delivery.
                if self.shared.abort.load(Ordering::SeqCst)
                    || self.shared.closing.load(Ordering::SeqCst)
                {
                    return Err(Rejected {
                        request: copy,
                        reason: Error::Busy,
                    });
                }
                slot.copy = Some(copy);
                slot.accounted = false;
                self.issued = next;
                lane.wake.notify_one();
                return Ok(());
            }
        }
        Err(Rejected {
            request: copy,
            reason: Error::Busy,
        })
    }
    pub fn close(&self) {
        self.shared.closing.store(true, Ordering::SeqCst);
        self.shared.wake();
    }
    pub fn abort(&self) {
        self.shared.abort.store(true, Ordering::SeqCst);
        self.shared.wake();
    }
    pub fn poll<S: Schema>(&mut self, engine: &Engine<S>) -> Result<Progress, Error> {
        let mut finished = true;
        for handle in &mut self.threads {
            if let Some(thread) = handle {
                if thread.is_finished() {
                    if handle.take().expect("Thread completed").join().is_err() {
                        engine.failed.store(true, Ordering::SeqCst);
                        self.shared
                            .fail(Error::InvalidState("Compression thread exits abnormally"));
                    }
                } else {
                    finished = false;
                }
            }
        }
        for (index, lane) in self.shared.lanes.iter().enumerate() {
            match lane.slot.try_lock() {
                Ok(slot) => self.known_copied[index] = slot.copied,
                Err(TryLockError::WouldBlock) => finished = false,
                Err(TryLockError::Poisoned(error)) => {
                    engine.failed.store(true, Ordering::SeqCst);
                    self.shared
                        .fail(Error::InvalidState("Compression work tank poisoning"));
                    self.known_copied[index] = error.into_inner().copied;
                }
            }
        }
        let copied = self.known_copied.iter().try_fold(0u64, |total, &n| {
            total.checked_add(n).ok_or(Error::CapacityExceeded)
        })?;
        if copied > self.issued {
            return Err(Error::InvalidState(
                "Compression count exceeds delivered requests",
            ));
        }
        let failure = self
            .shared
            .error
            .lock()
            .map_err(|_| Error::InvalidState("Compression error slot poisoning"))?
            .take();
        Ok(Progress {
            copied,
            failure,
            finished,
        })
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        // Already before normal reporting join;Exception discard only requests stop,Does not block destruction or wait for the current thread itself.
        self.abort();
    }
}
fn account(slot: &mut Slot) -> Result<(), Error> {
    if !slot.accounted
        && slot
            .copy
            .as_ref()
            .is_some_and(|copy| copy.published_address().is_some())
    {
        slot.copied = slot.copied.checked_add(1).ok_or(Error::CapacityExceeded)?;
        slot.accounted = true;
    }
    Ok(())
}
fn run<S: Schema>(weak: &Weak<Engine<S>>, shared: &Shared, lane: &Lane) -> Result<(), Error> {
    loop {
        let mut slot = lane
            .slot
            .lock()
            .map_err(|_| Error::InvalidState("Compression work tank poisoning"))?;
        while slot.copy.is_none() {
            if shared.abort.load(Ordering::SeqCst) || shared.closing.load(Ordering::SeqCst) {
                return Ok(());
            }
            // Bounded wait while observing stop bit,Avoid notifications preceded by wait The staggered loss of shutdown signal.
            slot = lane
                .wake
                .wait_timeout(slot, Duration::from_millis(10))
                .map_err(|_| Error::InvalidState("Compression job wait lock poisoning"))?
                .0;
        }
        let Some(engine) = weak.upgrade() else {
            return Ok(());
        };
        if engine.failed.load(Ordering::SeqCst) {
            account(&mut slot)?;
            return Ok(());
        }
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<bool, Error> {
                // preflight count;Do not commit new reads after stopping,Only charges accepted completions.
                slot.copied.checked_add(1).ok_or(Error::CapacityExceeded)?;
                engine
                    .io
                    .poll(&*engine.storage.device, PollBudget::default())
                    .inspect_err(|_| {
                        engine.failed.store(true, Ordering::SeqCst);
                    })?;
                if shared.abort.load(Ordering::SeqCst) {
                    return slot
                        .copy
                        .as_mut()
                        .expect("Slot request exists")
                        .drain(&engine.storage);
                }
                let step = engine.conditional_copy(
                    slot.copy.as_mut().expect("Slot request exists"),
                    PollBudget::default(),
                )?;
                Ok(matches!(step, CopyResult::Copied(_) | CopyResult::Obsolete))
            }));
        match outcome {
            Ok(Ok(done)) => {
                account(&mut slot)?;
                if done {
                    slot.copy = None;
                }
            }
            Ok(Err(error)) => {
                account(&mut slot)?;
                if matches!(error, Error::InvalidState(_)) {
                    engine.failed.store(true, Ordering::SeqCst);
                }
                shared.fail(error);
            }
            Err(_) => {
                account(&mut slot)?;
                engine.failed.store(true, Ordering::SeqCst);
                shared.fail(Error::InvalidState("Compression work step panic"));
            }
        }
        let active = slot.copy.is_some();
        if engine.failed.load(Ordering::SeqCst) {
            return Ok(());
        }
        drop(slot);
        drop(engine);
        if active {
            thread::park_timeout(Duration::from_micros(100));
        }
    }
}
