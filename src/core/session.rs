//! Session execution context (FASTER `ExecutionContext`, `HandleSpecialPhases`).
//!
//! A session protects the epoch for its whole life and advances it with
//! `refresh`, which also walks the session through any checkpoint phases.
//! In REST, operations touch only session-owned fields. Operations that need
//! a record from the device, or must wait for the checkpoint state machine,
//! copy their context, return `Pending`, and finish in `complete_pending`.
use super::address::Address;
use super::checkpoint::SessionRecord;
use super::epoch::SlotId;
use super::guid::Guid;
use super::kv::{DeleteContext, Key, ReadContext, RmwContext, UpsertContext, Value};
use super::pending::{
    DiskRecord, PendingDelete, PendingOp, PendingRead, PendingRmw, PendingUpsert, Step,
};
use super::record::Layout;
use super::state::{Action, Phase, SystemState};
use super::store::{FasterKv, OpMode, Outcome, Status};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

type Response = (u64, io::Result<Vec<u8>>);

struct Entry<K, V> {
    op: Box<dyn PendingOp<K, V>>,
    /// Checkpoint version the request was issued in.
    version: u32,
    /// Holds the old-side checkpoint lock for its key.
    marked: bool,
}

pub struct Session<'a, K: Key, V: Value> {
    store: &'a FasterKv<K, V>,
    slot: Option<SlotId>,
    guid: Guid,
    serial: u64,
    phase: Phase,
    version: u32,
    /// Serial at the switch to version v+1; persisted by the checkpoint.
    checkpoint_serial: u64,
    next_id: u64,
    pending: HashMap<u64, Entry<K, V>>,
    responses: Arc<Mutex<Vec<Response>>>,
    /// Pending operations to re-run after the next refresh.
    retry: Vec<u64>,
    /// Requests with smaller ids were issued before the current GC started.
    gc_barrier: u64,
    /// Generation of the state-machine run this session is following.
    generation: u16,
}

impl<'a, K: Key, V: Value> Session<'a, K, V> {
    pub(crate) fn new(store: &'a FasterKv<K, V>, guid: Guid, serial: u64) -> Self {
        let state = store.state.load();
        assert_eq!(state.phase, Phase::Rest, "sessions start only in REST");
        let slot = store.epoch.reserve().expect("all epoch slots are in use");
        store.epoch.protect_and_drain(&slot);
        Self {
            store,
            slot: Some(slot),
            guid,
            serial,
            phase: Phase::Rest,
            version: state.version,
            checkpoint_serial: serial,
            next_id: 0,
            pending: HashMap::new(),
            responses: Arc::default(),
            retry: Vec::new(),
            gc_barrier: 0,
            generation: state.generation,
        }
    }

    #[inline(always)]
    fn slot(&self) -> &SlotId {
        self.slot.as_ref().expect("live session")
    }

    pub fn guid(&self) -> Guid {
        self.guid
    }

    /// Serial number of the last operation issued by this session.
    pub fn serial(&self) -> u64 {
        self.serial
    }

    /// Number of operations still waiting in `complete_pending`.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    #[inline(always)]
    fn mode(&self) -> OpMode {
        OpMode {
            phase: self.phase,
            version: self.version,
            holds_old: false,
        }
    }

    #[inline]
    pub fn read<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: ReadContext<K, V> + Clone + 'static,
    {
        if self.phase != Phase::Rest {
            self.heavy_enter();
        }
        // The checkpoint persists the serial of the last version-v operation.
        // If this operation shifts to v+1, the previous serial is restored
        // while the session switches versions.
        let previous = std::mem::replace(&mut self.serial, serial);
        loop {
            match self.store.internal_read(ctx, self.mode()) {
                Outcome::Done(status) => return status,
                Outcome::Shift => self.shift(previous, serial),
                outcome => {
                    return self.suspend(
                        outcome,
                        PendingRead {
                            ctx: ctx.clone(),
                            truncations: self
                                .store
                                .truncations
                                .load(std::sync::atomic::Ordering::Acquire),
                        },
                    );
                }
            }
        }
    }

    #[inline]
    pub fn upsert<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        if self.phase != Phase::Rest {
            self.heavy_enter();
        }
        // The checkpoint persists the serial of the last version-v operation.
        // If this operation shifts to v+1, the previous serial is restored
        // while the session switches versions.
        let previous = std::mem::replace(&mut self.serial, serial);
        loop {
            match self.store.internal_upsert(self.slot(), ctx, self.mode()) {
                Outcome::Done(status) => return status,
                Outcome::Shift => self.shift(previous, serial),
                outcome => return self.suspend(outcome, PendingUpsert { ctx: ctx.clone() }),
            }
        }
    }

    #[inline]
    pub fn rmw<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: RmwContext<K, V> + Clone + 'static,
    {
        if self.phase != Phase::Rest {
            self.heavy_enter();
        }
        // The checkpoint persists the serial of the last version-v operation.
        // If this operation shifts to v+1, the previous serial is restored
        // while the session switches versions.
        let previous = std::mem::replace(&mut self.serial, serial);
        loop {
            match self.store.internal_rmw(self.slot(), ctx, self.mode()) {
                Outcome::Done(status) => return status,
                Outcome::Shift => self.shift(previous, serial),
                outcome => {
                    let observed = match outcome {
                        Outcome::Disk { observed, .. } => observed,
                        _ => Address::default(),
                    };
                    let op = PendingRmw {
                        ctx: ctx.clone(),
                        observed,
                    };
                    return self.suspend(outcome, op);
                }
            }
        }
    }

    #[inline]
    pub fn delete<C>(&mut self, ctx: &C, serial: u64) -> Status
    where
        C: DeleteContext<K> + Clone + 'static,
    {
        if self.phase != Phase::Rest {
            self.heavy_enter();
        }
        // The checkpoint persists the serial of the last version-v operation.
        // If this operation shifts to v+1, the previous serial is restored
        // while the session switches versions.
        let previous = std::mem::replace(&mut self.serial, serial);
        loop {
            match self.store.internal_delete(self.slot(), ctx, self.mode()) {
                Outcome::Done(status) => return status,
                Outcome::Shift => self.shift(previous, serial),
                outcome => return self.suspend(outcome, PendingDelete { ctx: ctx.clone() }),
            }
        }
    }

    pub(crate) fn store(&self) -> &'a FasterKv<K, V> {
        self.store
    }

    /// Copies a record to the tail if it is still the newest version of its
    /// key; used by compaction. Returns Ok (copied), Aborted or Pending.
    pub(crate) fn copy_record(&mut self, mut request: super::compact::CopyRequest<K>) -> Status {
        if self.phase != Phase::Rest {
            self.heavy_enter();
        }
        loop {
            match self
                .store
                .conditional_insert(self.slot(), &mut request, self.mode())
            {
                Outcome::Done(status) => return status,
                Outcome::Shift => self.shift(self.serial, self.serial),
                outcome => {
                    let observed = match outcome {
                        Outcome::Disk { observed, .. } => observed,
                        _ => Address::default(),
                    };
                    let op = super::compact::PendingCopy { request, observed };
                    return self.suspend(outcome, op);
                }
            }
        }
    }

    /// Waits out index growth, during which no session may use the index
    /// (FASTER `HeavyEnter`); refreshing also helps split the table.
    #[cold]
    fn heavy_enter(&mut self) {
        while self.growing() {
            self.refresh();
            if self.growing() {
                std::thread::yield_now();
            }
        }
    }

    #[inline(always)]
    fn growing(&self) -> bool {
        matches!(self.phase, Phase::GrowPrepare | Phase::GrowInProgress)
    }

    /// A CPR shift means the global state already reached v+1: catch up.
    #[cold]
    fn shift(&mut self, previous: u64, serial: u64) {
        self.serial = previous;
        let version = self.version;
        while self.version == version {
            self.refresh();
            if self.version == version {
                std::thread::yield_now();
            }
        }
        self.serial = serial;
    }

    #[cold]
    fn suspend(&mut self, outcome: Outcome, op: impl PendingOp<K, V> + 'static) -> Status {
        let id = self.next_id;
        self.next_id += 1;
        let step = match outcome {
            Outcome::Disk {
                address, marked, ..
            } => Step::Read { address, marked },
            Outcome::RetryLater { marked } => Step::RetryLater { marked },
            Outcome::Done(_) | Outcome::Shift => unreachable!("not a pending outcome"),
        };
        let entry = Entry {
            op: Box::new(op),
            version: self.version,
            marked: false,
        };
        self.pending.insert(id, entry);
        self.advance(id, step);
        Status::Pending
    }

    fn issue(&self, id: u64, address: Address) {
        let responses = self.responses.clone();
        self.store.hlog.read_async(
            address,
            Layout::<K, V>::SIZE,
            Box::new(move |result| responses.lock().unwrap().push((id, result))),
        );
    }

    fn advance(&mut self, id: u64, step: Step) {
        match step {
            Step::Done => {
                let entry = self.pending.remove(&id).expect("pending entry");
                if entry.marked {
                    let locks = self
                        .store
                        .locks()
                        .expect("marked requests outlive the checkpoint");
                    locks.unlock_old(entry.op.hash());
                }
            }
            Step::Read { address, marked } => {
                self.pending.get_mut(&id).expect("pending entry").marked |= marked;
                self.issue(id, address);
            }
            Step::RetryLater { marked } => {
                self.pending.get_mut(&id).expect("pending entry").marked |= marked;
                self.retry.push(id);
            }
        }
    }

    /// Mode for continuing `entry`: requests from the previous version keep
    /// running as version-v operations under their old-side lock.
    fn entry_mode(&self, entry: &Entry<K, V>) -> OpMode {
        if entry.version == self.version {
            OpMode {
                holds_old: entry.marked,
                ..self.mode()
            }
        } else {
            OpMode {
                phase: Phase::Prepare,
                version: entry.version,
                holds_old: entry.marked,
            }
        }
    }

    /// Advances this session's epoch, runs ready deferred actions, and takes
    /// part in any checkpoint state machine in progress.
    #[inline]
    pub fn refresh(&mut self) {
        self.store.epoch.protect_and_drain(self.slot());
        if self.phase != Phase::Rest || self.store.state.load().phase != Phase::Rest {
            self.handle_special_phases();
        }
    }

    #[cold]
    fn handle_special_phases(&mut self) {
        let store = self.store;
        let final_state = store.state.load();
        if final_state.phase == Phase::Rest {
            self.phase = Phase::Rest;
            self.version = final_state.version;
            self.generation = final_state.generation;
            return;
        }
        if self.phase != Phase::Rest && self.generation != final_state.generation {
            // This session missed the REST between the previous action, whose
            // last phase it still shows, and the current one.
            self.phase = Phase::Rest;
        }
        self.generation = final_state.generation;
        let mut previous = SystemState {
            action: final_state.action,
            phase: self.phase,
            version: self.version,
            generation: final_state.generation,
        };
        loop {
            let current = if previous == final_state {
                final_state
            } else {
                previous.next()
            };
            let entering = self.phase != current.phase || self.version != current.version;
            let phase_id = current.phase as u8;
            let slot = self.slot.as_ref().expect("live session");
            let finished = store.epoch.has_thread_finished_phase(slot, phase_id);
            let finish = |store: &FasterKv<K, V>| {
                if store.epoch.finish_thread_phase(slot, phase_id) {
                    store.global_move_to_next_state(current);
                }
            };
            match current.phase {
                Phase::PrepIndexCheckpoint | Phase::InProgress if !finished => {
                    if current.phase == Phase::InProgress && entering {
                        // The v-era requests stay tagged with version v.
                        self.checkpoint_serial = self.serial;
                    }
                    finish(store);
                }
                Phase::IndexCheckpoint if store.index_checkpoint_complete() => {
                    if current.action == Action::CheckpointIndex {
                        if !finished {
                            finish(store);
                        }
                    } else {
                        store.global_move_to_next_state(current);
                    }
                }
                Phase::Prepare if !finished => {
                    if entering {
                        Self::mark_all_pending(store, &mut self.pending);
                        store.register_session(self.guid);
                    }
                    finish(store);
                }
                Phase::WaitPending if !finished => {
                    if !self.pending.values().any(|e| e.version < current.version) {
                        finish(store);
                    }
                }
                Phase::WaitFlush if !finished && store.log_flushed() => {
                    store.persist_session(SessionRecord {
                        guid: self.guid,
                        serial: self.checkpoint_serial,
                        version: current.version - 1,
                    });
                    finish(store);
                }
                Phase::GcIoPending if !finished => {
                    if entering {
                        self.gc_barrier = self.next_id;
                    }
                    // No I/O issued before the shift may still be in flight.
                    if !self.pending.keys().any(|&id| id < self.gc_barrier) {
                        finish(store);
                    }
                }
                Phase::GcInProgress if !finished => {
                    store.gc_collect_all();
                    finish(store);
                }
                Phase::GrowPrepare if !finished => finish(store),
                Phase::GrowInProgress if !finished => {
                    store.grow_split_all();
                    finish(store);
                }
                Phase::PersistenceCallback if !finished => {
                    store.persistence_callback(self.checkpoint_serial);
                    finish(store);
                }
                _ => {}
            }
            self.phase = current.phase;
            self.version = current.version;
            if current == final_state {
                break;
            }
            previous = current;
        }
    }

    /// Takes the old-side lock for every request issued before PREPARE.
    fn mark_all_pending(store: &FasterKv<K, V>, pending: &mut HashMap<u64, Entry<K, V>>) {
        let locks = store.locks().expect("checkpoint locks exist in PREPARE");
        for entry in pending.values_mut().filter(|e| !e.marked) {
            assert!(
                locks.try_lock_old(entry.op.hash()),
                "no new-side locks exist before IN_PROGRESS"
            );
            entry.marked = true;
        }
    }

    /// Completes pending operations on this thread. With `wait`, blocks until
    /// none remain and the session is back in REST. Returns true in that case.
    pub fn complete_pending(&mut self, wait: bool) -> bool {
        loop {
            if self.growing() {
                // Completions may touch the index; wait for growth to end.
                self.heavy_enter();
            }
            let batch = std::mem::take(&mut *self.responses.lock().unwrap());
            for (id, result) in batch {
                let disk = result.ok().map(|bytes| DiskRecord::new(&bytes));
                let mode = self.entry_mode(&self.pending[&id]);
                let slot = self.slot.as_ref().expect("live session");
                let entry = self.pending.get_mut(&id).expect("pending entry");
                let step = entry.op.resume(self.store, slot, mode, disk);
                self.advance(id, step);
            }
            self.refresh();
            if self.growing() {
                continue;
            }
            for id in std::mem::take(&mut self.retry) {
                let mode = self.entry_mode(&self.pending[&id]);
                let slot = self.slot.as_ref().expect("live session");
                let entry = self.pending.get_mut(&id).expect("pending entry");
                let step = entry.op.retry(self.store, slot, mode);
                self.advance(id, step);
            }
            if self.pending.is_empty() && self.phase == Phase::Rest {
                return true;
            }
            if !wait {
                return false;
            }
            std::thread::yield_now();
        }
    }
}

impl<K: Key, V: Value> Drop for Session<'_, K, V> {
    fn drop(&mut self) {
        // Like StopSession: finish outstanding work and any checkpoint first.
        self.complete_pending(true);
        if let Some(slot) = self.slot.take() {
            self.store.epoch.release(slot);
        }
    }
}
