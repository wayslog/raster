//! Session execution context (FASTER `ExecutionContext`).
//!
//! A session protects the epoch for its whole life and advances it with
//! `refresh`. Operations touch only session-owned fields; they never enter or
//! leave the epoch. Operations that need a record from the device copy their
//! context, return `Pending`, and finish in `complete_pending`.
use super::address::Address;
use super::epoch::SlotId;
use super::kv::{DeleteContext, Key, ReadContext, RmwContext, UpsertContext, Value};
use super::pending::{DiskRecord, PendingOp, PendingRead, PendingRmw, Step};
use super::record::Layout;
use super::store::{FasterKv, Outcome, Status};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

type Response = (u64, io::Result<Vec<u8>>);

pub struct Session<'a, K: Key, V: Value> {
    store: &'a FasterKv<K, V>,
    slot: Option<SlotId>,
    serial: u64,
    next_id: u64,
    pending: HashMap<u64, Box<dyn PendingOp<K, V>>>,
    responses: Arc<Mutex<Vec<Response>>>,
    /// Pending operations to re-run after the next refresh.
    retry: Vec<u64>,
}

impl<'a, K: Key, V: Value> Session<'a, K, V> {
    pub(crate) fn new(store: &'a FasterKv<K, V>) -> Self {
        let slot = store.epoch.reserve().expect("all epoch slots are in use");
        store.epoch.protect_and_drain(&slot);
        Self {
            store,
            slot: Some(slot),
            serial: 0,
            next_id: 0,
            pending: HashMap::new(),
            responses: Arc::default(),
            retry: Vec::new(),
        }
    }

    #[inline(always)]
    fn slot(&self) -> &SlotId {
        self.slot.as_ref().expect("live session")
    }

    /// Serial number of the last operation issued by this session.
    pub fn serial(&self) -> u64 {
        self.serial
    }

    /// Number of operations still waiting in `complete_pending`.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    #[inline]
    pub fn read<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: ReadContext<K, V> + Clone + 'static,
    {
        self.serial = serial;
        match self.store.internal_read(ctx) {
            Outcome::Done(status) => status,
            Outcome::Disk { address, .. } => {
                self.suspend(address, PendingRead { ctx: ctx.clone() })
            }
            Outcome::RetryLater => unreachable!("reads never retry later"),
        }
    }

    #[inline]
    pub fn upsert<C: UpsertContext<K, V>>(&mut self, ctx: &mut C, serial: u64) -> Status {
        self.serial = serial;
        self.store.internal_upsert(self.slot(), ctx)
    }

    #[inline]
    pub fn rmw<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: RmwContext<K, V> + Clone + 'static,
    {
        self.serial = serial;
        match self.store.internal_rmw(self.slot(), ctx) {
            Outcome::Done(status) => status,
            Outcome::Disk { address, observed } => self.suspend(
                address,
                PendingRmw {
                    ctx: ctx.clone(),
                    observed,
                },
            ),
            Outcome::RetryLater => {
                let id = self.register(PendingRmw {
                    ctx: ctx.clone(),
                    observed: Default::default(),
                });
                self.retry.push(id);
                Status::Pending
            }
        }
    }

    #[inline]
    pub fn delete<C: DeleteContext<K>>(&mut self, ctx: &C, serial: u64) -> Status {
        self.serial = serial;
        self.store.internal_delete(self.slot(), ctx)
    }

    #[cold]
    fn register(&mut self, op: impl PendingOp<K, V> + 'static) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.pending.insert(id, Box::new(op));
        id
    }

    #[cold]
    fn suspend(&mut self, address: Address, op: impl PendingOp<K, V> + 'static) -> Status {
        let id = self.register(op);
        self.issue(id, address);
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

    /// Advances this session's epoch and runs any ready deferred actions.
    #[inline]
    pub fn refresh(&mut self) {
        self.store.epoch.protect_and_drain(self.slot());
    }

    /// Completes pending operations on this thread; with `wait`, blocks until
    /// none remain. Returns true when no operation is pending.
    fn advance(&mut self, id: u64, step: Step) {
        match step {
            Step::Done => {
                self.pending.remove(&id);
            }
            Step::Read(address) => self.issue(id, address),
            Step::RetryLater => self.retry.push(id),
        }
    }

    pub fn complete_pending(&mut self, wait: bool) -> bool {
        loop {
            let batch = std::mem::take(&mut *self.responses.lock().unwrap());
            for (id, result) in batch {
                let disk = result.ok().map(|bytes| DiskRecord::new(&bytes));
                let op = self
                    .pending
                    .get_mut(&id)
                    .expect("response for a pending operation");
                let step = op.resume(self.store, self.slot.as_ref().expect("live session"), disk);
                self.advance(id, step);
            }
            self.refresh();
            for id in std::mem::take(&mut self.retry) {
                let op = self.pending.get_mut(&id).expect("queued retry");
                let step = op.retry(self.store, self.slot.as_ref().expect("live session"));
                self.advance(id, step);
            }
            if self.pending.is_empty() {
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
        // Like StopSession: finish outstanding work before leaving the epoch.
        self.complete_pending(true);
        if let Some(slot) = self.slot.take() {
            self.store.epoch.release(slot);
        }
    }
}
