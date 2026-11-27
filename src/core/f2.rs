//! Session routing across the hot store and cold data log.

use super::address::Address;
use super::cold_store::{ColdStore, ColdStoreSession};
use super::guid::Guid;
use super::{
    DeleteContext, DeleteOptions, FasterKv, Key, KeyContext, ReadContext, RmwContext, Session,
    Status, UpsertContext, Value,
};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::{Mutex, RwLock};

#[path = "f2_checkpoint.rs"]
mod checkpoint;
#[path = "f2_compact.rs"]
mod compaction;

pub use checkpoint::F2Config;

pub struct F2Kv<K: Key, V: Value> {
    hot: FasterKv<K, V>,
    cold: ColdStore<K, V>,
    maintenance: Mutex<()>,
    hot_cut: RwLock<()>,
    persistent: Option<checkpoint::Persistence>,
    ready: bool,
}

impl<K: Key, V: Value> F2Kv<K, V> {
    /// Combines a hot store with a cold data store.
    pub fn new(hot: FasterKv<K, V>, cold: ColdStore<K, V>) -> Self {
        Self {
            hot,
            cold,
            maintenance: Mutex::new(()),
            hot_cut: RwLock::new(()),
            persistent: None,
            ready: true,
        }
    }

    pub fn try_start_session(&self) -> Option<F2Session<'_, K, V>> {
        self.start_session(Guid::new(), 0, true)
    }

    fn start_session(&self, guid: Guid, serial: u64, user: bool) -> Option<F2Session<'_, K, V>> {
        if !self.ready {
            return None;
        }
        let hot = Session::try_new(&self.hot, guid, serial)?;
        hot.park_epoch();
        let cold = self.cold.try_start_session_with_guid(guid)?;
        Some(F2Session {
            hot,
            cold,
            hot_cut: &self.hot_cut,
            history: self
                .persistent
                .as_ref()
                .filter(|_| user)
                .map(|p| &p.sessions),
            ready: Rc::default(),
        })
    }

    /// Resumes a user session from the recovered checkpoint prefix.
    pub fn continue_session(&self, guid: Guid) -> Option<(F2Session<'_, K, V>, u64)> {
        let mut history = self.persistent.as_ref()?.sessions.lock().unwrap();
        let serial = *history.resumable.get(&guid)?;
        let session = self.start_session(guid, serial, true)?;
        history.resumable.remove(&guid);
        Some((session, serial))
    }
}

type Continuation<K, V> = Box<dyn FnOnce(&mut F2Session<'_, K, V>)>;

pub struct F2Session<'a, K: Key, V: Value> {
    hot: Session<'a, K, V>,
    cold: ColdStoreSession<'a, K, V>,
    hot_cut: &'a RwLock<()>,
    history: Option<&'a Mutex<checkpoint::SessionHistory>>,
    ready: Rc<RefCell<Vec<Continuation<K, V>>>>,
}

#[derive(Clone, Copy)]
struct HotSnapshot {
    head: Address,
    begin: Address,
}

#[derive(Clone)]
struct ColdValueRead<C, F> {
    ctx: C,
    words: Vec<MaybeUninit<u64>>,
    complete: F,
}

// SAFETY: The owned context supplies the complete key or all shallow-key
// operations, and its clone retains the key for deferred completion.
unsafe impl<K: Key, C: KeyContext<K>, F> KeyContext<K> for ColdValueRead<C, F> {
    fn key(&self) -> &K {
        self.ctx.key()
    }

    fn key_size(&self) -> u32 {
        self.ctx.key_size()
    }

    fn key_hash(&self) -> u64 {
        self.ctx.key_hash()
    }

    fn key_equals(&self, stored: &K) -> bool {
        self.ctx.key_equals(stored)
    }

    unsafe fn write_key(&self, dst: *mut K) {
        // SAFETY: The caller supplies the unpublished key allocation.
        unsafe { self.ctx.write_key(dst) };
    }
}

impl<K: Key, V: Value, C: KeyContext<K>, F: FnMut(&mut C, Vec<MaybeUninit<u64>>, Status)>
    ReadContext<K, V> for ColdValueRead<C, F>
{
    fn get(&mut self, _: &V) {
        unreachable!("cold reads supply the complete record allocation");
    }

    unsafe fn get_from_log(&mut self, value: *const V, _: bool) {
        // SAFETY: The caller protects the complete initialized value.
        let len = unsafe { (*value).size() } as usize;
        self.words.resize(len.div_ceil(8), MaybeUninit::uninit());
        // SAFETY: The cold store only appends values. The complete immutable
        // allocation is readable during this callback and the buffers are disjoint.
        unsafe {
            std::ptr::copy_nonoverlapping(
                value.cast::<u8>(),
                self.words.as_mut_ptr().cast::<u8>(),
                len,
            );
        }
    }

    fn on_complete(&mut self, status: Status) {
        (self.complete)(&mut self.ctx, std::mem::take(&mut self.words), status);
    }
}

impl<K: Key, V: Value> F2Session<'_, K, V> {
    pub fn guid(&self) -> Guid {
        self.hot.guid()
    }

    pub fn serial(&self) -> u64 {
        self.hot.serial()
    }

    /// A hot tombstone completes the read without consulting the cold store.
    pub fn read<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: ReadContext<K, V> + Clone + 'static,
    {
        let ready = self.ready.clone();
        let status = self.with_hot(|hot| {
            hot.read_configured::<true, _, _>(ctx, serial, move |ctx, status| {
                if status != Status::NotFound {
                    ctx.on_complete(if status == Status::Aborted {
                        Status::NotFound
                    } else {
                        status
                    });
                    return;
                }
                let mut pending = ctx.clone();
                ready.borrow_mut().push(Box::new(move |session| {
                    let status = session.cold.read(&mut pending, serial);
                    if status != Status::Pending {
                        pending.on_complete(status);
                    }
                }));
            })
        });
        self.finish_hot_read(ctx, serial, status)
    }

    fn finish_hot_read<C>(&mut self, ctx: &mut C, serial: u64, status: Status) -> Status
    where
        C: ReadContext<K, V> + Clone + 'static,
    {
        match status {
            Status::NotFound => self.cold.read(ctx, serial),
            Status::Aborted => Status::NotFound,
            status => status,
        }
    }

    pub fn upsert<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        self.with_hot(|hot| hot.upsert(ctx, serial))
    }

    pub fn delete<C>(&mut self, ctx: &C, serial: u64) -> Status
    where
        C: DeleteContext<K> + Clone + 'static,
    {
        self.with_hot(|hot| {
            hot.delete_with_options(
                ctx,
                serial,
                DeleteOptions {
                    force_tombstone: true,
                },
            )
        })
    }

    /// Updates the hot value, or reads the cold value and conditionally
    /// publishes an updated copy in the hot log.
    pub fn rmw<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: RmwContext<K, V> + Clone + 'static,
    {
        loop {
            let ready = self.ready.clone();
            let (expected, status) = self.with_hot(|hot| {
                let expected = HotSnapshot {
                    begin: hot.store().hlog.begin.load(),
                    head: hot.rmw_head(ctx.key_hash()),
                };
                let status =
                    hot.rmw_configured::<false, true, _, _>(ctx, serial, move |ctx, status| {
                        if !matches!(status, Status::NotFound | Status::Aborted) {
                            ctx.on_complete(status);
                            return;
                        }
                        let mut pending = ctx.clone();
                        ready.borrow_mut().push(Box::new(move |session| {
                            let result =
                                session.finish_hot_rmw(&mut pending, serial, expected, status);
                            session.complete_routed_rmw(&mut pending, serial, result);
                        }));
                    });
                (expected, status)
            });
            if let ControlFlow::Break(status) = self.finish_hot_rmw(ctx, serial, expected, status) {
                return status;
            }
        }
    }

    fn finish_hot_rmw<C>(
        &mut self,
        ctx: &mut C,
        serial: u64,
        expected: HotSnapshot,
        status: Status,
    ) -> ControlFlow<Status>
    where
        C: RmwContext<K, V> + Clone + 'static,
    {
        match status {
            Status::Aborted => return self.publish_rmw(ctx, expected, None),
            Status::NotFound => {}
            status => return ControlFlow::Break(status),
        }
        let ready = self.ready.clone();
        let mut read = ColdValueRead {
            ctx: ctx.clone(),
            words: Vec::new(),
            complete: move |ctx: &mut C, words: Vec<MaybeUninit<u64>>, status| {
                let mut pending = ctx.clone();
                ready.borrow_mut().push(Box::new(move |session| {
                    let result = session.finish_cold_rmw(&mut pending, expected, status, &words);
                    session.complete_routed_rmw(&mut pending, serial, result);
                }));
            },
        };
        let status = self.cold.read(&mut read, serial);
        self.finish_cold_rmw(ctx, expected, status, &read.words)
    }

    fn finish_cold_rmw<C: RmwContext<K, V>>(
        &mut self,
        ctx: &mut C,
        expected: HotSnapshot,
        status: Status,
        words: &[MaybeUninit<u64>],
    ) -> ControlFlow<Status> {
        match status {
            Status::Ok => {
                // SAFETY: A successful cold read copied a complete V into
                // 8-byte-aligned storage that lives through the append.
                let value = unsafe { &*words.as_ptr().cast::<V>() };
                self.publish_rmw(ctx, expected, Some(value))
            }
            Status::NotFound => self.publish_rmw(ctx, expected, None),
            status => ControlFlow::Break(status),
        }
    }

    fn publish_rmw<C: RmwContext<K, V>>(
        &mut self,
        ctx: &mut C,
        expected: HotSnapshot,
        source: Option<&V>,
    ) -> ControlFlow<Status> {
        let _cut = self.hot_cut.read().unwrap();
        let status = self.with_hot(|hot| {
            if hot.store().hlog.begin.load() != expected.begin {
                return Status::Aborted;
            }
            hot.insert_rmw_if_head(ctx, expected.head, source)
        });
        match status {
            Status::Aborted => ControlFlow::Continue(()),
            status => ControlFlow::Break(status),
        }
    }

    fn complete_routed_rmw<C>(&mut self, ctx: &mut C, serial: u64, result: ControlFlow<Status>)
    where
        C: RmwContext<K, V> + Clone + 'static,
    {
        let status = match result {
            ControlFlow::Break(status) => status,
            ControlFlow::Continue(()) => {
                let latest_serial = self.hot.serial();
                let status = self.rmw(ctx, serial);
                self.hot.set_serial(latest_serial);
                status
            }
        };
        if status != Status::Pending {
            ctx.on_complete(status);
        }
    }

    fn with_hot<T>(&mut self, operation: impl FnOnce(&mut Session<'_, K, V>) -> T) -> T {
        let _guard = self.hot.epoch_scope();
        operation(&mut self.hot)
    }

    pub fn refresh(&mut self) {
        self.with_hot(|hot| hot.refresh());
        self.cold.refresh();
    }

    pub fn complete_pending(&mut self, wait: bool) -> bool {
        loop {
            let hot_done = self.with_hot(|hot| hot.complete_pending(false));
            let ready = std::mem::take(&mut *self.ready.borrow_mut());
            for resume in ready {
                resume(self);
            }
            let cold_done = self.cold.complete_pending(false);
            if hot_done
                && self.hot.pending_count() == 0
                && cold_done
                && self.ready.borrow().is_empty()
            {
                return true;
            }
            if !wait {
                return false;
            }
            std::thread::yield_now();
        }
    }
}

impl<K: Key, V: Value> Drop for F2Session<'_, K, V> {
    fn drop(&mut self) {
        self.complete_pending(true);
        if let Some(history) = self.history {
            history
                .lock()
                .unwrap()
                .completed
                .insert(self.guid(), self.serial());
        }
    }
}
