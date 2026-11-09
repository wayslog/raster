//! Cold data operations backed by the disk-backed index and a separate hybrid log.
//!
//! The index and data sessions protect separate epochs. Each parks its epoch
//! slot between calls so a page allocation cannot pin the other log. A
//! deferred operation retains its request, serial and logical addresses.

use super::address::Address;
use super::checkpoint::write_durable;
use super::cold_index::{
    ColdIndex, ColdIndexEntry, ColdIndexOutcome, ColdIndexResult, ColdIndexSession, ColdIndexUpdate,
};
use super::guid::Guid;
use super::hlog::LogConfig;
use super::recovery::RecoveryInfo;
use super::{
    DeleteContext, DeleteOptions, FasterKv, Key, KeyContext, ReadContext, Session, Status,
    StoreConfig, UpsertContext, Value, ValueSlot,
};
use std::cell::{Cell, RefCell};
use std::io;
use std::ops::ControlFlow;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub struct ColdStore<K: Key, V: Value> {
    index: ColdIndex,
    data: FasterKv<K, V>,
    ready: bool,
}

impl<K: Key, V: Value> ColdStore<K, V> {
    /// Opens separate cold data and index logs under the given root.
    pub fn open(
        root: &Path,
        index_buckets: u64,
        index_log: LogConfig,
        data_log: LogConfig,
    ) -> io::Result<Self> {
        Ok(Self {
            index: ColdIndex::open(&root.join("index"), index_buckets, index_log)?,
            data: FasterKv::open(StoreConfig {
                // The cold index owns all record-chain heads.
                index_buckets: 1,
                filepath: Some(root.join("data").join("log")),
                checkpoint_dir: Some(root.join("data").join("checkpoints")),
                log: data_log,
                ..StoreConfig::default()
            })?,
            ready: true,
        })
    }

    pub fn try_start_session(&self) -> Option<ColdStoreSession<'_, K, V>> {
        if !self.ready {
            return None;
        }
        let index = self.index.try_start_session()?;
        index.park_epoch();
        let data = self.data.try_start_session()?;
        data.park_epoch();
        Some(ColdStoreSession {
            index,
            data,
            ready: Rc::default(),
        })
    }

    /// Checkpoints the cold index and data log at a drained session boundary.
    /// The data token identifies the pair after both components are durable.
    pub fn checkpoint(&mut self) -> Result<Guid, Status> {
        let mut session = self.try_start_session().ok_or(Status::Aborted)?;
        let index_status = Arc::new(Mutex::new(Status::Pending));
        let completion = index_status.clone();
        let index_token = self
            .index
            .checkpoint(
                None,
                Some(Arc::new(move |status, _| {
                    *completion.lock().unwrap() = status;
                })),
            )
            .ok_or(Status::IoError)?;
        session.complete_pending(true);
        let status = *index_status.lock().unwrap();
        if status != Status::Ok {
            return Err(status);
        }
        let data_status = Arc::new(Mutex::new(Status::Pending));
        let completion = data_status.clone();
        let data_token = self
            .data
            .checkpoint(
                None,
                Some(Arc::new(move |status, _| {
                    *completion.lock().unwrap() = status;
                })),
            )
            .ok_or(Status::IoError)?;
        session.complete_pending(true);
        let status = *data_status.lock().unwrap();
        if status != Status::Ok {
            return Err(status);
        }
        let dirs = self
            .data
            .dirs
            .as_ref()
            .expect("cold data checkpoint directories");
        write_durable(
            &dirs.index(data_token).join("cold-index-token"),
            &index_token.0.to_le_bytes(),
        )
        .map_err(|_| Status::IoError)?;
        Ok(data_token)
    }

    /// Restores the cold index before replaying the data log. A partial
    /// recovery requires a fresh store before accepting more sessions.
    pub fn recover(&mut self, token: Guid) -> Result<RecoveryInfo, Status> {
        if !self.ready || !self.data.hlog.recovery_ready() {
            return Err(Status::Aborted);
        }
        let dirs = self
            .data
            .dirs
            .as_ref()
            .expect("cold data checkpoint directories");
        let bytes = std::fs::read(dirs.index(token).join("cold-index-token"))
            .map_err(|_| Status::IoError)?;
        let index_token = Guid(u128::from_le_bytes(
            bytes.try_into().map_err(|_| Status::Corruption)?,
        ));
        self.ready = false;
        self.index.recover(index_token)?;
        let mut index = self.index.try_start_session().ok_or(Status::Aborted)?;
        let mut serial = 0;
        let result = self.data.recover_with_index(
            token,
            token,
            Some(&mut |hash, address| {
                serial += 1;
                let completed = Rc::new(Cell::new(None));
                let observed = completed.clone();
                let update =
                    index.update(hash, ColdIndexUpdate::Set(address), serial, move |result| {
                        observed.set(Some(result));
                    });
                let update = if update == Err(Status::Pending) {
                    index.complete_pending(true);
                    completed.get().ok_or(Status::Corruption)?
                } else {
                    update
                };
                match update? {
                    ColdIndexOutcome::Entry(_) => Ok(()),
                    ColdIndexOutcome::Conflict(_) => Err(Status::Corruption),
                }
            }),
        );
        self.ready = result.is_ok();
        result
    }
}

#[derive(Clone, Copy)]
enum Mutation {
    Upsert,
    Delete { force: bool },
    Conditional(ColdIndexEntry),
}

#[derive(Clone)]
struct Tombstone<C>(C);

// SAFETY: The wrapped context owns the key and supplies all shallow-key operations.
unsafe impl<K: Key, C: DeleteContext<K>> KeyContext<K> for Tombstone<C> {
    fn key(&self) -> &K {
        self.0.key()
    }
    fn key_hash(&self) -> u64 {
        self.0.key_hash()
    }
    fn key_size(&self) -> u32 {
        self.0.key_size()
    }
    fn key_equals(&self, key: &K) -> bool {
        self.0.key_equals(key)
    }
    unsafe fn write_key(&self, dst: *mut K) {
        // SAFETY: The caller supplies the unpublished key allocation.
        unsafe { self.0.write_key(dst) };
    }
}

// SAFETY: The value initializes its own empty representation at the size
// selected by the delete context. Cold writes always append.
unsafe impl<K: Key, V: Value, C: DeleteContext<K>> UpsertContext<K, V> for Tombstone<C> {
    fn value_size(&self) -> u32 {
        self.0.value_size().unwrap_or(size_of::<V>() as u32)
    }
    fn put(&mut self, slot: ValueSlot<'_, V>) {
        V::initialize_empty(slot);
    }
    fn put_atomic(&mut self, _: &V) -> bool {
        false
    }
    fn on_complete(&mut self, status: Status) {
        self.0.on_complete(status);
    }
}

#[derive(Clone)]
struct Presence<C, F> {
    ctx: C,
    complete: F,
}

// SAFETY: All key operations use the owned, cloneable request.
unsafe impl<K: Key, C: KeyContext<K>, F> KeyContext<K> for Presence<C, F> {
    fn key(&self) -> &K {
        self.ctx.key()
    }
    fn key_hash(&self) -> u64 {
        self.ctx.key_hash()
    }
    fn key_size(&self) -> u32 {
        self.ctx.key_size()
    }
    fn key_equals(&self, key: &K) -> bool {
        self.ctx.key_equals(key)
    }
    unsafe fn write_key(&self, dst: *mut K) {
        // SAFETY: The caller supplies the unpublished key allocation.
        unsafe { self.ctx.write_key(dst) };
    }
}

impl<K: Key, V: Value, C: KeyContext<K>, F: FnMut(&mut C, Status)> ReadContext<K, V>
    for Presence<C, F>
{
    fn get(&mut self, _: &V) {}
    fn on_complete(&mut self, status: Status) {
        (self.complete)(&mut self.ctx, status);
    }
}

type Continuation<K, V> = Box<dyn FnOnce(&mut ColdStoreSession<'_, K, V>)>;

pub struct ColdStoreSession<'a, K: Key, V: Value> {
    index: ColdIndexSession<'a>,
    data: Session<'a, K, V>,
    ready: Rc<RefCell<Vec<Continuation<K, V>>>>,
}

impl<K: Key, V: Value> ColdStoreSession<'_, K, V> {
    /// Looks up the chain head, then reads the matching data record.
    pub fn read<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: ReadContext<K, V> + Clone + 'static,
    {
        let ready = self.ready.clone();
        let mut pending = ctx.clone();
        let result = self.with_index(|index| {
            index.lookup(ctx.key_hash(), serial, move |result| {
                ready.borrow_mut().push(Box::new(move |session| {
                    let status = Self::read_index_result(&mut session.data, &mut pending, result);
                    if status != Status::Pending {
                        pending.on_complete(status);
                    }
                }));
            })
        });
        Self::read_index_result(&mut self.data, ctx, result)
    }

    fn read_index_result<C>(
        data: &mut Session<'_, K, V>,
        ctx: &mut C,
        result: ColdIndexResult,
    ) -> Status
    where
        C: ReadContext<K, V> + Clone + 'static,
    {
        match result {
            Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Published(entry))) => {
                let _guard = data.epoch_scope();
                data.read_cold(ctx, entry)
            }
            Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Empty | ColdIndexEntry::Reserved)) => {
                Status::NotFound
            }
            Ok(ColdIndexOutcome::Conflict(_)) => unreachable!("lookup does not update the index"),
            Err(status) => status,
        }
    }

    /// Appends a record and conditionally publishes its address in the cold index.
    pub fn upsert<C>(&mut self, ctx: &mut C, serial: u64) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        self.mutate(ctx, serial, Mutation::Upsert)
    }

    pub fn delete<C>(&mut self, ctx: &C, serial: u64, options: DeleteOptions) -> Status
    where
        C: DeleteContext<K> + Clone + 'static,
    {
        self.mutate(
            &mut Tombstone(ctx.clone()),
            serial,
            Mutation::Delete {
                force: options.force_tombstone,
            },
        )
    }

    /// Observes the slot used by conditional_insert. The returned head may
    /// belong to a different key that shares the slot.
    pub fn lookup_head<F>(&mut self, hash: u64, serial: u64, complete: F) -> ColdIndexResult
    where
        F: FnOnce(ColdIndexResult) + Clone + 'static,
    {
        self.with_index(|index| index.lookup(hash, serial, complete))
    }

    /// Publishes a new record only if the slot still holds the caller's
    /// snapshot. A competing update returns Aborted.
    pub fn conditional_insert<C>(
        &mut self,
        ctx: &mut C,
        expected: ColdIndexEntry,
        serial: u64,
    ) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        self.mutate(ctx, serial, Mutation::Conditional(expected))
    }

    fn mutate<C>(&mut self, ctx: &mut C, serial: u64, mutation: Mutation) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        let ready = self.ready.clone();
        let mut pending = ctx.clone();
        let result = self.with_index(|index| {
            index.lookup(ctx.key_hash(), serial, move |result| {
                ready.borrow_mut().push(Box::new(move |session| {
                    let status = session.apply_mutation(&mut pending, mutation, serial, result);
                    if status != Status::Pending {
                        pending.on_complete(status);
                    }
                }));
            })
        });
        self.apply_mutation(ctx, mutation, serial, result)
    }

    fn apply_mutation<C>(
        &mut self,
        ctx: &mut C,
        mutation: Mutation,
        serial: u64,
        mut result: ColdIndexResult,
    ) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        loop {
            let expected = match result {
                Ok(ColdIndexOutcome::Entry(entry) | ColdIndexOutcome::Conflict(entry)) => entry,
                Err(status) => return status,
            };
            if matches!(mutation, Mutation::Conditional(snapshot) if expected != snapshot) {
                return Status::Aborted;
            }
            if let Mutation::Delete { force: false } = mutation {
                let status = self.check_delete_target(ctx, expected, serial);
                if status != Status::Ok {
                    return status;
                }
            }
            result = match self.publish_record(ctx, mutation, expected, serial) {
                ControlFlow::Continue(observed) => Ok(ColdIndexOutcome::Conflict(observed)),
                ControlFlow::Break(status) => return status,
            };
        }
    }

    fn check_delete_target<C>(&mut self, ctx: &C, expected: ColdIndexEntry, serial: u64) -> Status
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        let ColdIndexEntry::Published(address) = expected else {
            return Status::NotFound;
        };
        let ready = self.ready.clone();
        let mut probe = Presence {
            ctx: ctx.clone(),
            complete: move |ctx: &mut C, status| {
                let mut pending = ctx.clone();
                ready.borrow_mut().push(Box::new(move |session| {
                    let status = if status == Status::Ok {
                        let mutation = Mutation::Delete { force: false };
                        match session.publish_record(&mut pending, mutation, expected, serial) {
                            ControlFlow::Continue(entry) => session.apply_mutation(
                                &mut pending,
                                mutation,
                                serial,
                                Ok(ColdIndexOutcome::Conflict(entry)),
                            ),
                            ControlFlow::Break(status) => status,
                        }
                    } else {
                        status
                    };
                    if status != Status::Pending {
                        pending.on_complete(status);
                    }
                }));
            },
        };
        self.with_data(|data| data.read_cold(&mut probe, address))
    }

    fn publish_record<C>(
        &mut self,
        ctx: &mut C,
        mutation: Mutation,
        expected: ColdIndexEntry,
        serial: u64,
    ) -> ControlFlow<Status, ColdIndexEntry>
    where
        C: UpsertContext<K, V> + Clone + 'static,
    {
        let previous = match expected {
            ColdIndexEntry::Published(address) => address,
            ColdIndexEntry::Empty | ColdIndexEntry::Reserved => Address::INVALID,
        };
        let tombstone = matches!(mutation, Mutation::Delete { .. });
        let address = self.with_data(|data| data.append_cold_record(ctx, previous, tombstone));
        let ready = self.ready.clone();
        let mut pending = ctx.clone();
        let result = self.with_index(|index| {
            index.update(
                ctx.key_hash(),
                ColdIndexUpdate::CompareExchange { expected, address },
                serial,
                move |result| {
                    ready.borrow_mut().push(Box::new(move |session| {
                        let status = match session.finish_publication(address, result) {
                            ControlFlow::Continue(observed) => session.apply_mutation(
                                &mut pending,
                                mutation,
                                serial,
                                Ok(ColdIndexOutcome::Conflict(observed)),
                            ),
                            ControlFlow::Break(status) => status,
                        };
                        if status != Status::Pending {
                            pending.on_complete(status);
                        }
                    }));
                },
            )
        });
        self.finish_publication(address, result)
    }

    fn finish_publication(
        &mut self,
        address: Address,
        result: ColdIndexResult,
    ) -> ControlFlow<Status, ColdIndexEntry> {
        match result {
            Ok(ColdIndexOutcome::Entry(_)) => ControlFlow::Break(Status::Ok),
            Err(Status::Pending) => ControlFlow::Break(Status::Pending),
            Ok(ColdIndexOutcome::Conflict(entry)) => {
                self.with_data(|data| data.invalidate_cold_append(address));
                ControlFlow::Continue(entry)
            }
            Err(status) => {
                self.with_data(|data| data.invalidate_cold_append(address));
                ControlFlow::Break(status)
            }
        }
    }

    // Each nested session is parked while the other may wait for a page.
    // Pending operations retain owned state and log addresses between calls.
    fn with_index<T>(&mut self, operation: impl FnOnce(&mut ColdIndexSession<'_>) -> T) -> T {
        let _guard = self.index.epoch_scope();
        operation(&mut self.index)
    }

    fn with_data<T>(&mut self, operation: impl FnOnce(&mut Session<'_, K, V>) -> T) -> T {
        let _guard = self.data.epoch_scope();
        operation(&mut self.data)
    }

    pub fn refresh(&mut self) {
        self.with_index(|index| index.refresh());
        self.with_data(|data| data.refresh());
    }

    pub fn complete_pending(&mut self, wait: bool) -> bool {
        loop {
            let index_done = self.with_index(|index| index.complete_pending(false));
            let ready = std::mem::take(&mut *self.ready.borrow_mut());
            for resume in ready {
                resume(self);
            }
            let data_done = self.with_data(|data| data.complete_pending(false));
            if index_done
                && self.index.pending_count() == 0
                && data_done
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

impl<K: Key, V: Value> Drop for ColdStoreSession<'_, K, V> {
    fn drop(&mut self) {
        self.complete_pending(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Request {
        key: u64,
        value: u64,
        completion: Rc<Cell<Option<(Status, u64)>>>,
    }

    // SAFETY: The request owns a complete fixed-size key.
    unsafe impl KeyContext<u64> for Request {
        fn key(&self) -> &u64 {
            &self.key
        }
    }

    impl ReadContext<u64, u64> for Request {
        fn get(&mut self, value: &u64) {
            self.value = *value;
        }

        fn on_complete(&mut self, status: Status) {
            assert!(
                self.completion
                    .replace(Some((status, self.value)))
                    .is_none()
            );
        }
    }

    // SAFETY: Each put fully initializes a fixed-size value and declines
    // in-place mutation of shared data.
    unsafe impl UpsertContext<u64, u64> for Request {
        fn put(&mut self, slot: ValueSlot<'_, u64>) {
            slot.write(self.value);
        }

        fn put_atomic(&mut self, _: &u64) -> bool {
            false
        }

        fn on_complete(&mut self, status: Status) {
            ReadContext::on_complete(self, status);
        }
    }

    // SAFETY: The request keeps its complete key through deferred deletion.
    unsafe impl DeleteContext<u64> for Request {
        fn on_complete(&mut self, status: Status) {
            ReadContext::on_complete(self, status);
        }
    }

    const SMALL_LOG: LogConfig = LogConfig {
        page_bits: 12,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };

    fn request(key: u64, value: u64) -> Request {
        Request {
            key,
            value,
            completion: Rc::new(Cell::new(None)),
        }
    }

    fn finish(
        session: &mut ColdStoreSession<'_, u64, u64>,
        op: &Request,
        status: Status,
    ) -> Status {
        if status == Status::Pending {
            assert!(session.complete_pending(true));
            op.completion.take().expect("deferred result").0
        } else {
            status
        }
    }

    #[test]
    fn evicted_index_and_data_complete_one_read() {
        let root = std::env::temp_dir().join(format!("raster-cold-data-{}", Guid::new()));
        let config = LogConfig {
            page_bits: 12,
            memory_pages: 4,
            mutable_fraction: 0.5,
        };
        let store = ColdStore::<u64, u64>::open(&root, 256, config, config).unwrap();
        let observed = Rc::new(Cell::new(None));
        let mut request = Request {
            key: 0,
            value: 97,
            completion: observed.clone(),
        };
        let address = {
            let mut data = store.data.start_session();
            assert_eq!(data.upsert(&mut request, 1), Status::Ok);
            let address = store.data.index.find(0u64.hash()).unwrap().entry.address();
            for key in 1..=1024 {
                request.key = key;
                assert_eq!(data.upsert(&mut request, key + 1), Status::Ok);
            }
            assert!(address < store.data.hlog.head.load());
            address
        };

        let hash = 0u64.hash();
        {
            let mut index = store.index.try_start_session().unwrap();
            assert_eq!(
                index.update(hash, ColdIndexUpdate::Set(address), 1, |_| {}),
                Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Published(address)))
            );
            let filler_tag = (hash ^ (1 << 48)) & (7 << 48);
            for bucket in 0..128 {
                let filler = Address::new(0, 64);
                assert_eq!(
                    index.update(
                        filler_tag | bucket,
                        ColdIndexUpdate::Set(filler),
                        bucket + 2,
                        |_| {}
                    ),
                    Ok(ColdIndexOutcome::Entry(ColdIndexEntry::Published(filler)))
                );
            }
            assert_eq!(index.lookup(hash, 130, |_| {}), Err(Status::Pending));
        }

        request.key = 0;
        request.value = 0;
        {
            let mut session = store.try_start_session().unwrap();
            assert_eq!(session.read(&mut request, 1), Status::Pending);
            assert!(observed.get().is_none());
            assert!(session.complete_pending(true));
            assert_eq!(observed.get(), Some((Status::Ok, 97)));
            observed.set(None);
            request.value = 101;
            assert_eq!(session.upsert(&mut request, 2), Status::Pending);
            assert!(session.complete_pending(true));
            assert_eq!(observed.get(), Some((Status::Ok, 101)));
            observed.set(None);
            assert_eq!(session.read(&mut request, 3), Status::Ok);
            assert_eq!(request.value, 101);
            assert!(observed.get().is_none());
        }
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cold_delete_checks_an_evicted_key_and_preserves_collisions() {
        let root = std::env::temp_dir().join(format!("raster-cold-delete-{}", Guid::new()));
        let store = ColdStore::<u64, u64>::open(&root, 1, SMALL_LOG, SMALL_LOG).unwrap();
        let mut session = store.try_start_session().unwrap();
        let mut target = request(0, 97);
        let hash_slot = (0u64.hash() >> 48) & 255;
        let collision = (1..)
            .find(|key: &u64| (key.hash() >> 48) & 255 == hash_slot)
            .unwrap();
        let mut other = request(collision, 88);
        assert_eq!(session.upsert(&mut target, 1), Status::Ok);
        for serial in 2..=1024 {
            let status = session.upsert(&mut other, serial);
            assert_eq!(finish(&mut session, &other, status), Status::Ok);
        }
        assert!(store.data.hlog.head.load() > Address::new(0, 64));
        let status = session.delete(&target, 1025, DeleteOptions::default());
        assert_eq!(status, Status::Pending);
        assert_eq!(finish(&mut session, &target, status), Status::Ok);
        assert_eq!(session.read(&mut target, 1026), Status::NotFound);
        assert_eq!(session.read(&mut other, 1027), Status::Ok);
        assert_eq!(other.value, 88);
        assert_eq!(
            session.delete(&target, 1028, DeleteOptions::default()),
            Status::NotFound
        );
        drop(session);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn conditional_inserts_publish_only_one_winner() {
        let root = std::env::temp_dir().join(format!("raster-cold-insert-{}", Guid::new()));
        let store = ColdStore::<u64, u64>::open(&root, 1, SMALL_LOG, SMALL_LOG).unwrap();
        let ready = std::sync::Barrier::new(2);
        let results = std::thread::scope(|scope| {
            let handles = [10, 20].map(|value| {
                let (store, ready) = (&store, &ready);
                scope.spawn(move || {
                    let mut session = store.try_start_session().unwrap();
                    let mut op = request(0, value);
                    ready.wait();
                    let status = session.conditional_insert(&mut op, ColdIndexEntry::Empty, 1);
                    (finish(&mut session, &op, status), value)
                })
            });
            handles.map(|handle| handle.join().unwrap())
        });
        let [(status_a, value_a), (status_b, value_b)] = results;
        let expected = match (status_a, status_b) {
            (Status::Ok, Status::Aborted) => value_a,
            (Status::Aborted, Status::Ok) => value_b,
            _ => panic!("expected one winner, got {results:?}"),
        };
        let mut session = store.try_start_session().unwrap();
        let mut op = request(0, 0);
        assert_eq!(session.read(&mut op, 1), Status::Ok);
        assert_eq!(op.value, expected);
        drop(session);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cold_checkpoint_reopens_and_restores_tombstones() {
        let root = std::env::temp_dir().join(format!("raster-cold-recover-{}", Guid::new()));
        let token = {
            let mut store = ColdStore::<u64, u64>::open(&root, 64, SMALL_LOG, SMALL_LOG).unwrap();
            let mut session = store.try_start_session().unwrap();
            let mut deleted = request(1, 55);
            let mut kept = request(2, 66);
            assert_eq!(session.upsert(&mut deleted, 1), Status::Ok);
            assert_eq!(session.upsert(&mut kept, 2), Status::Ok);
            assert_eq!(
                session.delete(&deleted, 3, DeleteOptions::default()),
                Status::Ok
            );
            drop(session);
            store.checkpoint().unwrap()
        };
        let mut store = ColdStore::<u64, u64>::open(&root, 64, SMALL_LOG, SMALL_LOG).unwrap();
        store.recover(token).unwrap();
        let mut session = store.try_start_session().unwrap();
        let mut deleted = request(1, 0);
        let mut kept = request(2, 0);
        assert_eq!(session.read(&mut deleted, 1), Status::NotFound);
        assert_eq!(session.read(&mut kept, 2), Status::Ok);
        assert_eq!(kept.value, 66);
        deleted.value = 77;
        assert_eq!(session.upsert(&mut deleted, 3), Status::Ok);
        assert_eq!(session.read(&mut deleted, 4), Status::Ok);
        assert_eq!(deleted.value, 77);
        drop(session);
        assert!(store.checkpoint().is_ok());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn replay_waits_for_a_disk_backed_index_update() {
        use crate::core::checkpoint::{IndexMetadata, LogMetadata};

        let root = std::env::temp_dir().join(format!("raster-cold-replay-{}", Guid::new()));
        let (token, index_token) = {
            let mut store = ColdStore::<u64, u64>::open(&root, 256, SMALL_LOG, SMALL_LOG).unwrap();
            let mut session = store.try_start_session().unwrap();
            for key in 0..128 {
                let mut op = request(key, key + 1);
                let status = session.upsert(&mut op, key + 1);
                assert_eq!(finish(&mut session, &op, status), Status::Ok);
            }
            drop(session);
            let baseline = store.checkpoint().unwrap();
            let dirs = store.data.dirs.as_ref().unwrap();
            let index_bytes = std::fs::read(dirs.index(baseline).join("cold-index-token")).unwrap();
            let cutoff = LogMetadata::read(&dirs.log(baseline))
                .unwrap()
                .final_address;

            let mut session = store.try_start_session().unwrap();
            let mut op = request(0, 500);
            let status = session.upsert(&mut op, 1);
            assert_eq!(finish(&mut session, &op, status), Status::Ok);
            drop(session);
            let token = store.checkpoint().unwrap();

            // Pair an earlier index cut with a later data-log checkpoint.
            let index_dir = store.data.dirs.as_ref().unwrap().index(token);
            let mut metadata = IndexMetadata::read(&index_dir).unwrap();
            metadata.checkpoint_start_address = cutoff;
            metadata.write(&index_dir).unwrap();
            write_durable(&index_dir.join("cold-index-token"), &index_bytes).unwrap();
            (
                token,
                Guid(u128::from_le_bytes(index_bytes.try_into().unwrap())),
            )
        };

        {
            let mut index = ColdIndex::open(&root.join("index"), 256, SMALL_LOG).unwrap();
            index.recover(index_token).unwrap();
            let mut session = index.try_start_session().unwrap();
            assert_eq!(session.lookup(0u64.hash(), 1, |_| {}), Err(Status::Pending));
        }
        {
            let mut store = ColdStore::<u64, u64>::open(&root, 256, SMALL_LOG, SMALL_LOG).unwrap();
            let result =
                store
                    .data
                    .recover_with_index(token, token, Some(&mut |_, _| Err(Status::IoError)));
            assert!(matches!(result, Err(Status::IoError)));
            assert!(store.try_start_session().is_none());
        }
        let mut store = ColdStore::<u64, u64>::open(&root, 256, SMALL_LOG, SMALL_LOG).unwrap();
        store.recover(token).unwrap();
        let mut session = store.try_start_session().unwrap();
        let mut op = request(0, 0);
        assert_eq!(session.read(&mut op, 1), Status::Ok);
        assert_eq!(op.value, 500);
        drop(session);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn short_data_reads_and_io_errors_complete_once() {
        use crate::core::device::{Device, ReadCallback, WriteCallback};

        struct FaultyRead(bool);
        impl Device for FaultyRead {
            unsafe fn write_async(&self, _: u64, _: *const u8, _: usize, done: WriteCallback) {
                done(Ok(()));
            }

            fn read_async(&self, _: u64, len: usize, done: ReadCallback) {
                if self.0 {
                    done(Ok(vec![0; len - 1]));
                } else {
                    done(Err(io::Error::other("data device read failed")));
                }
            }
        }

        for short_read in [false, true] {
            let root = std::env::temp_dir().join(format!("raster-cold-read-error-{}", Guid::new()));
            let store = ColdStore::<u64, u64> {
                index: ColdIndex::open(&root, 256, SMALL_LOG).unwrap(),
                data: FasterKv::with_device(
                    StoreConfig {
                        index_buckets: 1,
                        log: SMALL_LOG,
                        ..StoreConfig::default()
                    },
                    Arc::new(FaultyRead(short_read)),
                ),
                ready: true,
            };
            let mut session = store.try_start_session().unwrap();
            let mut op = request(0, 97);
            assert_eq!(session.upsert(&mut op, 1), Status::Ok);
            let mut filler = request(1, 99);
            for serial in 2..=1024 {
                let status = session.upsert(&mut filler, serial);
                assert_eq!(finish(&mut session, &filler, status), Status::Ok);
            }
            let status = session.read(&mut op, 1025);
            assert_eq!(status, Status::Pending);
            assert_eq!(finish(&mut session, &op, status), Status::IoError);
            assert!(session.complete_pending(true));
            assert!(op.completion.get().is_none());
            drop(session);
            drop(store);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn concurrent_sessions_page_both_logs_without_pinning_the_other_epoch() {
        let root = std::env::temp_dir().join(format!("raster-cold-concurrent-{}", Guid::new()));
        let store = ColdStore::<u64, u64>::open(&root, 256, SMALL_LOG, SMALL_LOG).unwrap();
        let ready = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            for thread in 0..2 {
                let (store, ready) = (&store, &ready);
                scope.spawn(move || {
                    let mut session = store.try_start_session().unwrap();
                    ready.wait();
                    for serial in 1..=4096 {
                        let key = serial * 2 + thread;
                        let mut op = request(key, key);
                        let status = session.upsert(&mut op, serial);
                        assert_eq!(finish(&mut session, &op, status), Status::Ok);
                    }
                });
            }
        });
        let mut session = store.try_start_session().unwrap();
        for (serial, key) in [2, 3, 8192, 8193].into_iter().enumerate() {
            let mut op = request(key, 0);
            match session.read(&mut op, serial as u64 + 1) {
                Status::Ok => assert_eq!(op.value, key),
                Status::Pending => {
                    assert!(session.complete_pending(true));
                    assert_eq!(op.completion.get(), Some((Status::Ok, key)));
                }
                status => panic!("read failed: {status:?}"),
            }
        }
        drop(session);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
