//! Larger-than-memory behavior, after FASTER `cc/test/paging_test.h`.
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

type Store = FasterKv<u64, AtomicU64>;
const KEYS: u64 = 50_000;

/// Checks the value it observes and counts completions.
#[derive(Clone)]
struct Op {
    key: u64,
    input: u64,
    expect: Option<u64>,
    done: Rc<Cell<u64>>,
}
impl Op {
    fn new(key: u64, input: u64, expect: Option<u64>, done: &Rc<Cell<u64>>) -> Self {
        Self {
            key,
            input,
            expect,
            done: done.clone(),
        }
    }
    fn check(&self, status: Status, value: Option<u64>) {
        match self.expect {
            Some(want) => assert_eq!(
                (status, value),
                (Status::Ok, Some(want)),
                "key {}",
                self.key
            ),
            None => assert_eq!(status, Status::NotFound, "key {}", self.key),
        }
        self.done.set(self.done.get() + 1);
    }
}
impl KeyContext<u64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Op {
    fn get(&mut self, value: &AtomicU64) {
        self.input = value.load(Ordering::Relaxed);
    }
    fn on_complete(&mut self, status: Status) {
        self.check(status, (status == Status::Ok).then_some(self.input));
    }
}
impl UpsertContext<u64, AtomicU64> for Op {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::Relaxed);
        true
    }
}
impl RmwContext<u64, AtomicU64> for Op {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(old.load(Ordering::Relaxed) + self.input));
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        value.fetch_add(self.input, Ordering::Relaxed);
        true
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok, "rmw key {}", self.key);
        self.done.set(self.done.get() + 1);
    }
}

fn store(name: &str) -> (Store, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("raster-paging-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let device = Arc::new(FileDevice::open(&dir, 20, 4).unwrap());
    let config = StoreConfig {
        index_buckets: 1 << 14,
        filepath: None,
        pre_allocate: true,
        read_cache: None,
        auto_compaction: None,
        log_size_budget: 0,
        checkpoint_dir: None,
        log: LogConfig {
            page_bits: 12,
            memory_pages: 8,
            mutable_fraction: 0.5,
        },
    };
    (Store::with_device(config, device), dir)
}

/// Runs `op`; returns 1 if it went pending, otherwise checks it immediately.
fn read(session: &mut Session<'_, u64, AtomicU64>, mut op: Op) -> u64 {
    let serial = op.key;
    match session.read(&mut op, serial) {
        Status::Pending => 1,
        status => {
            ReadContext::on_complete(&mut op, status);
            0
        }
    }
}

#[test]
fn read_back_values_spilled_to_disk() {
    let (store, dir) = store("read");
    let done = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for key in 0..KEYS {
            assert_eq!(
                session.upsert(&mut Op::new(key, key * 7, None, &done), key),
                Status::Ok
            );
        }
        let mut pending = 0;
        for key in 0..KEYS + 100 {
            let expect = (key < KEYS).then_some(key * 7);
            pending += read(&mut session, Op::new(key, 0, expect, &done));
            if key % 1000 == 0 {
                session.complete_pending(false);
            }
        }
        assert!(session.complete_pending(true));
        assert!(
            pending > KEYS / 2,
            "most reads should hit the device, got {pending}"
        );
    }
    assert_eq!(done.get(), KEYS + 100);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn rmw_copies_values_from_disk() {
    let (store, dir) = store("rmw");
    let done = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for round in 0..3 {
            for key in 0..KEYS {
                let status =
                    session.rmw(&mut Op::new(key, key + 1, None, &done), round * KEYS + key);
                assert!(matches!(status, Status::Ok | Status::Pending), "{status:?}");
                if key % 512 == 0 {
                    session.complete_pending(false);
                }
            }
            assert!(session.complete_pending(true));
        }
        for key in 0..KEYS {
            read(&mut session, Op::new(key, 0, Some(3 * (key + 1)), &done));
        }
        assert!(session.complete_pending(true));
    }
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn concurrent_rmw_over_disk() {
    let (store, dir) = store("concurrent");
    const THREADS: u64 = 4;
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let store = &store;
            scope.spawn(move || {
                let done = Rc::new(Cell::new(0));
                let mut session = store.start_session();
                for key in 0..KEYS / 2 {
                    let _ = session.rmw(&mut Op::new(key, 1, None, &done), key);
                    if key % 256 == 0 {
                        session.complete_pending(false);
                    }
                }
                assert!(session.complete_pending(true));
                let _ = t;
            });
        }
    });
    let done = Rc::new(Cell::new(0));
    let mut session = store.start_session();
    for key in 0..KEYS / 2 {
        read(&mut session, Op::new(key, 0, Some(THREADS), &done));
    }
    assert!(session.complete_pending(true));
    assert_eq!(done.get(), KEYS / 2);
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

mod common;
use common::varlen::{self as variable, TestDirectory, VarRequest, VarValue};

fn variable_paging(threads: u64, rmw: bool, large: bool, name: &str) {
    let dir = TestDirectory::new(name);
    let store = variable::store(&dir, false, false);
    let keys = 800;
    let size = if large { 1024 } else { 24 };
    {
        let mut session = store.start_session();
        for id in 0..keys {
            let mut op = variable::request(id, size);
            op.data.fill(0);
            assert_eq!(session.upsert(&mut op, id), Status::Ok);
        }
    }
    std::thread::scope(|scope| {
        for worker in 0..threads {
            let store = &store;
            scope.spawn(move || {
                let mut session = store.start_session();
                for round in 0..2 {
                    for id in 0..keys {
                        if !rmw && id % threads != worker {
                            continue;
                        }
                        let mut op = variable::request(id, size + round as usize * 13);
                        let status = if rmw {
                            op.increment = 1;
                            op.suffix = vec![88; 8];
                            session.rmw(&mut op, round * keys + id)
                        } else {
                            session.upsert(&mut op, round * keys + id)
                        };
                        assert!(matches!(status, Status::Ok | Status::Pending));
                        if id % 32 == 0 {
                            session.complete_pending(false);
                        }
                    }
                    assert!(session.complete_pending(true));
                }
            });
        }
    });
    let mut session = store.start_session();
    let mut pending = 0;
    for id in 0..keys {
        let (status, bytes, disk) = variable::request(id, 0).read(&mut session, 2 * keys + id);
        assert_eq!(status, Status::Ok);
        pending += usize::from(disk);
        if rmw {
            assert_eq!(
                u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                2 * threads
            );
            assert_eq!(bytes.len(), size + 16 * threads as usize);
        } else {
            assert_eq!(bytes, vec![88; size + 13]);
        }
    }
    assert!(pending > 0);
}
#[test]
fn variable_upsert_read_serial() {
    variable_paging(1, false, false, "paging-upsert-serial");
}
#[test]
fn variable_upsert_read_concurrent() {
    variable_paging(4, false, true, "paging-upsert-concurrent");
}
#[test]
fn variable_rmw_serial() {
    variable_paging(1, true, false, "paging-rmw-serial");
}
#[test]
fn variable_rmw_large() {
    variable_paging(1, true, true, "paging-rmw-large");
}
#[test]
fn variable_rmw_concurrent() {
    variable_paging(4, true, false, "paging-rmw-concurrent");
}
#[test]
fn variable_rmw_concurrent_large() {
    variable_paging(4, true, true, "paging-rmw-concurrent-large");
}

struct ObservedDevice {
    file: FileDevice,
    reads: std::sync::Mutex<Vec<(u64, usize)>>,
}
impl device::Device for ObservedDevice {
    unsafe fn write_async(
        &self,
        offset: u64,
        data: *const u8,
        len: usize,
        done: device::WriteCallback,
    ) {
        // SAFETY: the caller keeps the same write buffer valid until done.
        unsafe { self.file.write_async(offset, data, len, done) };
    }
    fn read_async(&self, offset: u64, len: usize, done: device::ReadCallback) {
        self.reads.lock().unwrap().push((offset, len));
        self.file.read_async(offset, len, done);
    }
}
#[test]
fn variable_disk_read_expands_key_header_value_header_and_payload() {
    use common::varlen::VarKey;
    use raster::core::record::Layout;
    let dir = TestDirectory::new("sector-boundary");
    let device = Arc::new(ObservedDevice {
        file: FileDevice::open(dir.0.join("log"), 16, 2).unwrap(),
        reads: Default::default(),
    });
    let store =
        FasterKv::<VarKey, VarValue>::with_device(variable::config(&dir, false), device.clone());
    let mut session = store.start_session();
    let start = store.hlog.tail();
    let mut op = VarRequest::new(0, 701, 713);
    assert_eq!(session.upsert(&mut op, 0), Status::Ok);
    for id in 1..400 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 600), id),
            Status::Ok
        );
    }
    assert!(start < store.hlog.head.load());
    let (status, bytes, pending) = op.read(&mut session, 401);
    assert_eq!((status, bytes, pending), (Status::Ok, vec![88; 713], true));
    let offset = store.hlog.device_offset(start);
    let reads: Vec<_> = device
        .reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, _)| *at == offset)
        .map(|(_, len)| *len)
        .collect();
    assert_eq!(
        reads,
        vec![
            Layout::<VarKey, VarValue>::min_io_size(),
            Layout::<VarKey, VarValue>::min_disk_value_size(op.key_size()),
            Layout::<VarKey, VarValue>::disk_size(op.key_size(), VarValue::size_for(713)) as usize
        ]
    );
    assert!(offset / device::SECTOR < (offset + reads[2] as u64 - 1) / device::SECTOR);
}
