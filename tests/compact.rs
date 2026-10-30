//! Log compaction, after FASTER's CompactWithLookup tests.
use raster::core::address::Address;
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Clone)]
struct Op {
    key: u64,
    input: u64,
    out: Rc<Cell<Option<(Status, u64)>>>,
}
impl Op {
    fn new(key: u64, input: u64) -> Self {
        Self {
            key,
            input,
            out: Rc::default(),
        }
    }
}
// SAFETY: The borrowed scan record is complete and immutable; suspension
// deep-copies its initialized words before the scanner can release it.
unsafe impl KeyContext<u64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Op {
    fn get(&mut self, value: &AtomicU64) {
        self.input = value.load(Ordering::Relaxed);
    }
    fn on_complete(&mut self, status: Status) {
        self.out.set(Some((status, self.input)));
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl UpsertContext<u64, AtomicU64> for Op {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::Relaxed);
        true
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl RmwContext<u64, AtomicU64> for Op {
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
}
// SAFETY: The tombstone size matches the value header initialized by the store.
unsafe impl DeleteContext<u64> for Op {}

fn store(dir: &std::path::Path) -> FasterKv<u64, AtomicU64> {
    let _ = std::fs::remove_dir_all(dir);
    let device = Arc::new(FileDevice::open(dir, 16, 2).unwrap());
    FasterKv::with_device(
        StoreConfig {
            index_buckets: 1 << 12,
            filepath: None,
            pre_allocate: true,
            read_cache: None,
            auto_compaction: None,
            log_size_budget: 0,
            checkpoint_dir: None,
            log: LogConfig {
                page_bits: 12,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
        },
        device,
    )
}

fn read(session: &mut Session<'_, u64, AtomicU64>, key: u64) -> (Status, u64) {
    let mut op = Op::new(key, u64::MAX);
    let out = op.out.clone();
    match session.read(&mut op, key) {
        Status::Pending => assert!(session.complete_pending(true)),
        status => ReadContext::on_complete(&mut op, status),
    }
    out.get().expect("read finished")
}

#[test]
fn compaction_keeps_live_records_and_drops_the_rest() {
    let dir = std::env::temp_dir().join(format!("raster-compact-{}", std::process::id()));
    let store = store(&dir);
    const KEYS: u64 = 20_000;
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(session.upsert(&mut Op::new(key, 1), key), Status::Ok);
    }
    for key in (0..KEYS).step_by(2) {
        assert_eq!(session.upsert(&mut Op::new(key, 2), key), Status::Ok);
    }
    for key in (0..KEYS).step_by(3) {
        assert_eq!(session.delete(&Op::new(key, 0), key), Status::Ok);
    }
    session.refresh();
    let begin_before = store.hlog.begin.load();
    let until = session
        .compact(Address::MAX, true, 3, false)
        .expect("compaction succeeds")
        .until;
    assert!(until > begin_before);
    assert_eq!(store.hlog.begin.load(), until);
    for key in 0..KEYS {
        let expected = if key % 3 == 0 {
            (Status::NotFound, u64::MAX)
        } else if key % 2 == 0 {
            (Status::Ok, 2)
        } else {
            (Status::Ok, 1)
        };
        assert_eq!(read(&mut session, key), expected, "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn compaction_under_concurrent_rmw_loses_no_update() {
    let dir = std::env::temp_dir().join(format!("raster-compact-busy-{}", std::process::id()));
    let store = store(&dir);
    const THREADS: u64 = 4;
    const KEYS: u64 = 3_000;
    const ROUNDS: u64 = 30;
    let compactions = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let store = &store;
            scope.spawn(move || {
                // Another action may already be running; wait for REST.
                let mut session = loop {
                    if let Some(session) = store.try_start_session() {
                        break session;
                    }
                    std::thread::yield_now();
                };
                for round in 0..ROUNDS {
                    for key in 0..KEYS {
                        let serial = (round * KEYS + key) * THREADS + t;
                        match session.rmw(&mut Op::new(key, 1), serial) {
                            Status::Ok | Status::Pending => {}
                            status => panic!("rmw {status:?}"),
                        }
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
        let mut session = store.start_session();
        while compactions.load(Ordering::SeqCst) < 3 {
            std::thread::sleep(std::time::Duration::from_millis(5));
            assert_eq!(
                session
                    .compact(Address::MAX, true, 2, false)
                    .expect("compaction succeeds")
                    .checkpoint,
                None
            );
            compactions.fetch_add(1, Ordering::SeqCst);
        }
    });
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(
            read(&mut session, key),
            (Status::Ok, THREADS * ROUNDS),
            "key {key}"
        );
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn auto_compaction_keeps_the_log_near_its_budget() {
    use raster::core::auto_compact::AutoCompactionConfig;
    let dir = std::env::temp_dir().join(format!("raster-autocompact-{}", std::process::id()));
    let store = Arc::new(store(&dir));
    const KEYS: u64 = 2_000;
    const BUDGET: u64 = 256 << 10;
    let config = AutoCompactionConfig {
        check_interval: std::time::Duration::from_millis(5),
        threads: 2,
        ..AutoCompactionConfig::with_budget(BUDGET)
    };
    let begin = store.hlog.begin.load();
    let compactor = store.spawn_auto_compaction(config);
    std::thread::scope(|scope| {
        for t in 0..2u64 {
            let store = &store;
            scope.spawn(move || {
                // Another action may already be running; wait for REST.
                let mut session = loop {
                    if let Some(session) = store.try_start_session() {
                        break session;
                    }
                    std::thread::yield_now();
                };
                for round in 0..200u64 {
                    for key in 0..KEYS {
                        let _ =
                            session.upsert(&mut Op::new(key, round), (round * KEYS + key) * 2 + t);
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
            });
        }
    });
    drop(compactor);
    assert!(
        store.hlog.begin.load() > begin,
        "auto compaction truncated the log"
    );
    assert!(
        store.log_size() < 4 * BUDGET,
        "log size {}",
        store.log_size()
    );
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(read(&mut session, key), (Status::Ok, 199), "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

mod common;
use common::varlen::{self as variable, TestDirectory};

fn variable_compaction(resident: bool, long_keys: bool, name: &str) {
    let dir = TestDirectory::new(name);
    let store = variable::store(&dir, resident, false);
    let mut session = store.start_session();
    let make = |id| {
        common::varlen::VarRequest::new(
            id,
            if long_keys { 8 + id as usize % 701 } else { 8 },
            if long_keys { 8 } else { 8 + id as usize % 900 },
        )
    };
    for id in 0..1200 {
        assert_eq!(session.upsert(&mut make(id), id), Status::Ok);
    }
    let until = store.hlog.tail();
    store.hlog.shift_read_only_to(store.hlog.tail());
    session.refresh();
    for id in 0..1200 {
        if id % 3 == 0 {
            assert_eq!(session.delete(&make(id), 1200 + id), Status::Ok);
        } else if id % 2 == 0 {
            let mut op = make(id);
            op.data.fill(19);
            assert_eq!(session.upsert(&mut op, 1200 + id), Status::Ok);
        }
    }
    assert!(session.complete_pending(true));
    let compacted = session.compact(until, true, 3, false).unwrap().until;
    assert!(compacted <= until);
    for id in 0..1200 {
        let mut op = make(id);
        let expected_len = op.data.len();
        let (status, bytes, _) = op.read(&mut session, 2400 + id);
        if id % 3 == 0 {
            assert_eq!(status, Status::NotFound);
        } else {
            assert_eq!(status, Status::Ok);
            assert_eq!(bytes, vec![if id % 2 == 0 { 19 } else { 88 }; expected_len]);
        }
    }
}
#[test]
fn compact_in_memory_variable_length_keys() {
    variable_compaction(true, true, "compact-mem-key");
}
#[test]
fn compact_in_memory_variable_length_values() {
    variable_compaction(true, false, "compact-mem-value");
}
#[test]
fn compact_on_disk_variable_length_keys() {
    variable_compaction(false, true, "compact-disk-key");
}
#[test]
fn compact_on_disk_variable_length_values() {
    variable_compaction(false, false, "compact-disk-value");
}

#[test]
fn variable_disk_read_compaction_race() {
    let dir = TestDirectory::new("compact-pending-read");
    let store = variable::store(&dir, false, false);
    let mut session = store.start_session();
    for id in 0..500 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 333), id),
            Status::Ok
        );
    }
    let mut read = variable::request(0, 0);
    assert_eq!(session.read(&mut read, 500), Status::Pending);
    let until = store.hlog.safe_read_only.load();
    assert_eq!(
        session.compact(until, true, 2, false).unwrap().checkpoint,
        None
    );
    assert!(session.complete_pending(true));
    assert_eq!(
        read.out.borrow_mut().take(),
        Some((Status::Ok, vec![88; 333]))
    );
    assert_eq!(read.read(&mut session, 501).1, vec![88; 333]);
}

#[test]
fn variable_disk_delete_all_and_reinsert() {
    let dir = TestDirectory::new("compact-reinsert");
    let store = variable::store(&dir, false, false);
    let mut session = store.start_session();
    for id in 0..500 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 333), id),
            Status::Ok
        );
    }
    for id in 0..500 {
        assert_eq!(
            session.delete(&variable::request(id, 21), 500 + id),
            Status::Ok
        );
    }
    assert_eq!(
        session
            .compact(Address::MAX, true, 2, false)
            .unwrap()
            .checkpoint,
        None
    );
    for id in 0..500 {
        assert_eq!(
            session.upsert(&mut variable::request(id, 711), 1000 + id),
            Status::Ok
        );
    }
    assert_eq!(
        session
            .compact(Address::MAX, true, 2, false)
            .unwrap()
            .checkpoint,
        None
    );
    for id in 0..500 {
        assert_eq!(
            variable::request(id, 0).read(&mut session, 1500 + id).1,
            vec![88; 711]
        );
    }
}

fn variable_concurrent_compaction(resident: bool, name: &str) {
    let dir = TestDirectory::new(name);
    let store = variable::store(&dir, resident, false);
    {
        let mut session = store.start_session();
        for id in 0..500 {
            let mut op = variable::request(id, 64);
            op.data.fill(0);
            assert_eq!(session.upsert(&mut op, id), Status::Ok);
        }
    }
    std::thread::scope(|scope| {
        for _ in 0..3 {
            let store = &store;
            scope.spawn(move || {
                let mut session = variable::session(store);
                for round in 0..4 {
                    for id in 0..500 {
                        let mut op = variable::request(id, 0);
                        op.increment = 1;
                        op.suffix = vec![88; 8];
                        let status = session.rmw(&mut op, round * 500 + id);
                        assert!(matches!(status, Status::Ok | Status::Pending));
                        if id % 32 == 0 {
                            session.complete_pending(false);
                        }
                    }
                    session.complete_pending(true);
                }
            });
        }
        let mut session = variable::session(&store);
        for _ in 0..4 {
            store.hlog.shift_read_only_to(store.hlog.tail());
            session.refresh();
            assert_eq!(
                session
                    .compact(Address::MAX, true, 2, false)
                    .unwrap()
                    .checkpoint,
                None
            );
        }
    });
    let mut session = store.start_session();
    for id in 0..500 {
        let (status, bytes, _) = variable::request(id, 0).read(&mut session, id);
        assert_eq!(status, Status::Ok);
        assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), 12);
        assert_eq!(bytes.len(), 64 + 12 * 8);
    }
}
#[test]
fn variable_in_memory_concurrent_compaction() {
    variable_concurrent_compaction(true, "compact-concurrent-memory");
}
#[test]
fn variable_on_disk_concurrent_compaction() {
    variable_concurrent_compaction(false, "compact-concurrent-disk");
}

#[test]
fn budget_limited_writer_claims_compaction_pages() {
    let dir = TestDirectory::new("compact-assistance");
    let mut config = variable::config(&dir, false);
    config.log_size_budget = 1;
    let store = common::varlen::VarStore::with_device(
        config,
        Arc::new(FileDevice::open(dir.0.join("log"), 16, 2).unwrap()),
    );
    {
        let mut session = store.start_session();
        for id in 0..3000 {
            assert_eq!(
                session.upsert(&mut variable::request(id, 1000), id),
                Status::Ok
            );
        }
    }
    let done = std::sync::atomic::AtomicBool::new(false);
    let barrier = std::sync::Barrier::new(2);
    let count = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let mut session = store.start_session();
            common::varlen::SIZE_READS.with(|count| count.set(0));
            barrier.wait();
            let mut serial = 3000;
            while !done.load(Ordering::Acquire) {
                assert_eq!(
                    session.upsert(&mut variable::request(2999, 1000), serial),
                    Status::Ok
                );
                serial += 1;
                session.refresh();
            }
            common::varlen::SIZE_READS.with(|count| count.get())
        });
        let mut session = store.start_session();
        barrier.wait();
        assert_eq!(
            session
                .compact(Address::MAX, true, 1, false)
                .unwrap()
                .checkpoint,
            None
        );
        done.store(true, Ordering::Release);
        // The writer may still be finishing an epoch transition.
        drop(session);
        worker.join().unwrap()
    });
    assert!(
        count > 0,
        "writer did not inspect any claimed compaction records"
    );
    let mut session = store.start_session();
    for id in 0..3000 {
        assert_eq!(
            variable::request(id, 0).read(&mut session, id).1,
            vec![88; 1000]
        );
    }
}

#[test]
fn compaction_checkpoint_recovers_variable_values() {
    let dir = TestDirectory::new("compact-checkpoint");
    let store = variable::store(&dir, false, false);
    let token = {
        let mut session = store.start_session();
        for id in 0..600 {
            assert_eq!(
                session.upsert(&mut variable::request(id, 8 + id as usize % 701), id),
                Status::Ok
            );
        }
        session
            .compact(Address::MAX, true, 2, true)
            .unwrap()
            .checkpoint
            .expect("checkpoint requested")
            .expect("checkpoint succeeds")
    };
    drop(store);
    let mut store = variable::store(&dir, false, false);
    store.recover(token, token).unwrap();
    let mut session = store.start_session();
    for id in 0..600 {
        assert_eq!(
            variable::request(id, 0).read(&mut session, id).1,
            vec![88; 8 + id as usize % 701]
        );
    }
}

struct BrokenDevice {
    file: FileDevice,
    broken: std::sync::atomic::AtomicBool,
    corrupt: bool,
    target: std::sync::atomic::AtomicU64,
}
impl device::Device for BrokenDevice {
    unsafe fn write_async(
        &self,
        offset: u64,
        data: *const u8,
        len: usize,
        done: device::WriteCallback,
    ) {
        // SAFETY: forwarding the caller's unchanged buffer and completion lifetime.
        unsafe { self.file.write_async(offset, data, len, done) };
    }
    fn read_async(&self, offset: u64, len: usize, done: device::ReadCallback) {
        if !self.broken.load(Ordering::Acquire) {
            self.file.read_async(offset, len, done);
            return;
        }
        if !self.corrupt {
            done(Err(std::io::Error::other("injected read failure")));
            return;
        }
        let target = self.target.load(Ordering::Acquire);
        self.file.read_async(
            offset,
            len,
            Box::new(move |result| {
                done(result.map(|mut bytes| {
                    if target >= offset && target + 4 <= offset + bytes.len() as u64 {
                        let at = (target - offset) as usize;
                        bytes[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                    }
                    bytes
                }));
            }),
        );
    }
}

#[test]
fn compaction_does_not_truncate_after_read_failure_or_corruption() {
    for corrupt in [false, true] {
        let dir = TestDirectory::new(if corrupt {
            "compact-corrupt"
        } else {
            "compact-io-error"
        });
        let device = Arc::new(BrokenDevice {
            file: FileDevice::open(dir.0.join("log"), 16, 2).unwrap(),
            broken: std::sync::atomic::AtomicBool::new(false),
            corrupt,
            target: std::sync::atomic::AtomicU64::new(0),
        });
        let store =
            common::varlen::VarStore::with_device(variable::config(&dir, false), device.clone());
        let mut session = store.start_session();
        let begin = store.hlog.begin.load();
        device
            .target
            .store(store.hlog.device_offset(begin) + 8, Ordering::Release);
        for id in 0..600 {
            assert_eq!(
                session.upsert(&mut variable::request(id, 600), id),
                Status::Ok
            );
        }
        device.broken.store(true, Ordering::Release);
        assert_eq!(
            session.compact(Address::MAX, true, 2, false),
            Err(if corrupt {
                Status::Corruption
            } else {
                Status::IoError
            })
        );
        assert_eq!(store.hlog.begin.load(), begin);
    }
}
