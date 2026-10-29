//! Log truncation with ShiftBeginAddress, after FASTER's GC tests.
use raster::core::address::Address;
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Op {
    key: u64,
    value: u64,
    out: Rc<Cell<Option<(Status, u64)>>>,
}
impl Op {
    fn new(key: u64, value: u64) -> Self {
        Self {
            key,
            value,
            out: Rc::default(),
        }
    }
}
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<u64> for Op {
    fn key(&self) -> &u64 {
        &self.key
    }
}
// SAFETY: New values are fully initialized in their fixed-size slots;
// in-place updates use atomics or decline mutation of shared values.
unsafe impl UpsertContext<u64, AtomicU64> for Op {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.value));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.value, Ordering::Relaxed);
        true
    }
}
impl ReadContext<u64, AtomicU64> for Op {
    fn get(&mut self, value: &AtomicU64) {
        self.value = value.load(Ordering::Relaxed);
    }
    fn on_complete(&mut self, status: Status) {
        self.out.set(Some((status, self.value)));
    }
}

/// Reads `key` to completion and returns its status and value.
fn read(session: &mut Session<'_, u64, AtomicU64>, key: u64, serial: u64) -> (Status, u64) {
    let mut op = Op::new(key, u64::MAX);
    let out = op.out.clone();
    match session.read(&mut op, serial) {
        Status::Pending => assert!(session.complete_pending(true)),
        status => ReadContext::on_complete(&mut op, status),
    }
    out.get().expect("read finished")
}

fn checkpoint_after_truncation(retained: u64) {
    let dir = std::env::temp_dir().join(format!(
        "raster-gc-recovery-{retained}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let make_store = || {
        FasterKv::<u64, AtomicU64>::with_device(
            StoreConfig {
                index_buckets: 1 << 12,
                log: LogConfig {
                    page_bits: 12,
                    memory_pages: 16,
                    mutable_fraction: 0.5,
                },
                checkpoint_dir: Some(dir.join("checkpoints")),
                ..Default::default()
            },
            Arc::new(FileDevice::open(dir.join("log"), 14, 1).unwrap()),
        )
    };
    let store = make_store();
    let mut session = store.start_session();
    const KEYS: u64 = 4_000;
    for key in 0..KEYS {
        assert_eq!(session.upsert(&mut Op::new(key, key), key), Status::Ok);
    }
    let cut = store.hlog.tail();
    for key in KEYS..KEYS + retained {
        assert_eq!(session.upsert(&mut Op::new(key, key), key), Status::Ok);
    }
    let tail = store.hlog.tail();
    store.hlog.shift_read_only_to(tail);
    while store.hlog.safe_read_only.load() < tail {
        session.refresh();
    }
    let truncated = Arc::new(AtomicU64::new(0));
    let observed = truncated.clone();
    assert!(store.shift_begin_address(
        cut,
        Some(Box::new(move |offset| {
            observed.store(offset, Ordering::Release)
        })),
        None,
    ));
    assert!(session.complete_pending(true));
    assert!(truncated.load(Ordering::Acquire) >= 1 << 14);
    let completed = Arc::new(Mutex::new(None));
    let observed = completed.clone();
    let token = store
        .checkpoint(
            None,
            Some(Arc::new(move |status, _| {
                *observed.lock().unwrap() = Some(status)
            })),
        )
        .unwrap();
    assert!(session.complete_pending(true));
    assert_eq!(*completed.lock().unwrap(), Some(Status::Ok));
    drop(session);
    drop(store);

    let mut recovered = make_store();
    recovered
        .recover(token, token)
        .expect("valid checkpoint after GC");
    assert_eq!(recovered.hlog.begin.load(), cut);
    assert_eq!(recovered.hlog.tail(), tail);
    let mut session = recovered.start_session();
    assert_eq!(read(&mut session, 0, 0).0, Status::NotFound);
    for key in KEYS..KEYS + retained {
        assert_eq!(read(&mut session, key, key), (Status::Ok, key));
    }
    let key = KEYS + retained;
    assert_eq!(session.upsert(&mut Op::new(key, 99), key), Status::Ok);
    assert_eq!(read(&mut session, key, key + 1), (Status::Ok, 99));
    drop(session);
    drop(recovered);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_after_gc_with_an_empty_log() {
    checkpoint_after_truncation(0);
}

#[test]
fn recovery_after_gc_with_a_partial_page() {
    checkpoint_after_truncation(16);
}

#[test]
fn shift_begin_address_rejects_invalid_record_boundaries() {
    let store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
        index_buckets: 64,
        log: LogConfig {
            page_bits: 12,
            memory_pages: 16,
            mutable_fraction: 0.5,
        },
        ..Default::default()
    });
    let mut session = store.start_session();
    for key in 0..512 {
        assert_eq!(session.upsert(&mut Op::new(key, key), key), Status::Ok);
    }
    let tail = store.hlog.tail();
    store.hlog.shift_read_only_to(tail);
    while store.hlog.safe_read_only.load() < tail {
        session.refresh();
    }
    let begin = store.hlog.begin.load();
    for invalid in [Address::new(0, 65), Address::new(0, 4096)] {
        assert!(invalid > begin && invalid < tail);
        assert!(!store.shift_begin_address(invalid, None, None));
        assert_eq!(store.hlog.begin.load(), begin);
        assert_eq!(store.system_state().action, state::Action::None);
    }
}

#[test]
fn shift_begin_address_truncates_segments_and_index() {
    let dir = std::env::temp_dir().join(format!("raster-gc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let device = Arc::new(FileDevice::open(&dir, 16, 2).unwrap());
    let store = FasterKv::<u64, AtomicU64>::with_device(
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
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        device,
    );
    const KEYS: u64 = 20_000;
    let mut session = store.start_session();
    for key in 0..KEYS {
        assert_eq!(session.upsert(&mut Op::new(key, 1), key), Status::Ok);
    }
    let cut = store.hlog.tail();
    for key in 0..KEYS / 2 {
        assert_eq!(session.upsert(&mut Op::new(key, 2), KEYS + key), Status::Ok);
    }
    session.refresh();
    let truncated = Arc::new(Mutex::new(None));
    let completed = Arc::new(AtomicUsize::new(0));
    let (t, c) = (truncated.clone(), completed.clone());
    assert!(
        !store.shift_begin_address(store.hlog.tail(), None, None),
        "beyond read-only"
    );
    assert!(store.shift_begin_address(
        cut,
        Some(Box::new(move |offset| *t.lock().unwrap() = Some(offset))),
        Some(Box::new(move || {
            c.fetch_add(1, Ordering::SeqCst);
        })),
    ));
    assert!(session.complete_pending(true));
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    let offset = truncated.lock().unwrap().expect("truncate callback ran");
    assert!(offset > 0 && offset <= store.hlog.device_offset(cut));
    assert!(!dir.join("log.0").exists(), "first segment removed");

    let mut found = 0;
    for key in 0..KEYS {
        let (status, value) = read(&mut session, key, 2 * KEYS + key);
        assert_eq!(
            status == Status::Ok,
            key < KEYS / 2,
            "key {key}: {status:?}"
        );
        if status == Status::Ok {
            assert_eq!(value, 2, "key {key}");
        }
        found += (status == Status::Ok) as u64;
    }
    assert_eq!(found, KEYS / 2);
    // The truncated store keeps working.
    assert_eq!(
        session.upsert(&mut Op::new(KEYS - 1, 2), 3 * KEYS),
        Status::Ok
    );
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn gc_completes_while_sessions_keep_writing() {
    let dir = std::env::temp_dir().join(format!("raster-gc-busy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let device = Arc::new(FileDevice::open(&dir, 16, 2).unwrap());
    let store = FasterKv::<u64, AtomicU64>::with_device(
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
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        device,
    );
    const THREADS: u64 = 4;
    const KEYS: u64 = 2_000;
    const ROUNDS: u64 = 30;
    let mut cutoffs = Vec::new();
    {
        let mut session = store.start_session();
        for key in 0..THREADS * KEYS {
            assert_eq!(session.upsert(&mut Op::new(key, 0), key), Status::Ok);
            if [THREADS * KEYS / 3, THREADS * KEYS * 2 / 3, THREADS * KEYS].contains(&(key + 1)) {
                cutoffs.push(store.hlog.tail());
            }
        }
        let end = *cutoffs.last().unwrap();
        store.hlog.shift_read_only_to(end);
        while store.hlog.safe_read_only.load() < end {
            session.refresh();
        }
    }
    let completed = Arc::new(AtomicUsize::new(0));
    let ready = std::sync::Barrier::new(THREADS as usize + 1);
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let store = &store;
            let completed = &completed;
            let ready = &ready;
            scope.spawn(move || {
                // Another action may already be running; wait for REST.
                let mut session = loop {
                    if let Some(session) = store.try_start_session() {
                        break session;
                    }
                    std::thread::yield_now();
                };
                ready.wait();
                for round in 0..ROUNDS {
                    for key in 0..KEYS {
                        // Values are the round number; every write appends
                        // once the record leaves the mutable region.
                        let _ =
                            session.upsert(&mut Op::new(t * KEYS + key, round), round * KEYS + key);
                    }
                    session.complete_pending(false);
                }
                assert!(session.complete_pending(true));
                // Keep driving GC even if this writer finishes its rounds early.
                while completed.load(Ordering::Acquire) < 3 {
                    session.refresh();
                    std::thread::yield_now();
                }
            });
        }
        ready.wait();
        let mut shifted = 0;
        while shifted < 3 {
            std::thread::sleep(std::time::Duration::from_millis(5));
            // Reclaim only the frozen old prefix, never a writer's final value.
            let begin = cutoffs[shifted];
            let c = completed.clone();
            if store.shift_begin_address(
                begin,
                None,
                Some(Box::new(move || {
                    c.fetch_add(1, Ordering::SeqCst);
                })),
            ) {
                shifted += 1;
                while completed.load(Ordering::SeqCst) < shifted {
                    std::thread::yield_now();
                }
            }
        }
    });
    assert_eq!(completed.load(Ordering::SeqCst), 3);
    let mut session = store.start_session();
    for key in 0..THREADS * KEYS {
        let (status, value) = read(&mut session, key, key);
        assert_eq!((status, value), (Status::Ok, ROUNDS - 1), "key {key}");
    }
    drop(session);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
