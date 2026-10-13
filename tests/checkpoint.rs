//! CPR checkpoints and recovery, after FASTER `cc/test/recovery_test.h`.
use raster::core::hlog::LogConfig;
use raster::core::state::Phase;
use raster::core::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type Store = FasterKv<u64, AtomicU64>;

#[derive(Clone)]
struct Incr {
    key: u64,
}
impl KeyContext<u64> for Incr {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl RmwContext<u64, AtomicU64> for Incr {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(1));
    }
    fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(old.load(Ordering::Relaxed) + 1));
    }
    fn rmw_atomic(&mut self, value: &AtomicU64) -> bool {
        value.fetch_add(1, Ordering::Relaxed);
        true
    }
}

fn store(dir: &std::path::Path) -> Store {
    let device = Arc::new(device::FileDevice::open(dir.join("log"), 30, 2).unwrap());
    Store::with_device(
        StoreConfig {
            index_buckets: 1 << 12,
            log_size_budget: 0,
            log: LogConfig {
                page_bits: 16,
                memory_pages: 16,
                mutable_fraction: 0.5,
            },
            checkpoint_dir: Some(dir.join("checkpoints")),
        },
        device,
    )
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("raster-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn full_checkpoint_completes_under_concurrent_rmw() {
    let dir = temp_dir("ckpt-smoke");
    let store = store(&dir);
    const THREADS: usize = 4;
    let index_calls = Arc::new(AtomicUsize::new(0));
    let log_serials = Arc::new(Mutex::new(Vec::new()));
    let stop = AtomicBool::new(false);
    let started = AtomicUsize::new(0);
    let token = std::thread::scope(|scope| {
        for t in 0..THREADS {
            let (store, stop, started) = (&store, &stop, &started);
            scope.spawn(move || {
                let mut session = store.start_session();
                started.fetch_add(1, Ordering::SeqCst);
                let mut serial = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    serial += 1;
                    let key = (serial * 7 + t as u64) % 2000;
                    let status = session.rmw(&mut Incr { key }, serial);
                    assert!(matches!(status, Status::Ok | Status::Pending), "{status:?}");
                    if serial.is_multiple_of(64) {
                        session.complete_pending(false);
                    }
                }
                assert!(session.complete_pending(true));
            });
        }
        while started.load(Ordering::SeqCst) != THREADS {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        let calls = index_calls.clone();
        let serials = log_serials.clone();
        let token = store
            .checkpoint(
                Some(Box::new(move |status| {
                    assert_eq!(status, Status::Ok);
                    calls.fetch_add(1, Ordering::SeqCst);
                })),
                Some(Arc::new(move |status, serial| {
                    assert_eq!(status, Status::Ok);
                    serials.lock().unwrap().push(serial);
                })),
            )
            .expect("checkpoint starts");
        assert!(store.checkpoint(None, None).is_none(), "one at a time");
        while store.system_state().phase != Phase::Rest {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        stop.store(true, Ordering::SeqCst);
        token
    });
    assert_eq!(store.system_state().version, 2);
    assert_eq!(index_calls.load(Ordering::SeqCst), 1);
    let serials = log_serials.lock().unwrap();
    assert_eq!(serials.len(), THREADS);
    assert!(serials.iter().all(|&s| s > 0));
    let root = dir.join("checkpoints");
    for file in ["ht.dat", "ofb.0.dat", "info.dat"] {
        assert!(
            root.join("index-checkpoints")
                .join(token.to_string())
                .join(file)
                .exists()
        );
    }
    let log_dir = root.join("cpr-checkpoints").join(token.to_string());
    assert_eq!(std::fs::read_dir(&log_dir).unwrap().count(), 1 + THREADS);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[derive(Clone)]
struct Get {
    key: u64,
    out: std::rc::Rc<std::cell::Cell<Option<u64>>>,
}
impl KeyContext<u64> for Get {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, AtomicU64> for Get {
    fn get(&mut self, value: &AtomicU64) {
        self.out.set(Some(value.load(Ordering::Relaxed)));
    }
    fn on_complete(&mut self, status: Status) {
        match status {
            Status::Ok => {}
            Status::NotFound => self.out.set(Some(0)),
            other => {
                eprintln!("pending read of key {} finished with {other:?}", self.key);
                FAILED_KEY.with(|k| k.set(Some(self.key)));
                self.out.set(Some(u64::MAX));
            }
        }
    }
}

fn read_all(store: &Store, keys: u64) -> Vec<u64> {
    let mut session = store.start_session();
    let cells: Vec<_> = (0..keys)
        .map(|_| std::rc::Rc::new(std::cell::Cell::new(None)))
        .collect();
    for key in 0..keys {
        let mut get = Get {
            key,
            out: cells[key as usize].clone(),
        };
        match session.read(&mut get, key) {
            Status::Ok | Status::Pending => {}
            Status::NotFound => cells[key as usize].set(Some(0)),
            other => panic!("read {other:?}"),
        }
        if key % 256 == 0 {
            session.complete_pending(false);
        }
    }
    assert!(session.complete_pending(true));
    cells
        .iter()
        .map(|c| c.get().expect("every read completes"))
        .collect()
}

fn key_of(t: u64, i: u64, keys: u64) -> u64 {
    (i * 7 + t) % keys
}

#[test]
fn recovery_restores_exactly_each_session_prefix() {
    recover_prefix(4, "ckpt-recover-4");
}

#[test]
fn recovery_restores_exactly_one_session_prefix() {
    recover_prefix(1, "ckpt-recover-1");
}

fn recover_prefix(threads: u64, name: &str) {
    let dir = temp_dir(name);
    const KEYS: u64 = 60_000;
    let (token, guids) = {
        let store = store(&dir);
        let stop = AtomicBool::new(false);
        let started = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|t| {
                    let (store, stop, started) = (&store, &stop, &started);
                    scope.spawn(move || {
                        let mut session = store.start_session();
                        started.fetch_add(1, Ordering::SeqCst);
                        let mut i = 0u64;
                        while !stop.load(Ordering::SeqCst) || i < 200_000 {
                            i += 1;
                            let status = session.rmw(
                                &mut Incr {
                                    key: key_of(t, i, KEYS),
                                },
                                i,
                            );
                            assert!(matches!(status, Status::Ok | Status::Pending));
                            if i.is_multiple_of(128) {
                                session.complete_pending(false);
                            }
                        }
                        assert!(session.complete_pending(true));
                        session.guid()
                    })
                })
                .collect();
            while started.load(Ordering::SeqCst) != threads as usize {
                std::thread::yield_now();
            }
            std::thread::sleep(std::time::Duration::from_millis(30));
            let token = store.checkpoint(None, None).expect("checkpoint starts");
            while store.system_state().phase != Phase::Rest {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            // Keep running past the checkpoint; recovery must drop this work.
            std::thread::sleep(std::time::Duration::from_millis(30));
            stop.store(true, Ordering::SeqCst);
            let guids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            (token, guids)
        })
    };

    let store = store(&dir);
    let info = store.recover(token, token).expect("recovery succeeds");
    assert_eq!(info.version, 1);
    assert_eq!(store.system_state().version, 2);
    let mut expected = vec![0u64; KEYS as usize];
    let mut serials = Vec::new();
    for (t, guid) in guids.iter().enumerate() {
        assert!(info.sessions.contains(guid));
        let (_session, serial) = store.continue_session(*guid).expect("session recovered");
        serials.push(serial);
        assert!(
            serial > 0,
            "session {t} made progress before the checkpoint"
        );
        for i in 1..=serial {
            expected[key_of(t as u64, i, KEYS) as usize] += 1;
        }
    }
    assert!(store.continue_session(guids[0]).is_none(), "resumed once");
    let actual = read_all(&store, KEYS);
    if let Some(key) = FAILED_KEY.with(|k| k.get()) {
        dump_key(&dir, token, key, &guids, &serials, KEYS);
        panic!("pending read of key {key} failed");
    }
    let wrong = (0..KEYS as usize)
        .filter(|&k| actual[k] != expected[k])
        .count();
    if wrong > 0 {
        let more = (0..KEYS as usize)
            .filter(|&k| actual[k] > expected[k])
            .count();
        let samples: Vec<_> = (0..KEYS as usize)
            .filter(|&k| actual[k] != expected[k])
            .take(8)
            .map(|k| (k, expected[k], actual[k]))
            .collect();
        eprintln!(
            "wrong={wrong} more={more} less={} samples(key,exp,act)={samples:?}",
            wrong - more
        );
        dump_key(&dir, token, samples[0].0 as u64, &guids, &serials, KEYS);
    }
    assert_eq!(wrong, 0, "keys whose value is not the recovered prefix");

    // The recovered store keeps working.
    {
        let mut session = store.start_session();
        for key in 0..1000 {
            let _ = session.rmw(&mut Incr { key }, key + 1);
        }
        assert!(session.complete_pending(true));
    }
    let after = read_all(&store, 1000);
    assert!((0..1000).all(|k| after[k] == expected[k] + 1));
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// Diagnostic: prints every log record of `key` from the segment file.
fn dump_key(
    dir: &std::path::Path,
    token: raster::core::guid::Guid,
    key: u64,
    guids: &[raster::core::guid::Guid],
    serials: &[u64],
    keys: u64,
) {
    use raster::core::checkpoint::{CheckpointDirs, IndexMetadata, LogMetadata};
    let dirs = CheckpointDirs {
        root: dir.join("checkpoints"),
    };
    let index = IndexMetadata::read(&dirs.index(token)).unwrap();
    let log = LogMetadata::read(&dirs.log(token)).unwrap();
    let addr = |a: raster::core::address::Address| (a.page(), a.offset());
    eprintln!(
        "start={:?} final={:?} serials={serials:?}",
        addr(index.checkpoint_start_address),
        addr(log.final_address)
    );
    for (t, &serial) in serials.iter().enumerate().take(guids.len()) {
        let ops: Vec<u64> = (1..=serial + 20_000)
            .filter(|&i| key_of(t as u64, i, keys) == key)
            .collect();
        eprintln!("  thread {t} ops on key {key}: {ops:?}");
    }
    let bytes = std::fs::read(dir.join("log").join("log.0")).unwrap();
    let page_size = 1usize << 16;
    for page in 0..bytes.len() / page_size + 1 {
        let mut offset = if page == 0 { 64 } else { 0 };
        while offset + 24 <= page_size && page * page_size + offset + 24 <= bytes.len() {
            let at = page * page_size + offset;
            let header = u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
            if header == 0 {
                // Report zero runs that are followed by more records (holes).
                let run_start = offset;
                while offset + 8 <= page_size
                    && page * page_size + offset + 8 <= bytes.len()
                    && bytes[page * page_size + offset..page * page_size + offset + 8] == [0; 8]
                {
                    offset += 8;
                }
                if offset + 8 <= page_size && page * page_size + offset + 8 <= bytes.len() {
                    eprintln!(
                        "  hole ({page},{run_start})..({page},{offset}) = {} bytes",
                        offset - run_start
                    );
                }
                continue;
            }
            let k = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap());
            if k == key {
                let value = u64::from_le_bytes(bytes[at + 16..at + 24].try_into().unwrap());
                let prev = header & ((1 << 48) - 1);
                eprintln!(
                    "  record ({page},{offset}) v={} invalid={} prev=({},{}) value={value}",
                    (header >> 48) & 0x1fff,
                    (header >> 61) & 1,
                    prev >> 25,
                    prev & ((1 << 25) - 1)
                );
            }
            offset += 24;
        }
    }
}

thread_local! {
    static FAILED_KEY: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

mod common;
use common::varlen::{self as variable, TestDirectory};

fn variable_recovery(mode: &str, large: bool, threads: u64, name: &str) {
    let dir = TestDirectory::new(name);
    let keys = 64;
    let length = if large { 1001 } else { 31 };
    let store = variable::store(&dir, false, false);
    if mode != "insert" {
        let mut session = store.start_session();
        for id in 0..keys * threads {
            let mut op = variable::request(id, length);
            op.data.fill(0);
            assert_eq!(session.upsert(&mut op, id), Status::Ok);
        }
    }
    let started = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let (token, guids) = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..threads {
            let (store, started, stop) = (&store, &started, &stop);
            handles.push(scope.spawn(move || {
                let mut session = store.start_session();
                for serial in 1..=512 {
                    let key = if mode == "insert" {
                        (serial - 1) * threads + worker
                    } else {
                        worker * keys + (serial - 1) % keys
                    };
                    let mut op = variable::request(key, length + serial as usize % 17);
                    let status = if mode == "rmw" {
                        op.increment = 1;
                        op.suffix = vec![88; 8];
                        session.rmw(&mut op, serial)
                    } else {
                        op.data[..8].copy_from_slice(&serial.to_le_bytes());
                        session.upsert(&mut op, serial)
                    };
                    assert!(matches!(status, Status::Ok | Status::Pending));
                    if serial % 8 == 0 {
                        session.complete_pending(false);
                    }
                    if serial == 16 {
                        started.fetch_add(1, Ordering::SeqCst);
                    }
                }
                while !stop.load(Ordering::SeqCst) {
                    session.complete_pending(false);
                    std::thread::yield_now();
                }
                session.complete_pending(true);
                session.guid()
            }));
        }
        while started.load(Ordering::SeqCst) < threads as usize {
            std::thread::yield_now();
        }
        let token = store.checkpoint(None, None).unwrap();
        while store.system_state().phase != Phase::Rest {
            std::thread::yield_now();
        }
        stop.store(true, Ordering::SeqCst);
        (
            token,
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>(),
        )
    });
    drop(store);
    let store = variable::store(&dir, false, false);
    let info = store.recover(token, token).unwrap();
    let mut expected = std::collections::BTreeMap::new();
    for (worker, guid) in guids.into_iter().enumerate() {
        assert!(info.sessions.contains(&guid));
        let (session, serial) = store.continue_session(guid).unwrap();
        drop(session);
        assert!((16..=512).contains(&serial));
        if mode != "insert" {
            for id in 0..keys {
                expected.insert(worker as u64 * keys + id, (0u64, length));
            }
        }
        for i in 1..=serial {
            let key = if mode == "insert" {
                (i - 1) * threads + worker as u64
            } else {
                worker as u64 * keys + (i - 1) % keys
            };
            if mode == "rmw" {
                let value = expected.get_mut(&key).unwrap();
                value.0 += 1;
                value.1 += 8;
            } else {
                expected.insert(key, (i, length + i as usize % 17));
            }
        }
    }
    let mut session = store.start_session();
    for (serial, (key, (count, len))) in expected.into_iter().enumerate() {
        let (status, bytes, _) = variable::request(key, 0).read(&mut session, serial as u64);
        assert_eq!(status, Status::Ok, "key {key}");
        assert_eq!(
            u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            count,
            "key {key}"
        );
        assert_eq!(bytes.len(), len, "key {key}");
    }
}
#[test]
fn recovery_serial_variable_length_keys() {
    variable_recovery("insert", false, 1, "recover-serial-var-key");
}
#[test]
fn recovery_concurrent_insert_small() {
    variable_recovery("insert", false, 4, "recover-insert-small");
}
#[test]
fn recovery_concurrent_insert_large() {
    variable_recovery("insert", true, 4, "recover-insert-large");
}
#[test]
fn recovery_concurrent_update_small() {
    variable_recovery("update", false, 4, "recover-update-small");
}
#[test]
fn recovery_concurrent_update_large() {
    variable_recovery("update", true, 4, "recover-update-large");
}
#[test]
fn recovery_concurrent_rmw_small() {
    variable_recovery("rmw", false, 4, "recover-rmw-small");
}
#[test]
fn recovery_concurrent_rmw_large() {
    variable_recovery("rmw", true, 4, "recover-rmw-large");
}
