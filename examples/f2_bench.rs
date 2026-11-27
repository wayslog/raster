//! Buffered F2 benchmark paired with tools/performance/f2.cc.
use raster::core::address::Address;
use raster::core::checkpoint::{CheckpointDirs, LogMetadata};
use raster::core::cold_store::ColdStore;
use raster::core::f2::{F2Config, F2Kv};
use raster::core::hlog::LogConfig;
use raster::core::{
    FasterKv, Key, KeyContext, ReadContext, RmwContext, Status, StoreConfig, UpsertContext, Value,
    ValueSlot,
};
use std::cell::Cell;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const BUCKETS: u64 = 1 << 18;
const LOG: LogConfig = LogConfig {
    page_bits: 25,
    memory_pages: 8,
    mutable_fraction: 0.5,
};

#[derive(Clone, Copy, Eq, PartialEq)]
struct BenchKey(u64);

// SAFETY: The key is an inline integer with stable bytes.
unsafe impl Key for BenchKey {
    fn hash(&self) -> u64 {
        (self.0 & (BUCKETS - 1)) | ((self.0 >> 18) << 48)
    }
}

#[repr(C)]
struct Payload<const N: usize> {
    word: AtomicU64,
    padding: [u64; N],
}

impl<const N: usize> Payload<N> {
    fn new(word: u64) -> Self {
        Self {
            word: AtomicU64::new(word),
            padding: [0; N],
        }
    }
}

// SAFETY: All fields are relocatable integers with stable allocation size.
unsafe impl<const N: usize> Value for Payload<N> {
    fn initialize_empty(slot: ValueSlot<'_, Self>) {
        slot.write(Self::new(0));
    }
}

#[derive(Clone)]
struct Request {
    key: BenchKey,
    output: u64,
    done: Rc<Cell<Option<(Status, u64)>>>,
}

// SAFETY: The context owns the fixed-size key.
unsafe impl KeyContext<BenchKey> for Request {
    fn key(&self) -> &BenchKey {
        &self.key
    }
}

// SAFETY: The complete value is initialized and in-place writes are declined.
unsafe impl<const N: usize> UpsertContext<BenchKey, Payload<N>> for Request {
    fn put(&mut self, slot: ValueSlot<'_, Payload<N>>) {
        slot.write(Payload::new(self.key.0 + 1));
    }
    fn put_atomic(&mut self, _: &Payload<N>) -> bool {
        false
    }
    fn on_complete(&mut self, status: Status) {
        self.done.set(Some((status, self.output)));
    }
}

impl<const N: usize> ReadContext<BenchKey, Payload<N>> for Request {
    fn get(&mut self, value: &Payload<N>) {
        self.output = value.word.load(Ordering::Relaxed);
    }
    fn on_complete(&mut self, status: Status) {
        self.done.set(Some((status, self.output)));
    }
}

// SAFETY: Initial and copy paths initialize the entire slot. Updates are atomic.
unsafe impl<const N: usize> RmwContext<BenchKey, Payload<N>> for Request {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, Payload<N>>) {
        self.output = 1;
        slot.write(Payload::new(self.output));
    }
    fn rmw_copy(&mut self, old: &Payload<N>, slot: ValueSlot<'_, Payload<N>>) {
        self.output = old.word.load(Ordering::Relaxed) + 1;
        slot.write(Payload::new(self.output));
    }
    fn rmw_atomic(&mut self, value: &Payload<N>) -> bool {
        self.output = value.word.fetch_add(1, Ordering::SeqCst) + 1;
        true
    }
    fn on_complete(&mut self, status: Status) {
        self.done.set(Some((status, self.output)));
    }
}

fn run<const N: usize>(root: &Path, records: u64, requests: u64, rmw: bool) {
    let cold =
        ColdStore::<BenchKey, Payload<N>>::open(&root.join("cold"), BUCKETS, LOG, LOG).unwrap();
    let mut request = Request {
        key: BenchKey(0),
        output: 0,
        done: Rc::default(),
    };
    {
        let mut session = cold.try_start_session().unwrap();
        for key in 0..records {
            request.key = BenchKey(key);
            let status = session.upsert(&mut request, key + 1);
            if status == Status::Pending {
                session.complete_pending(true);
                assert_eq!(request.done.take().unwrap().0, Status::Ok);
            } else {
                assert_eq!(status, Status::Ok);
            }
        }
    }
    let hot = FasterKv::open(StoreConfig {
        filepath: Some(root.join("hot")),
        index_buckets: BUCKETS,
        log: LOG,
        ..StoreConfig::default()
    })
    .unwrap();
    let f2 = F2Kv::new(hot, cold);
    let mut session = f2.try_start_session().unwrap();
    let mut samples = Vec::new();
    let mut pending = 0;
    let mut checksum = 0;
    let started = Instant::now();
    for i in 0..requests {
        let key = if rmw {
            (i * 17) & 8191
        } else {
            (i * 17) & 4095
        };
        request.key = BenchKey(key);
        request.output = 0;
        let sampled = (i & 15) == ((i >> 4) & 15);
        let clock = sampled.then(Instant::now);
        let status = if rmw {
            session.rmw(&mut request, i + 1)
        } else {
            session.read(&mut request, i + 1)
        };
        let (status, result) = if status == Status::Pending {
            pending += 1;
            assert!(session.complete_pending(true));
            request.done.take().unwrap()
        } else {
            (status, request.output)
        };
        if let Some(clock) = clock {
            samples.push(clock.elapsed().as_nanos());
        }
        assert_eq!(status, Status::Ok);
        assert_eq!(result, key + 1 + u64::from(rmw));
        checksum += result;
        if i & 255 == 255 {
            session.refresh();
        }
    }
    let elapsed = started.elapsed().as_nanos();
    samples.sort_unstable();
    println!(
        "{{\"elapsed_ns\":{elapsed},\"requests\":{requests},\"p99_ns\":{},\"pending\":{pending},\"checksum\":{checksum}}}",
        samples[samples.len() * 99 / 100]
    );
}

fn migrate(root: &Path) {
    type Store = F2Kv<BenchKey, Payload<511>>;
    let config = F2Config {
        hot_index_buckets: BUCKETS,
        cold_index_buckets: BUCKETS,
        hot_log: LOG,
        cold_log: LOG,
        cold_index_log: LOG,
        ..F2Config::new(root)
    };
    let mut store = Store::open(config.clone()).unwrap();
    let mut request = Request {
        key: BenchKey(0),
        output: 0,
        done: Rc::default(),
    };
    {
        let mut session = store.try_start_session().unwrap();
        for key in 0..81920 {
            request.key = BenchKey(key);
            assert_eq!(session.upsert(&mut request, key + 1), Status::Ok);
        }
    }
    let cutoff = Address::new(2, 0);
    assert!(store.hot_compaction_limit() >= cutoff);
    let started = Instant::now();
    let result = store.compact_hot(cutoff, 1).unwrap();
    let migration_ns = started.elapsed().as_nanos();
    assert_eq!(result.begin_address, cutoff);
    let checkpoint_started = Instant::now();
    let token = store.checkpoint().unwrap();
    let checkpoint_ns = checkpoint_started.elapsed().as_nanos();
    drop(store);
    let tails = ["cold/data/checkpoints", "cold/index/checkpoints"].map(|p| {
        let dirs = CheckpointDirs { root: root.join(p) };
        let mut entries = std::fs::read_dir(dirs.root.join("cpr-checkpoints")).unwrap();
        let path = entries.next().unwrap().unwrap().path();
        assert!(entries.next().is_none());
        LogMetadata::read(&path).unwrap().final_address.control() - 64
    });
    let mut store = Store::open(config).unwrap();
    let recover_started = Instant::now();
    assert_eq!(store.recover(token).unwrap().len(), 1);
    let recover_ns = recover_started.elapsed().as_nanos();
    let mut session = store.try_start_session().unwrap();
    let mut checksum = 0;
    for (i, key) in (0..81920).step_by(7).enumerate() {
        request.key = BenchKey(key);
        let status = session.read(&mut request, i as u64 + 1);
        let (status, result) = if status == Status::Pending {
            assert!(session.complete_pending(true));
            request.done.take().unwrap()
        } else {
            (status, request.output)
        };
        assert_eq!(status, Status::Ok);
        assert_eq!(result, key + 1);
        checksum += result;
    }
    println!(
        "{{\"migration_ns\":{migration_ns},\"checkpoint_ns\":{checkpoint_ns},\"recover_ns\":{recover_ns},\"data_bytes\":{},\"index_bytes\":{},\"checksum\":{checksum}}}",
        tails[0], tails[1]
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 4, "usage: f2_bench WORKLOAD DIRECTORY REQUESTS");
    let root = Path::new(&args[2]);
    let requests = args[3].parse().unwrap();
    assert!(requests > 0 && (args[1] != "rmw" || requests <= 8192));
    match args[1].as_str() {
        "migrate" => migrate(root),
        "resident" => run::<0>(root, 8192, requests, false),
        "index" => run::<0>(root, 1_200_000, requests, false),
        "both" => run::<63>(root, 1_200_000, requests, false),
        "rmw" => run::<63>(root, 1_200_000, requests, true),
        _ => panic!("unknown workload"),
    }
}
