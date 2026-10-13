//! Compaction benchmark paired with tools/performance/compact.cc.
//! Usage: compact THREADS KEYS DIR
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone)]
struct Request {
    key: u64,
    input: u64,
    sum: Rc<Cell<u64>>,
}
impl KeyContext<u64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertContext<u64, AtomicU64> for Request {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.input));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.input, Ordering::SeqCst);
        true
    }
}
impl ReadContext<u64, AtomicU64> for Request {
    fn get(&mut self, value: &AtomicU64) {
        self.input = value.load(Ordering::Relaxed);
    }
    fn get_atomic(&mut self, value: &AtomicU64) {
        self.input = value.load(Ordering::SeqCst);
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.sum.set(self.sum.get() + self.input);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 3, "usage: compact THREADS KEYS DIR");
    let threads: usize = args[0].parse().unwrap();
    let keys: u64 = args[1].parse().unwrap();
    let device = Arc::new(FileDevice::open_with(&args[2], 30, 8, false).unwrap());
    let store = FasterKv::<u64, AtomicU64>::with_device(
        StoreConfig {
            index_buckets: 1 << 22,
            log_size_budget: 0,
            checkpoint_dir: None,
            log: LogConfig {
                page_bits: 25,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        device,
    );
    let sum = Rc::new(Cell::new(0u64));
    let mut session = store.start_session();
    let request = |key, input| Request {
        key,
        input,
        sum: sum.clone(),
    };
    for key in 0..keys {
        assert_eq!(session.upsert(&mut request(key, key), key), Status::Ok);
        if key & 255 == 255 {
            session.refresh();
        }
    }
    for key in (0..keys).step_by(2) {
        assert_eq!(
            session.upsert(&mut request(key, key + 1), keys + key),
            Status::Ok
        );
        if key & 511 == 510 {
            session.refresh();
        }
    }
    session.complete_pending(true);
    let begin = store.hlog.begin.load();
    let until = store.hlog.safe_read_only.load();
    let size_before = store.log_size();
    let started = Instant::now();
    session
        .compact(until, true, threads, false)
        .expect("compaction succeeds");
    let compact_ns = started.elapsed().as_nanos();
    let size_after = store.log_size();
    for key in 0..keys {
        let mut read = request(key, 0);
        match session.read(&mut read, 2 * keys + key) {
            Status::Ok => sum.set(sum.get() + read.input),
            status => assert_eq!(status, Status::Pending),
        }
        if key & 255 == 255 {
            session.complete_pending(false);
        }
    }
    session.complete_pending(true);
    let expected = keys * (keys - 1) / 2 + keys.div_ceil(2);
    println!(
        "{{\"engine\":\"rust\",\"threads\":{threads},\"keys\":{keys},\"compact_ns\":{compact_ns},\"compacted_bytes\":{},\"size_before\":{size_before},\"size_after\":{size_after},\"sum\":{},\"expected\":{expected}}}",
        store.hlog.device_offset(until) - store.hlog.device_offset(begin),
        sum.get()
    );
    assert_eq!(sum.get(), expected);
}
