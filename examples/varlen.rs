//! Variable-value benchmark paired with tools/performance/varlen.cc.
//! Usage: varlen OP THREADS KEYS COUNT SAMPLE_STRIDE DIR MIN_VALUE MAX_VALUE
use raster::core::device::FileDevice;
use raster::core::hlog::LogConfig;
use raster::core::*;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const LOCKED: u64 = 1 << 62;
const REPLACED: u64 = 1 << 63;
const FILL: u64 = 0x5858_5858_5858_5858;
#[repr(C)]
struct BytesValue {
    generation: AtomicU64,
    capacity: u32,
    length: AtomicU32,
}
// SAFETY: Headers use integers/atomics only; initialized inline payloads are
// relocatable, resource-free, and retain their allocation size.
unsafe impl Value for BytesValue {
    #[inline(always)]
    fn size(&self) -> u32 {
        16 + self.capacity
    }
    fn initialize_empty(slot: ValueSlot<'_, Self>) {
        Self::initialize(slot, 0, 0);
    }
}
impl BytesValue {
    #[inline(always)]
    fn word(&self, index: usize) -> &AtomicU64 {
        // SAFETY: all benchmark values have capacity / 8 initialized inline words.
        unsafe { &*(self as *const Self).add(1).cast::<AtomicU64>().add(index) }
    }
    fn initialize(mut slot: ValueSlot<'_, Self>, counter: u64, length: u32) {
        let capacity = slot.len() - 16;
        let ptr = slot.as_mut_ptr();
        // SAFETY: exclusive slot contains the header and capacity inline bytes.
        unsafe {
            ptr.write(Self {
                generation: AtomicU64::new(0),
                capacity,
                length: AtomicU32::new(length),
            });
            for i in 0..capacity as usize / 8 {
                ptr.add(1)
                    .cast::<AtomicU64>()
                    .add(i)
                    .write(AtomicU64::new(if i == 0 { counter } else { FILL }));
            }
        }
    }
    fn lock(&self) -> bool {
        loop {
            let current = self.generation.load(Ordering::SeqCst);
            if current & REPLACED != 0 {
                return false;
            }
            if current & LOCKED == 0
                && self
                    .generation
                    .compare_exchange(
                        current,
                        current | LOCKED,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return true;
            }
            std::hint::spin_loop();
        }
    }
    fn unlock(&self, replaced: bool) {
        let current = self.generation.load(Ordering::SeqCst);
        self.generation.store(
            ((current & !LOCKED) + 1) | if replaced { REPLACED } else { 0 },
            Ordering::SeqCst,
        );
    }
}
#[derive(Clone)]
struct Request {
    key: u64,
    input: u64,
    output: u64,
    length: u32,
    max: u32,
    verify: bool,
    sum: Rc<Cell<u64>>,
}
impl Request {
    fn add_output(&self) {
        self.sum.set(self.sum.get().wrapping_add(self.output));
    }
    fn read_value(&mut self, value: &BytesValue) {
        loop {
            let before = value.generation.load(Ordering::SeqCst);
            if before & LOCKED != 0 {
                std::hint::spin_loop();
                continue;
            }
            let length = value.length.load(Ordering::SeqCst);
            let counter = value.word(0).load(Ordering::SeqCst);
            if self.verify {
                for i in 1..length as usize / 8 {
                    assert_eq!(value.word(i).load(Ordering::SeqCst), FILL);
                }
            }
            if before == value.generation.load(Ordering::SeqCst) {
                self.output = counter.wrapping_add((length as u64).rotate_left(32));
                return;
            }
        }
    }
}
// SAFETY: The context owns a fixed-size integer key with stable bytes;
// default hashing, equality and copying use that complete field.
unsafe impl KeyContext<u64> for Request {
    #[inline(always)]
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadContext<u64, BytesValue> for Request {
    fn get(&mut self, value: &BytesValue) {
        self.read_value(value);
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.add_output();
    }
}
// SAFETY: All reserved value bytes are initialized and capacity stays fixed;
// shared payload updates use the generation lock and atomic fields.
unsafe impl UpsertContext<u64, BytesValue> for Request {
    fn value_size(&self) -> u32 {
        16 + self.length
    }
    fn put(&mut self, slot: ValueSlot<'_, BytesValue>) {
        BytesValue::initialize(slot, self.input, self.length);
        self.output = self
            .input
            .wrapping_add((self.length as u64).rotate_left(32));
    }
    fn put_atomic(&mut self, value: &BytesValue) -> bool {
        if !value.lock() {
            return false;
        }
        if self.length > value.capacity {
            value.unlock(true);
            return false;
        }
        value.word(0).store(self.input, Ordering::SeqCst);
        for i in 1..self.length as usize / 8 {
            value.word(i).store(FILL, Ordering::SeqCst);
        }
        value.length.store(self.length, Ordering::SeqCst);
        value.unlock(false);
        self.output = self
            .input
            .wrapping_add((self.length as u64).rotate_left(32));
        true
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.add_output();
    }
}
// SAFETY: All reserved value bytes are initialized and capacity stays fixed;
// shared payload updates use the generation lock and atomic fields.
unsafe impl RmwContext<u64, BytesValue> for Request {
    fn initial_value_size(&self) -> u32 {
        16 + self.length
    }
    fn copy_value_size(&self, old: &BytesValue) -> u32 {
        16 + (old.capacity + 8).min(self.max)
    }
    fn rmw_initial(&mut self, slot: ValueSlot<'_, BytesValue>) {
        BytesValue::initialize(slot, self.input, self.length);
        self.output = self.input;
    }
    fn rmw_copy(&mut self, old: &BytesValue, slot: ValueSlot<'_, BytesValue>) {
        let length = slot.len() - 16;
        self.output = old.word(0).load(Ordering::SeqCst).wrapping_add(self.input);
        BytesValue::initialize(slot, self.output, length);
    }
    fn rmw_atomic(&mut self, value: &BytesValue) -> bool {
        if !value.lock() {
            return false;
        }
        if value.capacity < self.max {
            value.unlock(true);
            return false;
        }
        self.output = value
            .word(0)
            .fetch_add(self.input, Ordering::SeqCst)
            .wrapping_add(self.input);
        value.unlock(false);
        true
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        self.add_output();
    }
}
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
fn length(key: u64, min: u32, max: u32) -> u32 {
    (min + (key.hash() % (max - min + 1) as u64) as u32) & !7
}
#[derive(Default)]
#[repr(align(64))]
struct WorkerResult {
    elapsed: u64,
    sum: u64,
    pending: u64,
    samples: Vec<u64>,
}
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        8,
        "usage: varlen OP THREADS KEYS COUNT SAMPLE_STRIDE DIR MIN_VALUE MAX_VALUE"
    );
    let operation = args[0].as_str();
    let threads: usize = args[1].parse().unwrap();
    let keys: u64 = args[2].parse().unwrap();
    let count: usize = args[3].parse().unwrap();
    let stride: usize = args[4].parse().unwrap();
    let min: u32 = args[6].parse().unwrap();
    let max: u32 = args[7].parse().unwrap();
    assert!(matches!(operation, "read" | "upsert" | "rmw"));
    assert!(
        threads > 0
            && keys > 0
            && count > 0
            && keys.is_multiple_of(threads as u64)
            && count.is_multiple_of(threads)
            && stride.is_power_of_two()
    );
    assert!(
        min >= 8
            && min <= max
            && max < (1 << 25) - 32
            && min.is_multiple_of(8)
            && max.is_multiple_of(8)
    );
    let store = FasterKv::<u64, BytesValue>::with_device(
        StoreConfig {
            index_buckets: 1 << 22,
            filepath: None,
            pre_allocate: true,
            read_cache: None,
            auto_compaction: None,
            log_size_budget: 0,
            checkpoint_dir: None,
            log: LogConfig {
                page_bits: 25,
                memory_pages: 8,
                mutable_fraction: 0.5,
            },
        },
        Arc::new(FileDevice::open_with(&args[5], 30, 8, false).unwrap()),
    );
    let ignored = Rc::new(Cell::new(0));
    {
        let mut session = store.start_session();
        for key in 0..keys {
            let mut ctx = Request {
                key,
                input: key,
                output: 0,
                length: length(key, min, max),
                max,
                verify: false,
                sum: ignored.clone(),
            };
            assert_eq!(session.upsert(&mut ctx, key), Status::Ok);
            if key & 255 == 255 {
                session.refresh();
            }
        }
        session.complete_pending(true);
    }
    let enrolled = AtomicUsize::new(0);
    let results: Vec<WorkerResult> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|worker| {
                let (store, enrolled) = (&store, &enrolled);
                scope.spawn(move || {
                    let mut session = store.start_session();
                    let sum = Rc::new(Cell::new(0u64));
                    let mut result = WorkerResult {
                        samples: Vec::with_capacity(count / threads / stride + 1),
                        ..Default::default()
                    };
                    let mut state = (worker as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    enrolled.fetch_add(1, Ordering::SeqCst);
                    while enrolled.load(Ordering::SeqCst) != threads {
                        std::thread::yield_now();
                    }
                    let started = Instant::now();
                    for i in 0..count / threads {
                        let random = next(&mut state);
                        let key = if operation == "upsert" {
                            worker as u64 + random % (keys / threads as u64) * threads as u64
                        } else {
                            random % keys
                        };
                        let round = i as u64 / (keys / threads as u64) + 1;
                        let mut ctx = Request {
                            key,
                            input: if operation == "upsert" { key } else { 1 },
                            output: 0,
                            length: length(key.wrapping_add(round), min, max),
                            max,
                            verify: false,
                            sum: sum.clone(),
                        };
                        let sampled = (i & (stride - 1)) == ((i / stride) & (stride - 1));
                        let clock = sampled.then(Instant::now);
                        let status = match operation {
                            "read" => session.read(&mut ctx, i as u64),
                            "upsert" => session.upsert(&mut ctx, i as u64),
                            _ => session.rmw(&mut ctx, i as u64),
                        };
                        match status {
                            Status::Ok => {
                                ctx.add_output();
                                if let Some(clock) = clock {
                                    result.samples.push(clock.elapsed().as_nanos() as u64);
                                }
                            }
                            Status::Pending => result.pending += 1,
                            other => panic!("unexpected status {other:?}"),
                        }
                        if i & 255 == 255 {
                            session.complete_pending(false);
                        }
                    }
                    session.complete_pending(true);
                    result.elapsed = started.elapsed().as_nanos() as u64;
                    result.sum = sum.get();
                    result
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let (mut elapsed, mut sum, mut pending, mut samples) = (0u64, 0u64, 0u64, Vec::new());
    for result in results {
        elapsed = elapsed.max(result.elapsed);
        sum = sum.wrapping_add(result.sum);
        pending += result.pending;
        samples.extend(result.samples);
    }
    samples.sort_unstable();
    let digest = Rc::new(Cell::new(0u64));
    let mut session = store.start_session();
    for key in 0..keys {
        let mut ctx = Request {
            key,
            input: 0,
            output: 0,
            length: min,
            max,
            verify: true,
            sum: digest.clone(),
        };
        match session.read(&mut ctx, key) {
            Status::Ok => ctx.add_output(),
            status => assert_eq!(status, Status::Pending),
        }
        if key & 255 == 255 {
            session.complete_pending(false);
        }
    }
    session.complete_pending(true);
    println!(
        "{{\"engine\":\"rust\",\"operation\":\"{operation}\",\"threads\":{threads},\"keys\":{keys},\"count\":{count},\"elapsed_ns\":{elapsed},\"p99_ns\":{},\"samples\":{},\"pending\":{pending},\"checksum\":{sum},\"digest\":{}}}",
        samples.get(samples.len() * 99 / 100).copied().unwrap_or(0),
        samples.len(),
        digest.get()
    );
}
