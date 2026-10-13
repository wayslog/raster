//! Minimal fixed-size store usage: cargo run --example basic.
use raster::core::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct Request {
    key: u64,
    value: u64,
}
impl KeyContext<u64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertContext<u64, AtomicU64> for Request {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.value));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.value, Ordering::Relaxed);
        true
    }
}
impl ReadContext<u64, AtomicU64> for Request {
    fn get(&mut self, value: &AtomicU64) {
        self.value = value.load(Ordering::Relaxed);
    }
}
fn main() {
    let store = FasterKv::<u64, AtomicU64>::new(StoreConfig {
        index_buckets: 64,
        log: raster::core::hlog::LogConfig {
            page_bits: 12,
            memory_pages: 8,
            mutable_fraction: 0.5,
        },
        ..Default::default()
    });
    let mut session = store.start_session();
    let mut request = Request { key: 7, value: 42 };
    assert_eq!(session.upsert(&mut request, 1), Status::Ok);
    request.value = 0;
    assert_eq!(session.read(&mut request, 2), Status::Ok);
    assert_eq!(request.value, 42);
    session.refresh();
    println!("value={}", request.value);
}
