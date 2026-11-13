//! Opens a persistent F2 store and increments a counter across checkpoints.
use raster::core::f2::{F2Config, F2Kv};
use raster::core::hlog::LogConfig;
use raster::core::{KeyContext, ReadContext, RmwContext, Status, ValueSlot};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct Counter {
    key: u64,
    value: u64,
}

// SAFETY: The request owns a complete fixed-size key.
unsafe impl KeyContext<u64> for Counter {
    fn key(&self) -> &u64 {
        &self.key
    }
}

// SAFETY: New slots are initialized completely and shared values use atomics.
unsafe impl RmwContext<u64, AtomicU64> for Counter {
    fn rmw_initial(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(1));
    }

    fn rmw_copy(&mut self, old: &AtomicU64, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(old.load(Ordering::Relaxed) + 1));
    }

    fn rmw_atomic(&mut self, current: &AtomicU64) -> bool {
        current.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
    }
}

impl ReadContext<u64, AtomicU64> for Counter {
    fn get(&mut self, value: &AtomicU64) {
        self.value = value.load(Ordering::Relaxed);
    }

    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, Status::Ok);
        println!("count = {}", self.value);
    }
}

fn main() {
    let root = std::env::args_os().nth(1).expect("usage: f2 DIRECTORY");
    let log = LogConfig {
        page_bits: 16,
        memory_pages: 4,
        mutable_fraction: 0.5,
    };
    let mut store = F2Kv::<u64, AtomicU64>::open(F2Config {
        hot_log: log,
        cold_log: log,
        cold_index_log: log,
        hot_index_buckets: 256,
        cold_index_buckets: 256,
        ..F2Config::new(root)
    })
    .expect("open F2 storage");
    match store.latest_checkpoint() {
        Ok(token) => {
            store.recover(token).expect("recover F2 checkpoint");
        }
        Err(Status::NotFound) => {}
        Err(status) => panic!("could not read F2 checkpoint: {status:?}"),
    }
    {
        let mut session = store.try_start_session().expect("start F2 session");
        let mut counter = Counter { key: 1, value: 0 };
        let status = session.rmw(&mut counter, 1);
        if status == Status::Pending {
            assert!(session.complete_pending(true));
        } else {
            assert_eq!(status, Status::Ok);
        }
        let status = session.read(&mut counter, 2);
        if status == Status::Pending {
            assert!(session.complete_pending(true));
        } else {
            ReadContext::on_complete(&mut counter, status);
        }
    }
    let token = store.checkpoint().expect("publish F2 checkpoint");
    println!("checkpoint {token}");
}
