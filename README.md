# RasterKV

RasterKV is a Rust implementation of the FASTER single-store contract: inline fixed-size and variable-size records, session-level epoch protection, Pending operations, CPR fold-over checkpoints, recovery, compaction, index growth, and an optional read cache. In-place concurrency belongs to the value type. The C++ reference is commit `321d872eabda6a0345c8bd76419f89723ed864ae`.

Rust **1.98.1** is required. The file device supports Unix development hosts; benchmark acceptance runs on Linux. Configuration parsing uses Serde and TOML.

```sh
cargo test --locked
cargo test --locked --release
cargo run --locked --example basic
cargo run --locked --example variable
```

The smallest operation context implements `KeyContext`, plus the operation traits it needs:

```rust
use raster::core::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct Put { key: u64, value: u64 }
impl KeyContext<u64> for Put {
    fn key(&self) -> &u64 { &self.key }
}
impl UpsertContext<u64, AtomicU64> for Put {
    fn put(&mut self, slot: ValueSlot<'_, AtomicU64>) {
        slot.write(AtomicU64::new(self.value));
    }
    fn put_atomic(&mut self, value: &AtomicU64) -> bool {
        value.store(self.value, Ordering::Relaxed);
        true
    }
}
```

A session protects log memory until its next `refresh`. Call `refresh` periodically and use `complete_pending` to finish asynchronous operations. A Pending operation owns a clone of its context; consume its result through `on_complete`, not the original context. Log keys and values are bytewise-relocatable data and never run destructors. Variable types report their allocation size and initialize their inline payload through `ValueSlot`.

- [Minimal fixed-size example](examples/basic.rs)
- [Minimal variable-value example](examples/variable.rs)
- [API and configuration contract](docs/03-功能与API契约.md)
- [Architecture and terminology](CONTEXT.md)
- [Implementation and acceptance plan](docs/06-变长键值与收尾计划.md)
- [Benchmark pairs and results](tools/performance/README.md)
- [Document index](docs/README.md)

F2 and a native asynchronous O_DIRECT backend remain deferred. Azure tiered storage is outside scope. Buffered tmpfs benchmarks do not establish real SSD performance parity. Current acceptance evidence and any remaining gates are recorded in the plan.
