# RasterKV

RasterKV is a Rust implementation of the FASTER embedded key-value store. It follows the FASTER C++ contract (session-level epoch protection with `refresh`, `Status`/`Pending`/`complete_pending`, value types responsible for in-place concurrency, CPR checkpoints, and inline 8-byte record headers). The performance target is parity with or better than FASTER C++ at commit `321d872eabda6a0345c8bd76419f89723ed864ae`.

The engine is being rebuilt from scratch. The previous implementation was removed; see the [rewrite plan](docs/05-FASTER%E5%A5%91%E7%BA%A6%E9%87%8D%E5%86%99%E8%AE%A1%E5%88%92.md) and [ADR 0001](docs/adr/0001-adopt-faster-cpp-contract.md) for the decision, phases, and acceptance gates.

- [Document index](docs/README.md)
- [Domain terminology](CONTEXT.md)
- [C++ comparison driver](tools/performance/README.md)

The toolchain and MSRV are **Rust 1.98.1**.

```sh
cargo test --locked
```
