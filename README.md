# RasterKV

RasterKV is a Rust embedded key-value store that reproduces the functional and behavioral baseline of FASTER C++ while using an independent disk format. The current implementation includes shared storage and sessions, four operations, a hybrid log with disk pending requests, three checkpoint and recovery modes, online index growth, a read cache, physical scans, compaction, and automatic maintenance.

Phase I implementation and delivery acceptance are complete for P0 through P9. Ordinary blind deletion and safe index removal follow the upstream behavior; forced tombstone reachability is an explicit compatibility rule. The performance review preserves the measured throughput and tail-latency gap of the earlier memory prototype and records the limits of short macOS samples. It does not claim parity with the old prototype or with C++ performance.

- [Phase I implementation plan](docs/14-RasterKV%E7%AC%AC%E4%B8%80%E6%9C%9F%E5%AE%9E%E7%8E%B0%E8%AE%A1%E5%88%92%E5%9C%B0%E5%9B%BE.md): task status and the next execution entry.
- [Phase I acceptance report](docs/acceptance/%E4%B8%80%E6%9C%9F%E9%AA%8C%E6%94%B6%E6%8A%A5%E5%91%8A.md): upstream comparison, full history, failures, and remaining differences.
- [Performance review and limitations](docs/acceptance/P9.2%E6%80%A7%E8%83%BD%E5%A4%8D%E6%A0%B8%E7%BB%93%E8%AE%BA.md): same-machine comparison and measurement basis.
- [Public interface](docs/10-RasterKV%E5%85%AC%E5%BC%80%E6%8E%A5%E5%8F%A3.md) and [usage flow](docs/16-%E5%85%AC%E5%BC%80%E6%8E%A5%E5%8F%A3%E4%BD%BF%E7%94%A8%E6%B5%81%E7%A8%8B.md): signatures, results, errors, and runnable examples.
- [Configuration and diagnostics](docs/15-%E9%85%8D%E7%BD%AE%E4%B8%8E%E8%AF%8A%E6%96%AD.md), [architecture](docs/09-RasterKV%E6%A8%A1%E5%9D%97%E8%AE%BE%E8%AE%A1.md), [module access map](docs/13-%E6%A8%A1%E5%9D%97%E5%9F%BA%E7%A1%80%E9%AA%A8%E6%9E%B6.md), and [state protocol](docs/11-RasterKV%E5%86%85%E5%AD%98%E4%B8%8E%E7%8A%B6%E6%80%81%E5%8D%8F%E8%AE%AE.md).
- [Document index](docs/README.md), [Phase I scope and dependency selection](docs/05-RasterKV%E7%AC%AC%E4%B8%80%E9%98%B6%E6%AE%B5%E4%B8%8E%E6%94%AF%E6%8C%81%E5%BA%93%E9%80%89%E5%9E%8B.md), and [domain terminology](CONTEXT.md).

The toolchain and MSRV are **Rust 1.98.1**. The supported native file backends for this phase are Linux and macOS. Windows, io_uring, remote devices, F2/ColdIndex, cross-store compaction, and external async runtimes remain follow-up work. All feature combinations compile; compilation does not make the io_uring backend available. Publishing to crates.io is currently disabled.

```sh
cargo test --locked --all-features
cargo run --locked --release --example memory
cargo run --locked --release --example interface
cargo run --locked --release --example disk_lifecycle
cargo run --locked --release --example automatic
```

The memory example uses the null backend and does not provide disk spilling or persistence. The disk example creates an independent temporary directory; the explicitly supplied directory must not already exist and is retained after success. CI checks formatting, Clippy, unit and integration tests, examples, strict rustdoc, and the default, `config-toml`, and all-feature builds on Linux and macOS. Acceptance workflows cover failures, resources, upstream comparison, and performance matrices.

`Pending` and `Ready` describe result delivery. Completion, draining, and shutdown do not replace checkpoint persistence. An index checkpoint does not promise session persistence progress; a log checkpoint must bind the submitted index material, and a recovered session must be explicitly resumed. Scans return owned physical records and may include old versions and duplicate keys; log span is not the number of valid keys. See the v1 [independent format specification](docs/acceptance/%E7%A3%81%E7%9B%98%E6%A0%BC%E5%BC%8F%E8%A7%84%E8%8C%83.md) and [recovery safety record](docs/acceptance/P5.4%E6%81%A2%E5%A4%8D%E5%AE%89%E5%85%A8%E4%BA%A4%E4%BB%98%E8%AE%B0%E5%BD%95.md).
