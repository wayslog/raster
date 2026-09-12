# Direct-engine performance comparisons

`memory.cc` and `examples/parity.rs` run the same resident integer read,
upsert, and RMW workloads. Build C++ against the pinned upstream commit with
`build.sh`, then run `compare.py`. It checks independent expected results and
requires Rust throughput of at least 90% of C++ and P99 of at most 110%.

```sh
cargo build --locked --release --example parity
sh tools/performance/build.sh ../FASTER target/parity-build
python3 tools/performance/compare.py --rust target/release/examples/parity \
  --cpp target/parity-build/faster-memory --output target/parity-comparison
```

For a same-host Rust regression comparison, replace `--cpp` with
`--baseline-rust` and set `--min-throughput 0.98`. Output directories must be new.
Measured pairs alternate after an unmeasured warmup; raw output and binary hashes
are retained even when a threshold fails.

## Resident append and scan regression checks

`examples/parity_scan.rs` inserts unique integer keys and scans every physical
record eight times, verifying values, order, flags, and end-of-scan behavior.
`compare_scan.py` compares identical drivers built against two Rust versions.
It independently checks the checksum and checks append throughput, scan
throughput, and sampled scan P99. It does not establish C++ scan parity.

```sh
cargo build --locked --release --example parity_scan
python3 tools/performance/compare_scan.py \
  --baseline target/preserved/parity_scan \
  --candidate target/release/examples/parity_scan \
  --output target/scan-comparison
```

The `cpp-parity` workflow runs these additional regression checks when given
`baseline_ref`, using the current scan driver with each version of the engine.
An optional `scan_baseline_ref` adds a second full-commit scan baseline on the
same runner. Use it to verify that a later change also closes an older scan
regression, while retaining the immediate baseline comparison and all results.
The full performance objective also includes delete, variable values,
over-memory I/O, checkpoint/recovery, compaction, and automatic maintenance.
Passing these resident cases alone does not complete that objective.
