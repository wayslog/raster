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
Likewise, `memory_baseline_ref` adds a second full-commit memory baseline with
the current direct driver. Both memory comparisons use the same case filter
and thresholds, and still run when the C++ parity step fails.
With `baseline_ref`, the workflow also compares repeated variable-memory traces
with an identical-baseline control and runs the three paired lifecycle scenarios
with the same control and six execution orders.
It overlays the same benchmark, generator, and oracle sources on the baseline.
The lifecycle step requires throughput of at least 0.98 and P99 of at most 1.10
relative to that baseline; the older tool's looser review flag is not the gate.
The full performance objective also includes delete, variable values,
over-memory I/O, checkpoint/recovery, compaction, and automatic maintenance.
Passing these resident cases alone does not complete that objective.

The native workflow preserves source hashes, executable hashes, and integer-driver
disassembly under `native-code/`. Use a baseline with identical runtime source to
check build and timing variation before attributing unexplained changes to an
engine optimization. Preserve all failed measurements alongside those controls.

## Repeated variable-memory diagnosis

`benchmark_probe` repeats the unchanged 16,384-operation variable-value hotspot
trajectory on fresh stores. It reuses the original worker and operation callbacks,
checks all results and all 2,048 final keys per repetition, and rejects data I/O,
Pending, or retries. Each repetition resets the store, so increasing the repeat
count preserves the original value-growth pattern. Timing excludes setup and final
key verification. Checkpoint and recovery coverage stays with `benchmark`.

```sh
cargo build --locked --release --all-features --example benchmark_probe
target/release/examples/benchmark_probe target/repeated-memory 32
python3 tools/performance/compare_repeat.py \
  --baseline target/preserved/benchmark_probe \
  --candidate target/release/examples/benchmark_probe \
  --repetitions 32 --output target/repeated-comparison
```

Build both runtime versions with identical probe, callback, generator, and oracle
sources. The comparator executes six permutations of the baseline, candidate, and
an identical-baseline control. It verifies the fixed input hash, row counts,
operation/key counts, and timing sums, then applies the existing 0.98 throughput
and 1.10 overall P99 limits. Preserve short-window failures alongside this evidence.

The JSON field `engine_ns` sums the original worker's `execute` intervals, including
its API adapter and user callbacks; it is not exclusive engine CPU time. Wall time
also includes the worker's result checks and bookkeeping. `operation_time_ratios`
are candidate duration divided by baseline duration, so lower values are faster.
This diagnostic does not establish C++ parity or replace lifecycle acceptance.
