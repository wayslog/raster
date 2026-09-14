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

## Focused single-thread controls

When a short comparison's identical-program control fails, use
`focused_compare.py` to diagnose the same case with longer measurement windows.
It pins its Linux process to one allowed CPU; benchmark processes and their
worker threads inherit that mask. It executes all six orders of baseline,
candidate, and an identical-baseline control, with 20 million operations per
process by default. Affinity inheritance follows the Linux
[process](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html) and
[thread](https://man7.org/linux/man-pages/man3/pthread_setaffinity_np.3.html) rules;
it applies to internal threads as well as the requested worker. Each program has
an unmeasured warmup. Raw results, commands,
binary hashes, the CPU mask, and every measured round are retained.

```sh
python3 tools/performance/focused_compare.py \
  --baseline target/preserved/parity \
  --candidate target/release/examples/parity \
  --case upsert/shared-hot/1 --count 20000000 \
  --output target/focused-comparison
```

The candidate keeps the existing 0.98 throughput and 1.10 P99 limits. The
identical control additionally uses reciprocal bounds: throughput must be between
0.98 and 1/0.98, and P99 between 1/1.10 and 1.10. An implausibly faster control
is also unstable. A failed control makes the diagnostic fail even when the
candidate is faster. Per-round ratios are retained alongside the median verdict.
Longer windows and CPU affinity do not guarantee stable measurements;
retain the earlier failures and inspect the control before interpreting a delta.
Only single-thread cases are accepted, so this tool cannot silently serialize a
multi-thread workload. `--allow-unpinned --count 16384` is available for local
smoke validation on platforms without affinity; its timing is not native evidence.

The `cpp-focused` workflow mode runs this diagnostic on Ubuntu when supplied a
`baseline_ref` and a single-thread `case`. It preserves the existing `cpp-parity`
matrix and lifecycle gates. A passing focused result does not establish C++
parity or replace failed full-scope measurements.

The native workflow preserves source hashes, executable hashes, and integer-driver
disassembly under `native-code/`. Use a baseline with identical runtime source to
check build and timing variation before attributing unexplained changes to an
engine optimization. Preserve all failed measurements alongside those controls.

Build both timed programs with the same Cargo command, features, profile, and
compiler options. Obtain assembly by disassembling the exact frozen executables.
Do not substitute a program rebuilt with `cargo rustc -- --emit=asm,link` for one
built with ordinary `cargo build`: the extra compiler options can change the
generated program. If the build recipe changes, rebuild both sides and repeat
the comparison; record their hashes before interpreting the result.

## Retained result-budget diagnosis

`result_budget` uses public APIs and the serialized integer layout. A blocking
Read callback makes other reads return Pending; the driver then completes those
requests but retains their tickets while timing synchronous reads. Setup, pending
completion, final ticket collection, and shutdown are outside the timed interval.
Every retained output is checked after timing, including its one-time delivery.

```sh
cargo build --locked --release --all-features --example result_budget
python3 tools/performance/compare_result_budget.py \
  --baseline-rust target/preserved/result_budget \
  --rust target/release/examples/result_budget \
  --output target/result-budget-comparison
```

The default cases retain 0, 32, or 960 completed results and perform two million
reads, with 1,024 warmup reads per process and one latency sample per 64 reads.
Each case includes three unmeasured warmup processes and all six orders of the
baseline, candidate, and identical-baseline control. The comparator validates
both timed and retained checksums, the final accepted serial, and sample counts.
It uses the same candidate and reciprocal-control limits as focused comparisons.
Failed controls invalidate their case even if its candidate ratio passes.

The native `cpp-parity` workflow builds this same driver for both Rust versions
and preserves the additional results under `result-budget/`. This diagnoses Rust
result ownership overhead; it does not replace the C++ matrix or lifecycle gates.
For a historical primary baseline that still holds its operation stripe across
a REST Read callback, supply `result_baseline_ref` with a compatible full commit
SHA, such as `74d4d4dc5093337173ddf7c318681a8a87a725d3`. That input selects only
the additional retained-result comparison; it does not replace the primary or
retained memory baselines. Incompatible setup must fail without producing a
performance sample.

Use workflow mode `result-budget` with `baseline_ref` to rerun just these three
retained-result cases and their interleaved controls after a targeted correction.
The build recipes and output validation are unchanged. This shorter diagnostic
does not run or replace the complete `cpp-parity` regression and lifecycle gates.

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
