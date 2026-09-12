# Failure and full-history acceptance

`fault-matrix.json` lists 40 test entry points and their verification boundaries. The combinations inside each entry are defined by the test code. The matrix covers admission, user callbacks, short I/O, cancellation and close, checkpoint publication and interruption, recovery, garbage collection, recovery-set release, multi-worker coordination, and automatic maintenance. It also runs 32 complete disk-history and recovery groups and distinguishes internal value-permission contention from user-visible busy errors.

```sh
python3 -B tools/acceptance/run_faults.py --output target/p9-faults
```

The output directory must be new. The script builds all-feature tests, verifies with `--list` that every selected test name exists exactly once, runs each test with `--exact --nocapture`, and requires one real pass with zero failures and zero ignored tests. An empty filter is rejected. Artifacts contain raw output, the test list, matrix results, and 32 full-history groups. Builds and subprocesses have deadlines and return a non-zero exit status on error.

The 40 entries are not 40 individual failure points. The user-callback matrix contains 11 valid error and panic combinations. Three checkpoint matrices interrupt every runtime I/O event, and the release matrix is also exercised event by event. Expected panics are part of the input under test; the test result and command exit status determine acceptance. Process interruption and synchronized-data or namespace power-loss models are verified separately; this is not a real hardware power outage.

This matrix does not replace the independent reference model, the same-business upstream trajectory, performance checks, or long-term resource checks. See the [Phase I acceptance report](../../docs/acceptance/%E4%B8%80%E6%9C%9F%E9%AA%8C%E6%94%B6%E6%8A%A5%E5%91%8A.md) for current status. CI runs natively on Linux and macOS and preserves itemized evidence.

## Same-process resource cycle

The `resource_cycles` example warms up eight rounds and measures 64 rounds in one process. It runs a full disk lifecycle and two recoveries per round and checks fixed allocation, RSS, handle, and thread-growth thresholds. The native workflow records the environment, one CSV row per round, and the final status on both platforms.

```sh
gh run download RUN_ID --dir target/p9-resources-native
python3 -B tools/acceptance/audit_resources.py --sha FULL_SHA --run-id RUN_ID target/p9-resources-native
```

Replace `RUN_ID` and `FULL_SHA` with the workflow run number and full commit SHA. The audit verifies the commit, native target, final status of both jobs, real test and business counts, consecutive rounds, and all configured thresholds, then writes a new `audit.json`. It refuses to overwrite an existing audit. Allocation peaks are recorded for diagnosis and do not change the growth threshold fixed before the run. See the [resource acceptance record](../../docs/acceptance/P9.2%E8%B5%84%E6%BA%90%E4%B8%8E%E6%80%A7%E8%83%BD%E9%AA%8C%E6%94%B6.md).

## Miri memory-safety check

This machine can run Miri on nightly Rust; formal project acceptance still uses Rust 1.98.1.

```sh
python3 -B tools/acceptance/run_miri.py --output target/p9-miri
```

The script records the interpreter version and whether the commit and workspace are clean. It runs each group and verifies the real pass count. Page ranges and preallocation, built-in value layout, record lifetime, cache behavior, the acceptance counting allocator, and result-budget ownership cover 40 cases with default leak detection. The deliberate `forget` lease case runs separately with `-Zmiri-ignore-leaks` for access-safety checks only. The matrix excludes native file backends and does not replace dual-platform failure, recovery, or concurrent-interleaving evidence.

## Complete performance matrix

```sh
RASTER_BENCH_OUTPUT=target/p9-benchmark cargo run --locked --release --all-features --example benchmark
python3 -B tools/acceptance/audit_benchmark.py target/p9-benchmark
```

The matrix parameters are fixed before the run in the [performance record](../../docs/acceptance/P9.2%E5%AE%8C%E6%95%B4%E6%80%A7%E8%83%BD%E7%9F%A9%E9%98%B5.md). The default contains 48 groups covering atomic integers and variable bytes, uniform and per-thread hotspot access, one and four threads, memory-only and over-memory storage, and three rounds per group. Each group verifies 16,384 business operations step by step and then checks every key and session progress after full recovery. The independent audit reinterprets eight inputs, checks the complete matrix and I/O, and recomputes the amplification denominator; one passing group cannot satisfy the full audit.

`benchmark.yml` is a workflow-dispatch job on Linux and macOS. The benchmark exit status only proves that business and measurement structures passed; it does not resolve the separately recorded P3 performance regression. The output and audit are never overwritten, and a failed run retains its storage directory.

After downloading both native products, run:

```sh
python3 -B tools/acceptance/audit_benchmark.py --sha FULL_SHA --run-id RUN_ID target/p9-benchmark-native
```

The native audit checks job status, commit, target, the three real tests, all 48 result groups, every CSV and fixed input, and the recalculated result against the native `audit.json`.

## Synchronous-write allocation comparison

`audit_ready_comparison.py` recomputes the eight-group median from the [synchronous-write allocation record](../../docs/acceptance/P9.2%E5%90%8C%E6%AD%A5%E5%86%99%E5%85%A5%E5%88%86%E9%85%8D%E4%BC%98%E5%8C%96.md), checks all 192 samples, fixed operands, and file hashes, and retains the first sample that triggers review. Partial improvement does not make the old P3 benchmark pass overall.

```sh
python3 -B tools/acceptance/audit_ready_comparison.py docs/acceptance/data/p9-ready-experiment
```

## Result-budget reuse comparison

The [result-budget reuse record](../../docs/acceptance/P9.2%E7%BB%93%E6%9E%9C%E9%A2%84%E7%AE%97%E5%A4%8D%E7%94%A8.md) keeps the complete short matrices, binary calibration, and longer hotspot comparisons. The audit never merges or deletes samples that triggered review.

```sh
python3 -B tools/acceptance/audit_result_pool.py docs/acceptance/data/p9-result-pool-experiment
```

## Serial-number precheck comparison

```sh
python3 -B tools/acceptance/audit_serial_precheck.py docs/acceptance/data/p9-serial-precheck
```

The audit performs 228 recalculations across eight complete groups and two batches of single-threaded hotspot supplements while retaining negative scenarios. The old P3 result is reviewed separately.

## Synchronous-path cost diagnostics

```sh
python3 -B tools/acceptance/audit_cost_attribution.py docs/acceptance/data/p9-cost-attribution
```

The diagnosis attributes only the single-threaded resident-memory loop. It verifies the final value, acceptance progress, and group medians and does not treat removal-protocol variants as functional acceptance. Sampling timing is separate from normal timing.

## Variable-record boundary comparison

```sh
python3 -B tools/acceptance/audit_mutable_boundary.py docs/acceptance/data/p9-mutable-boundary
```

The audit reuses the same matrix format for all 228 successful cases and recomputes the median. The old P3 result remains a separate review item.

## Original P3 performance review materials

```sh
python3 -B tools/acceptance/audit_performance_review.py docs/acceptance/data/p9-performance-review
```

The audit recomputes 192 samples across eight same-machine groups, 56 restricted-loop samples, configured assignments, and three real tests that failed after protocol removal. It keeps the 25% and 30% review thresholds, prints how many groups still trigger review, and presents data consistency separately from the acceptance explanation.

## Native paired benchmark review

`paired_benchmark.py` executes two built binaries on the same host in AB/BA and BA/AB order. `--interleaved-control` adds a third tag for the same old binary, and six permutations balance process position. Fixed inputs, operation results, full-key recovery, and deduplicated compression are checked continuously. Any throughput ratio below 0.75 or P99 ratio above 1.30 returns non-zero; a later green run cannot replace an earlier red result.

```sh
python3 -B tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/focus
python3 -B tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/remaining
python3 -B tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/control
python3 -B tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/interleaved
```

Each audit recomputes the median from round CSV files and complete fixed inputs, checks the original review judgment, and requires matching A/A binary SHA-256 values. Approval means that materials and calculations are consistent; it does not erase the original numerical regression. Diagnostic workflow source is archived in each group, while the mainline 48-group workflow remains unchanged.
