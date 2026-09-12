# Fixed upstream behavior executor

This is a P9 acceptance tool, not a RasterKV runtime dependency or public product interface. See the [Phase I acceptance report](../../docs/acceptance/%E4%B8%80%E6%9C%9F%E9%AA%8C%E6%94%B6%E6%8A%A5%E5%91%8A.md) for measured differences. The executor uses MIT-licensed Microsoft FASTER from a fixed commit in a standalone checkout; its engine sources are not copied into this repository. Variable-length key contexts follow the shallow-key and deep-key API used by upstream `cc/test/in_memory_test.cc`.

On Linux, install `g++`, `libaio-dev`, `libtbb-dev`, and `uuid-dev`, then build the fixed upstream commit:

```sh
sh tools/upstream/build.sh ../FASTER target/p9-build
python3 -B tools/upstream/compare.py --cpp target/p9-build/faster-replay --corrected-cpp target/p9-build/faster-replay-retained --output target/p9-comparison
```

The output directory must be new. The executor saves the input, Rust and C++ results, build and run logs, and line-by-line differences. The forced-tombstone difference from the original upstream remains recorded; the corrected executable supplies the reference for the confirmed contract. A normal executor exit does not by itself prove that both implementations agree.

`compare.py --disk` runs Rust and C++ with their own native file backends. The automated and manual workflows named "P9 random contract comparison" run the null backend and two file groups with the same bounds and five seeds for 7,508 steps. The original and corrected C++ output, Rust output, and all diffs are archived.

A direct `faster-replay input.trace output.results` run uses the null backend. `--disk NEW_CATALOG` selects the Linux `libaio` file backend. The upstream `ThreadPoolIoHandler` exists only on Windows, so Linux control runs use a worker-thread backend that is intentionally different from the RasterKV macOS and Linux implementation.

The upstream page is fixed at 32 MiB. File comparison uses 256 MiB, a 0.4 variable-length ratio, at least two mutable pages, and four immutable pages. A small file track does not necessarily reach the disk region; verify `Pending` output and an over-memory workload. Every external process has a deadline. Do not resubmit an already accepted request.

RasterKV execution ends with:

```sh
cargo test --locked --test p9_upstream -- --nocapture
```

The fixed trajectory runs by default. Environment variables only select acceptance inputs, result paths, and recovery locations; they are not part of the RasterKV public API:

| Variable | Meaning |
| --- | --- |
| `RASTER_UPSTREAM_GENERATED` | Set to `1` and require byte-for-byte equality with the fixed SplitMix64 trajectory. |
| `RASTER_UPSTREAM_TRACE` | Input trajectory; defaults to `p0.trace`. |
| `RASTER_UPSTREAM_RESULT` | Result output path. |
| `RASTER_UPSTREAM_ROOT` | New root directory for the native file backend; defaults to the null backend. |
| `RASTER_UPSTREAM_SPLIT` | Checkpoint after this many steps, then close, recover, and resume the session. |
| `RASTER_UPSTREAM_CHECKPOINT` | `full` or `pair`; the latter uses index plus log pairing. |
| `RASTER_UPSTREAM_TIMEOUT` | Seconds for one checkpoint and boundary close; default 60, maximum 600. It does not change the engine recovery budget. |
| `RASTER_UPSTREAM_SEGMENT_BYTES` | RasterKV file segment size; default 1 MiB. Long lifecycle runs use 32 MiB while the page window remains four 32 KiB pages. |

The same-business upstream trajectory, the Rust reference model, concurrency history, and protocol-failure tests are separate evidence levels. Native random, disk/recovery comparison, and the two-platform fault matrix cover different boundaries. See the acceptance report for the current result.

## CI upstream contract and lifecycle comparison

CI builds `faster-replay` and `faster-replay-retained`; the latter changes only index-removal conditions when `force_tombstone` is false. It archives the forced-tombstone diff and JSON, source and patch SHA-256 values, and the unchanged upstream checkout. Randomized control requires exact row-by-row equality with the corrected copy and archives original upstream output and all differences. Without `--corrected-cpp`, the original strict control is used and every difference fails. `probe.json` reports only the fixed-bound approval contract and does not replace random or lifecycle acceptance.

## Checkpoint and hybrid-log lifecycle

```sh
python3 -B tools/upstream/lifecycle.py --cpp target/p9-build/faster-replay --output target/p9-lifecycle
```

The lifecycle tool sends the same file to both execution ends at fixed full or index-plus-log boundaries, closes the instance, restores it, and resumes three sessions. C++ verifies the identity and timing of each persistence callback, status, and serial number; it starts the next action only after every session has returned. Each operation result is compared, and any difference fails.

The upstream checkpoint scenario uses a 2,048-bucket index, while the original short trajectory keeps 128 buckets. Upstream splits the index into 256 blocks and requires each block to satisfy 512-byte sector alignment, so 128 buckets do not meet the checkpoint prerequisites. The first native run therefore fails on that assertion; the test configuration is corrected without changing upstream algorithms or assertions.

`scenarios.py` defines three scene groups: 141 small trajectories for full and index-plus-log checkpoints, and large trajectories with 32,768 steps including at least 16 KiB of append upserts, cold keys, variable-length RMW, tombstones, and post-recovery reads and writes. Large trajectories must produce more than 512 MiB of actual upstream log span and non-zero read, RMW, and pending output. The generator records steps, recovery boundaries, write load, and SHA-256; input size is not a substitute for actual log span.

Each process has a 1,200-second deadline. RasterKV checkpoint waits have a 600-second deadline, C++ checkpoints 120 seconds, and one pending request 60 seconds. Rust runs in release mode. Output includes the complete input, results, logs, and synthesis storage directory on failure; CI uploads it and cleans up after success. A small upstream trajectory that never enters `Pending` is still recorded truthfully. This lifecycle covers only the fixed scenarios and does not replace the independent 7,508-step random contract comparison.
