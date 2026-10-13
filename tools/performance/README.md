# C++ comparison driver

`memory.cc` runs resident integer Read, Upsert, and RMW workloads directly against FASTER C++ at the pinned commit. The Rust counterpart (`examples/parity.rs`) must keep identical keys, hashing, values, refresh cadence, timing, sampling, and output fields so that both engines produce identical checksums and digests.

```sh
sh tools/performance/build.sh ../FASTER target/parity-build
target/parity-build/faster-memory read uniform 4 2000000 64
```

Arguments: `OP DISTRIBUTION THREADS COUNT SAMPLE_STRIDE`, where `OP` is `read|upsert|rmw`, `DISTRIBUTION` is `uniform|worker-hot|shared-hot`, and `THREADS` is `1|4`. `build.sh` requires libaio, libuuid, and TBB; set `CXX` to add include and library paths.

## Other pairs

Each C++ driver has a Rust counterpart under `examples/` with the same arguments and JSON output fields. Both sides must report identical business results.

| C++ driver | Rust example | Arguments |
|---|---|---|
| `disk.cc` | `disk.rs` | `OP THREADS KEYS COUNT SAMPLE_STRIDE DIR [READ_CACHE_MB]` |
| `checkpoint.cc` | `checkpoint.rs` | `run THREADS KEYS OPS DIR` or `recover THREADS KEYS TOKEN DIR` |
| `compact.cc` | `compact.rs` | `THREADS KEYS DIR` |
| `varlen.cc` | `varlen.rs` | `OP THREADS KEYS COUNT SAMPLE_STRIDE DIR MIN_VALUE MAX_VALUE` |

`disk` loads `KEYS` keys into a 256 MiB log window, so most of the log is on the device. `OP` is `read|rmw|hotread`; `hotread` sends 9 of 10 reads to the first `KEYS / 32` keys. A non-zero `READ_CACHE_MB` enables a read cache of that size, with 90% of it mutable. The C++ drivers link against `file_system_disk.h` with buffered I/O, so put `DIR` on tmpfs to compare engine overhead rather than storage.


## Variable-value pair

`varlen` supports read, upsert, and rmw over u64 keys and inline byte values.
MIN_VALUE and MAX_VALUE are payload bytes, multiples of eight, with a minimum
of eight bytes. Initial lengths are derived from the key hash. Upserts change
length by round and use disjoint worker key partitions so the final digest is
deterministic. RMW grows by eight bytes until MAX_VALUE, then updates in place.
Both engines use the same generation/replaced lock and atomic payload words.
Verification checks every payload word and includes logical length in the final
digest. P99 samples only synchronously completed operations, matching `disk`.

`varlen_run.py OUTDIR CPP_BIN RUST_BIN LOG_ROOT [resident|disk]` runs three
alternating rounds on CPU 4 or CPUs 4-7, NUMA node 0. It records binary hashes,
raw outputs, throughput, P99 and Pending counts. It refuses to record timing
summaries after a checksum/digest mismatch. The resident case uses 8192 keys
and requires zero Pending operations; the disk case uses 16 million keys with
a 256 MiB memory window. Both use payloads from 8 to 256 bytes.

For short resident comparisons, bind each worker before it enters application
code as well as restricting the process CPU mask. On Linux:

```sh
cc -O2 -std=c11 -Wall -Wextra -Werror -shared -fPIC -pthread \
  tools/performance/pin_threads.c -ldl -o target/libpin_threads.so
LD_PRELOAD="$PWD/target/libpin_threads.so" numactl --physcpubind=4,5,6,7 --membind=0 \
  target/release/examples/parity upsert uniform 4 2000000 64
```

Apply the identical preload to the baseline, candidate, and C++ processes.
The helper assigns threads round-robin within the inherited mask; it changes
neither the workload nor the frozen engine binary. Keep its source/library
hashes with the results and run a same-binary A/A control first. This helper is
only for isolated benchmark processes. Prior results without this control must
remain available rather than being replaced by successful reruns.

## Measurement and acceptance

The fixed Rust baseline is `746c562`; the C++ reference is
`321d872eabda6a0345c8bd76419f89723ed864ae`. Keep all binaries, source archives,
raw results and hash manifests from each candidate, including failed runs.
Host connection details and profiling artifacts stay in the ignored local
benchmark directory.

Resident, disk, cache, and variable-value throughput includes the complete
operation batch and Pending drain. P99 samples only synchronously completed
operations; it does not measure asynchronous completion latency. The resident
`retries` output is a placeholder, not an internal CAS retry counter. Checkpoint
and compaction timings include their completion and business verification is
performed separately. All current device comparisons use buffered I/O on
tmpfs; none establishes SSD or O_DIRECT parity.

The final resident cycle measurements use six alternating perf-stat pairs for
each case and 24 for `upsert/shared-hot/4`. Each perf-stat process includes
setup as well as the operation batch. Wall-clock throughput and P99 use the
three uninstrumented AB/BA rounds. Single perf-stat failures from earlier
candidates remain in the diagnostic record. Fixed workloads require at least
98% of baseline throughput; time metrics allow the reciprocal of that bound.
Variable workloads require throughput at least equal to C++ and P99 no higher
than C++.

Historical failures, diagnostic experiments, and measurement controls are
recorded in docs/06. The results below describe the frozen runtime source
commit `b76a975`.

## Frozen implementation `b76a975`

The runtime sources match the frozen benchmark source archive byte for byte.
Formatting, Clippy, debug/release tests and 30 repetitions of all 11 test
binaries passed (96 tests passed, one manual timing test ignored).
Resident fixed-size, device, cache, compaction and all 12 variable-value gates
passed. The nine-round checkpoint matrix also meets the numerical thresholds,
but checkpoint acceptance remains open: all three tested affinity configurations
failed the same-binary A/A control. Passing candidate timings do not settle that
gate without a stable control. All raw results and frozen artifact hashes are
retained locally; the evidence archive and its seven artifact hashes were verified.

### Resident fixed-size workloads

| Workload | Rust/base throughput | Rust/C++ throughput | C++/Rust P99 (ns) | Rust/base cycles |
| --- | ---: | ---: | ---: | ---: |
| read/uniform/1 | 0.997 | 3.042 | 101/56 | 1.002 |
| read/uniform/4 | 1.032 | 2.822 | 130/63 | 0.993 |
| read/worker-hot/1 | 1.062 | 3.262 | 102/52 | 0.958 |
| read/worker-hot/4 | 1.061 | 3.257 | 102/51 | 0.956 |
| read/shared-hot/1 | 1.062 | 3.196 | 101/52 | 0.958 |
| read/shared-hot/4 | 1.061 | 3.195 | 101/51 | 0.957 |
| upsert/uniform/1 | 1.039 | 2.230 | 115/71 | 0.970 |
| upsert/uniform/4 | 1.030 | 2.171 | 177/130 | 0.987 |
| upsert/worker-hot/1 | 1.080 | 2.341 | 111/63 | 0.945 |
| upsert/worker-hot/4 | 1.081 | 2.349 | 112/63 | 0.944 |
| upsert/shared-hot/1 | 1.079 | 2.339 | 111/64 | 0.945 |
| upsert/shared-hot/4 | 1.043 | 1.351 | 719/734 | 0.947 |
| rmw/uniform/1 | 1.029 | 2.153 | 111/69 | 0.992 |
| rmw/uniform/4 | 1.086 | 1.979 | 160/128 | 0.977 |
| rmw/worker-hot/1 | 1.043 | 2.198 | 107/63 | 0.977 |
| rmw/worker-hot/4 | 1.044 | 2.189 | 108/64 | 0.975 |
| rmw/shared-hot/1 | 1.042 | 2.167 | 107/64 | 0.977 |
| rmw/shared-hot/4 | 1.018 | 1.286 | 696/578 | 1.005 |

For `upsert/shared-hot/4`, the baseline comparison recorded 734/745 ns
(base/Rust); the independent C++ comparison recorded 719/734 ns (C++/Rust).
The throughput gates pass, but this workload retains a small P99 gap.

### Fixed-size device and cache workloads

Workload labels encode operation, thread count and cache capacity in MiB.

| Workload (cache MiB) | C++ Mops/s | Rust Mops/s | Rust/base throughput | Rust/C++ throughput | C++/base/Rust P99 (ns) |
| --- | ---: | ---: | ---: | ---: | ---: |
| read-1-0 | 0.509 | 1.155 | 1.015 | 2.267 | 706/636/640 |
| read-4-0 | 1.523 | 3.566 | 1.011 | 2.342 | 698/660/667 |
| rmw-1-0 | 0.464 | 1.007 | 1.011 | 2.169 | 755/678/678 |
| rmw-4-0 | 1.392 | 3.187 | 0.995 | 2.289 | 801/735/744 |
| hotread-1-0 | 0.275 | 0.661 | 1.025 | 2.400 | 796/711/702 |
| hotread-4-0 | 0.766 | 1.984 | 1.083 | 2.592 | 810/718/718 |
| hotread-1-256 | 1.140 | 2.163 | 1.009 | 1.898 | 623/582/580 |
| hotread-4-256 | 3.569 | 7.427 | 1.037 | 2.081 | 626/587/577 |

### Variable-value workloads

| Workload | C++ Mops/s | Rust Mops/s | Throughput ratio | C++/Rust P99 (ns) | P99 ratio | C++/Rust Pending |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| resident/read/1 | 6.288 | 11.115 | 1.768 | 180/147 | 0.817 | 0/0 |
| resident/read/4 | 25.083 | 44.270 | 1.765 | 204/151 | 0.740 | 0/0 |
| resident/upsert/1 | 3.026 | 3.870 | 1.279 | 466/412 | 0.884 | 0/0 |
| resident/upsert/4 | 13.071 | 17.522 | 1.341 | 508/438 | 0.862 | 0/0 |
| resident/rmw/1 | 5.217 | 7.407 | 1.420 | 393/323 | 0.822 | 0/0 |
| resident/rmw/4 | 14.174 | 20.814 | 1.468 | 962/617 | 0.641 | 0/0 |
| disk/read/1 | 0.212 | 0.320 | 1.514 | 787/766 | 0.973 | 1829328/1829328 |
| disk/read/4 | 0.609 | 1.039 | 1.707 | 834/759 | 0.910 | 1829265/1829265 |
| disk/upsert/1 | 2.131 | 2.296 | 1.077 | 738/721 | 0.977 | 0/0 |
| disk/upsert/4 | 5.798 | 6.914 | 1.192 | 1061/833 | 0.785 | 0/0 |
| disk/rmw/1 | 0.193 | 0.289 | 1.496 | 1162/1008 | 0.867 | 1863676/1840527 |
| disk/rmw/4 | 0.569 | 0.938 | 1.647 | 1216/1038 | 0.854 | 1863853/1840662 |

### Compaction

| Metric | C++ | Baseline | Rust | Rust/base ratio | Rust/C++ ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| compact (ms) | 7559.913 | 1985.308 | 1948.441 | 0.981 | 0.258 |

### Checkpoint and recovery

| Metric | C++ | Baseline | Rust | Rust/base ratio | Rust/C++ ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| mops | 10.100 | 16.158 | 16.388 | 1.014 | 1.623 |
| checkpoint (ms) | 492.002 | 527.573 | 469.087 | 0.889 | 0.953 |
| mops_during | 8.163 | 10.365 | 10.179 | 0.982 | 1.247 |
| recover (ms) | 412.473 | 262.963 | 263.741 | 1.003 | 0.639 |

Checkpoint acceptance remains open because the same-binary controls failed.
These candidate measurements alone do not override that measurement failure.

### Instruction-count diagnostic

Callgrind includes process initialization. The normalized difference divides
that total difference by 400,000 operations; it is not a pure hot-loop count.
These diagnostics supplement the throughput, cycle and latency measurements.

| Workload | Baseline instructions | Rust instructions | Difference per operation |
| --- | ---: | ---: | ---: |
| read/uniform | 345272689 | 342493127 | -6.95 |
| upsert/uniform | 374520010 | 370872843 | -9.12 |
| rmw/worker-hot | 369327332 | 366478036 | -7.12 |

### Checkpoint measurement controls

Each control used the same frozen baseline binary for both labels, with 12
alternating rounds. The ratios below are B/A medians. The third experiment also
pinned the coordinator to CPU 7; that experimental affinity helper was not adopted.

| Affinity configuration | Overall throughput | Checkpoint duration | Throughput during checkpoint | Recovery duration |
| --- | ---: | ---: | ---: | ---: |
| Per-thread CPU affinity | 1.007 | 1.105 | 1.035 | 1.001 |
| Process CPU mask | 1.040 | 1.029 | 0.924 | 1.117 |
| Per-thread affinity with pinned coordinator | 0.986 | 1.070 | 1.018 | 1.000 |

All three controls failed. These results do not establish a scheduling root cause
or exclude a candidate regression. Final checkpoint acceptance requires a stable
control followed by an interleaved comparison of the same frozen binaries; the
original thresholds remain unchanged.
