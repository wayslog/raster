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

`disk` loads `KEYS` keys into a 256 MiB log window, so most of the log is on the device. `OP` is `read|rmw|hotread`; `hotread` sends 9 of 10 reads to the first `KEYS / 32` keys. A non-zero `READ_CACHE_MB` enables a read cache of that size, with 90% of it mutable. The C++ drivers link against `file_system_disk.h` with buffered I/O, so put `DIR` on tmpfs to compare engine overhead rather than storage.
