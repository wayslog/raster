# C++ comparison driver

`memory.cc` runs resident integer Read, Upsert, and RMW workloads directly against FASTER C++ at the pinned commit. The Rust counterpart (`examples/parity.rs`) must keep identical keys, hashing, values, refresh cadence, timing, sampling, and output fields so that both engines produce identical checksums and digests.

```sh
sh tools/performance/build.sh ../FASTER target/parity-build
target/parity-build/faster-memory read uniform 4 2000000 64
```

Arguments: `OP DISTRIBUTION THREADS COUNT SAMPLE_STRIDE`, where `OP` is `read|upsert|rmw`, `DISTRIBUTION` is `uniform|worker-hot|shared-hot`, and `THREADS` is `1|4`. `build.sh` requires libaio, libuuid, and TBB; set `CXX` to add include and library paths.
