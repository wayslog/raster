# C++ comparison driver

Latest acceptance: the [second premerge repair](#second-premerge-repair-2026-10-29-checkpoint-gate-failed)
passed the six failed Read cases but stopped at the checkpoint throughput gate.
Earlier passing matrices below apply only to their named frozen runtimes.

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

`oracle.cc` independently computes the expected request checksum and final
digest for the disk and variable-value pairs without importing either engine.
It replays the deterministic key sequence, counts each key's RMW updates, and
tracks logical lengths for variable-value writes. RMW sums are independent of
completion order. Run this correctness-only model outside timed measurements
and compare both fields with every driver's output. It supplements the full
readback and payload checks already performed by the drivers.

```sh
c++ -std=c++17 -O2 -Wall -Wextra -Werror tools/performance/oracle.cc -o target/business-oracle
target/business-oracle fixed read 4 16000000 2000000
target/business-oracle varlen rmw 4 16000000 2000000 8 256
```

Arguments are `fixed|varlen OP THREADS KEYS COUNT [MIN_VALUE MAX_VALUE]`.
For fixed values, `OP` is `read|hotread|rmw`; variable values use
`read|upsert|rmw`. These are business expectations, not timing results.


## Variable-value pair

`varlen` supports read, upsert, and rmw over u64 keys and inline byte values.
MIN_VALUE and MAX_VALUE are payload bytes, multiples of eight, with a minimum
of eight bytes. Initial lengths are derived from the key hash. Upserts change
length by round and use disjoint worker key partitions so the final digest is
deterministic. RMW grows by eight bytes until MAX_VALUE, then updates in place.
Both engines use the same generation/replaced lock and atomic payload words.
Verification checks every payload word and includes logical length in the final
digest. P99 measures submission through completion, including Pending callbacks,
matching `disk`. Both drivers assert that every selected sample completes and
report `pending_samples` separately. Historical synchronous-only samples are retained in the evidence archive;
they are not end-to-end latency evidence.

`varlen_run.py OUTDIR CPP_BIN RUST_BIN LOG_ROOT [resident|disk] [IO_THREADS]` runs
three alternating rounds with the mandatory role binding described below. The
default Rust I/O pool has eight threads; pass `1` for a separate sensitivity
experiment. It records binary and tool hashes, raw outputs, verified affinities,
throughput, end-to-end P99 and Pending sample counts. It refuses to record timing
summaries after a checksum/digest mismatch. The resident case uses 8192 keys
and requires zero Pending operations; the disk case uses 16 million keys with
a 256 MiB memory window. Both use payloads from 8 to 256 bytes.

The disk and variable-value drivers drain Pending requests every 256 submitted
operations and once more at the end. End-to-end P99 includes time waiting for
that callback processing; this progress cadence is part of the paired workload.
Rust buffered reads and writes execute in the submitting thread. Its I/O pool
handles direct reads and writes, while the C++ buffered driver uses libaio submission.
The one-thread sensitivity run changes the Rust pool only; it does not make
the two backends identical. Do not attribute buffered-read throughput to eight
parallel Rust read workers. The tmpfs measurements compare engine overhead,
not SSD or O_DIRECT performance.

Build the Linux role-binding helper and require it in every runner:

```sh
cc -O2 -std=c11 -Wall -Wextra -Werror -shared -fPIC -pthread \
  tools/performance/pin_threads.c -ldl -o target/libpin_threads.so
export RASTER_BENCH_PIN_LIBRARY="$PWD/target/libpin_threads.so"
python3 tools/performance/varlen_run.py RESULTS CPP_BIN RUST_BIN /dev/shm resident
```

`affinity.py` binds each workload worker to CPU 4-7, the main/coordinator thread
to CPU 8 and auxiliary/I/O threads to CPU 9, NUMA 0. Confirm that these six CPUs
are allowed, distinct physical cores before running. It loads the helper only
after numactl establishes the full mask. The frozen startup contract is explicit:
Rust device drivers create their configured I/O pool before the workers; the
memory driver and C++ drivers create workers first. Recovery has no workers.
Compaction has three spawned workers and one working coordinator. Library
receipts verify each actual affinity and the expected worker count; any missing
configuration or receipt fails the run. Changing thread creation requires
revalidating this contract. This helper is only for isolated benchmark processes.

## Final closeout, 2026-10-19

The final frozen runtime is pre-consolidation commit `49f882d`. Acceptance follows
`docs/08`, including sections 8 and 9. Rust baseline: `746c562`; C++ reference:
`321d872e`. Only checkpoint comparisons use the explicitly approved private
reference with `cpp-pending-retry.patch`; the upstream checkout is unchanged.
The original binary, livelock evidence, failures and diagnostic variants remain
preserved. No diagnostic driver variant was used for this matrix.

The frozen private closeout controllers and evidence are retained under
`.perf-baseline/closeout/section8-20261019/`. That continuation reused the first
216 valid samples. The final continuation
under `.perf-baseline/closeout/checkpoint-final-20261019/` reused all 753 retained
outputs, reassessed the existing 64 control pairs, and ran only the unstarted
36-round checkpoint comparison (228 new run/recovery outputs including warmups).
Source, binary, binding and controller
hashes are verified. The standalone historical runners retain their earlier
gates; the closeout controller applies the user-approved docs/08 policy.
The frozen C++ resident binary omits `pending` while enforcing synchronous
Status::Ok internally. Only that output may omit the field; an explicit nonzero
value is rejected, and Rust resident output must still report zero. The fix
was verified by replaying retained output before resuming any measurements.

Resident workloads use six balanced three-engine orders; disk, hotread and
variable-value workloads use three alternating rounds. Compaction uses one
warmup per label and six orders. Checkpoint uses two warmups per label, 64
same-binary pairs, then 36 balanced three-engine rounds. Bootstrap uses seed
20261014 and 20,000 order-stratified resamples. A/A requires all four 95% intervals
to lie within [0.98, 1/0.98]; they need not contain one, and there is no separate
interval-width gate. The original contains-one failure remains preserved.
Candidate/base checkpoint speed
interval lower bounds must be at least 0.98. C++ median speed ratios must be at
least one. No samples were added or excluded after observing results.

H1-H5 passed under this policy. Throughput/base ratios below 0.95 would stop
acceptance; 0.95-0.98 is recorded as a known deviation. P99/base is recorded,
not gated. C++ P99 is gated except for the two explicitly listed shared-hot
four-thread updates. All device P99 values include Pending completion. Buffered
tmpfs results describe engine overhead, not SSD, O_DIRECT or io_uring parity.
Rust buffered reads and writes execute on the submitting thread; the configured
eight-thread pool does not imply eight asynchronous buffered readers.

### Complete matrix

Speed ratios are candidate/reference; P99 ratios are duration ratios. Compaction
speed is reference duration/candidate duration. Missing baseline fields for
variable values are intentional. See docs/07 for historical A/A widths and
known P99 deviations, including differences predating the review repairs.

| Workload | Speed/C++ | P99/C++ | Speed/base | P99/base | P99 policy |
|---|---:|---:|---:|---:|---|
| memory/read/uniform/1 | 3.16041 | 0.53465 | 1.02501 | 0.98182 | gate |
| memory/read/uniform/4 | 2.86489 | 0.51292 | 1.05206 | 1.02963 | gate |
| memory/read/worker-hot/1 | 3.30748 | 0.50495 | 1.07974 | 0.98077 | gate |
| memory/read/worker-hot/4 | 3.30888 | 0.54545 | 1.07979 | 0.98077 | gate |
| memory/read/shared-hot/1 | 3.25963 | 0.50000 | 1.08198 | 0.98077 | gate |
| memory/read/shared-hot/4 | 3.22042 | 0.50495 | 1.07102 | 0.98077 | gate |
| memory/upsert/uniform/1 | 2.22460 | 0.59483 | 1.03197 | 1.01471 | gate |
| memory/upsert/uniform/4 | 2.17431 | 0.72303 | 1.02076 | 1.04202 | gate |
| memory/upsert/worker-hot/1 | 2.33376 | 0.57399 | 1.07669 | 1.01587 | gate |
| memory/upsert/worker-hot/4 | 2.33360 | 0.57399 | 1.07583 | 1.01587 | gate |
| memory/upsert/shared-hot/1 | 2.33343 | 0.57658 | 1.07614 | 1.01587 | gate |
| memory/upsert/shared-hot/4 | 1.36777 | 1.03544 | 1.03522 | 1.01247 | record_only |
| memory/rmw/uniform/1 | 2.11536 | 0.61607 | 1.01519 | 1.00000 | gate |
| memory/rmw/uniform/4 | 1.85085 | 0.85802 | 0.99627 | 1.13008 | gate |
| memory/rmw/worker-hot/1 | 2.16399 | 0.59813 | 1.02736 | 1.01587 | gate |
| memory/rmw/worker-hot/4 | 2.15832 | 0.58879 | 1.02410 | 1.00000 | gate |
| memory/rmw/shared-hot/1 | 2.13286 | 0.59346 | 1.02851 | 1.00794 | gate |
| memory/rmw/shared-hot/4 | 1.32886 | 0.83489 | 1.04678 | 0.99828 | record_only |
| disk/read/0/1 | 2.24621 | 0.19330 | 1.00701 | 1.01162 | gate |
| disk/read/0/4 | 2.45151 | 0.30797 | 1.01854 | 0.99916 | gate |
| disk/rmw/0/1 | 2.13777 | 0.20518 | 1.00797 | 1.01230 | gate |
| disk/rmw/0/4 | 2.33543 | 0.29453 | 1.00543 | 1.00886 | gate |
| hotread/hotread/0/1 | 2.36917 | 0.25550 | 1.02021 | 1.00468 | gate |
| hotread/hotread/0/4 | 2.61467 | 0.32186 | 1.01368 | 1.00724 | gate |
| hotread/hotread/256/1 | 2.04019 | 0.23672 | 1.00563 | 1.00775 | gate |
| hotread/hotread/256/4 | 2.24524 | 0.34511 | 1.01100 | 1.00390 | gate |
| varlen-resident/read/0/1 | 1.67768 | 0.99245 | - | - | gate |
| varlen-resident/read/0/4 | 1.72920 | 0.77273 | - | - | gate |
| varlen-resident/upsert/0/1 | 1.27681 | 0.87002 | - | - | gate |
| varlen-resident/upsert/0/4 | 1.32923 | 0.85798 | - | - | gate |
| varlen-resident/rmw/0/1 | 1.39045 | 0.83571 | - | - | gate |
| varlen-resident/rmw/0/4 | 1.45629 | 0.67215 | - | - | gate |
| varlen-disk/read/0/1 | 1.51654 | 0.56205 | - | - | gate |
| varlen-disk/read/0/4 | 1.68610 | 0.80110 | - | - | gate |
| varlen-disk/upsert/0/1 | 1.07417 | 0.97835 | - | - | gate |
| varlen-disk/upsert/0/4 | 1.06955 | 0.84337 | - | - | gate |
| varlen-disk/rmw/0/1 | 1.49566 | 0.58237 | - | - | gate |
| varlen-disk/rmw/0/4 | 1.64629 | 0.80459 | - | - | gate |
| compact/compact/0/4 | 3.81385 | - | 0.99464 | - | gate |

### Checkpoint

Speed/base uses the median paired log speed ratio. Speed/C++ uses the ratio
of the two engines' metric medians. Duration metrics are inverted.

| Metric | Speed/C++ | Speed/base | Base 95% interval | A/A 95% interval |
|---|---:|---:|---:|---:|
| mops | 1.65846 | 1.00007 | 0.99312-1.00858 | 0.99307-1.00128 |
| checkpoint_ns | 1.03288 | 1.00043 | 0.99897-1.00221 | 0.99957-1.00261 |
| mops_during | 1.29880 | 0.99479 | 0.99270-0.99954 | 0.99558-1.00202 |
| recover_ns | 1.52170 | 0.99940 | 0.99884-1.00004 | 0.99906-0.99998 |

### Shared-hot P99 record-only cases

These two exceptions follow docs/08 section 8. The diagnostic timing variant
was not adopted. Their throughput gates remain enforced.

| Workload | Candidate P99/ns | C++ P99/ns | Base P99/ns | P99/C++ |
|---|---:|---:|---:|---:|
| memory/upsert/shared-hot/4 | 730.5 | 705.5 | 721.5 | 1.03544 |
| memory/rmw/shared-hot/4 | 581.5 | 696.5 | 582.5 | 0.83489 |

Historical results and rejected candidates are preserved in the original branch,
backup tags and `.perf-baseline/closeout/notes/`. They are not acceptance evidence
for a different source build. F2, real-disk/io_uring comparisons and Azure remain
outside this closeout. No push was performed.

## PR1 premerge validation, 2026-10-29: stopped

Acceptance under docs/09 is incomplete. Correctness repairs are `d594403`
(shared GC/recovery boundary validation) and `5535671` (reject sessions and
recovery retries after an index-load-stage failure). The sole permitted hot-field
repair is `de1c56f`; it remains local and failed the throughput gate. No push,
merge, second repair, added samples, or diagnostic workload was performed.

At `de1c56f`, fmt, Clippy with `-D warnings`, Debug/Release (144 runtime and
11 compile-fail tests each), seven Miri checks, and 13 release binaries x 30
rounds passed. The three checkpoint stress processes each completed 29 of 200
rounds: 87 passes, zero test failures, and three in-flight processes cancelled
at the performance stop. This does not satisfy the 600-run requirement. Earlier
stress results from `5535671` are separate and cannot be pooled with these.

### Initial frozen runtime: 5535671

Each resident case has six balanced three-engine orders. C++ is `321d872e`,
Rust baseline is `746c562`. H2, applicable H3, and business checks passed;
six Read cases failed the 0.98 baseline-throughput gate. These results do not
constitute a complete matrix for `de1c56f`.

| Workload | Speed/C++ | P99/C++ | Speed/base | P99/base | Result |
|---|---:|---:|---:|---:|---|
| memory/read/uniform/1 | 2.63035 | 0.54455 | 0.85350 | 1.00000 | throughput failed |
| memory/read/uniform/4 | 2.47807 | 0.56504 | 0.90963 | 1.03731 | throughput failed |
| memory/read/worker-hot/1 | 2.75167 | 0.50980 | 0.89687 | 1.00000 | throughput failed |
| memory/read/worker-hot/4 | 2.75063 | 0.51232 | 0.89496 | 1.00000 | throughput failed |
| memory/read/shared-hot/1 | 2.68707 | 0.51232 | 0.89728 | 1.00000 | throughput failed |
| memory/read/shared-hot/4 | 2.68368 | 0.51232 | 0.89387 | 1.00000 | throughput failed |
| memory/upsert/uniform/1 | 2.21403 | 0.60944 | 1.02450 | 1.04412 | passed |
| memory/upsert/uniform/4 | 2.28836 | 0.68908 | 1.06934 | 1.03361 | passed |
| memory/upsert/worker-hot/1 | 2.30405 | 0.58036 | 1.06182 | 1.03175 | passed |
| memory/upsert/worker-hot/4 | 2.28603 | 0.58036 | 1.06487 | 1.03175 | passed |
| memory/upsert/shared-hot/1 | 2.31769 | 0.58559 | 1.06795 | 1.03175 | passed |
| memory/upsert/shared-hot/4 | 1.29521 | 1.09670 | 1.00043 | 1.05079 | P99 record only |
| memory/rmw/uniform/1 | 2.07587 | 0.61607 | 0.99525 | 1.00000 | passed |
| memory/rmw/uniform/4 | 2.01518 | 0.78683 | 1.07075 | 1.05462 | passed |
| memory/rmw/worker-hot/1 | 2.12541 | 0.61215 | 1.00961 | 1.03150 | passed |
| memory/rmw/worker-hot/4 | 2.12828 | 0.59535 | 1.00957 | 1.01587 | passed |
| memory/rmw/shared-hot/1 | 2.09310 | 0.59813 | 1.00939 | 1.01587 | passed |
| memory/rmw/shared-hot/4 | 1.26116 | 0.84342 | 0.98045 | 1.01630 | P99 record only |

All four fresh 64-pair A/A intervals fit [0.98, 1/0.98]. The subsequent
36-round comparison used two warmups per label, seed 20261014, and 20,000
order-stratified bootstrap samples. All recovery checks passed. Overall
throughput failed H5; the other three H5 metrics and all four C++ metrics passed.

| Metric | Speed/C++ | Speed/base | Base 95% interval | A/A 95% interval |
|---|---:|---:|---:|---:|
| mops | 1.67502 | 0.98445 | 0.979413-0.989311 | 0.993424-1.007872 |
| checkpoint_ns | 1.04591 | 1.00236 | 1.001788-1.003889 | 0.996746-1.002853 |
| mops_during | 1.31262 | 0.99110 | 0.988694-0.993740 | 0.995792-1.002940 |
| recover_ns | 1.50804 | 0.99231 | 0.991321-0.994520 | 0.998936-1.000972 |

### Sole repair: de1c56f

Hot tail, frame pointers, page geometry, begin/head/read_only are inline again.
Page ownership and asynchronously updated safe boundaries remain shared so that
moving or dropping a handle does not invalidate callbacks. Exclusive recovery
is preserved. The first failed resident workload was retested with six orders:

| Workload | Speed/C++ | P99/C++ | Speed/base | P99/base | Result |
|---|---:|---:|---:|---:|---|
| memory/read/uniform/1 | 2.89729 | 0.52941 | 0.93900 | 0.98182 | failed; stopped |

The baseline ratio improved from 0.85350 to 0.93900, still below 0.98. The
remaining five failed Read cases and the repaired checkpoint comparison were
not run after this stop. The approved A/A was retained without rerunning it.

The initial 816 engine outputs and the repair's 18 outputs were archived,
SHA256-verified, and independently replayed for business, affinity, and statistics.
Evidence: `.perf-baseline/pr1-premerge-20261029/` and
`.perf-baseline/pr1-premerge-hotfields-20261029/`. Source/archive/binary hashes
and cancellation receipts are retained there. Historical P99 control widths
and known deviations are in docs/07; no new causal attribution was made.
Disk, hotread, variable-length and compaction matrices were outside this rerun.
F2, real-disk/O_DIRECT/io_uring work, and Azure remain outside this closeout.

## Second premerge repair, 2026-10-29: checkpoint gate failed

The user explicitly authorized a second repair. Frozen runtime `a6a3d2b`
checks inline read_only before following shared safe_read_only on a read hit.
Since safe_read_only cannot exceed read_only, mutable-tail hits can select
get_atomic immediately; reads in the fuzzy region retain the original safe
boundary check. Callback ownership and exclusive recovery are unchanged.
No driver changes, extra samples, or third repair were made. No push or merge.

### Six previously failed Read cases

Each case uses six balanced three-engine orders. All H2/H3/H4 and the 0.98
baseline-throughput gate pass. The other 12 resident cases were not rerun and
retain their `5535671` evidence; they are not measurements of this runtime.

| Workload | Speed/C++ | P99/C++ | Speed/base | P99/base | Result |
|---|---:|---:|---:|---:|---|
| memory/read/uniform/1 | 3.03595 | 0.55385 | 0.98692 | 0.98182 | passed |
| memory/read/uniform/4 | 2.90263 | 0.47518 | 1.05530 | 0.95035 | passed |
| memory/read/worker-hot/1 | 3.06137 | 0.50000 | 1.00004 | 0.98077 | passed |
| memory/read/worker-hot/4 | 3.06298 | 0.50495 | 1.00083 | 0.98077 | passed |
| memory/read/shared-hot/1 | 3.00953 | 0.50246 | 1.00014 | 0.98077 | passed |
| memory/read/shared-hot/4 | 3.01240 | 0.50495 | 0.99930 | 0.98077 | passed |

Read/uniform/1 improved from 0.93900 to 0.98692 against the Rust baseline,
remaining approximately 1.31% below it within the authorized tolerance.
All six P99/base ratios are at most one. Previously recorded write-workload
P99 differences are unaffected as evidence; they were not remeasured here.

### Checkpoint

The accepted 64-pair control from this day was reused with identical baseline,
binding, and statistical-helper hashes. The 36-round comparison, two warmups
per label, seed 20261014, and 20,000 order-stratified bootstrap samples remain
unchanged. All recovery business checks and affinity receipts passed.

| Metric | Speed/C++ | Speed/base | Base 95% interval | H5 |
|---|---:|---:|---:|---|
| mops | 1.63272 | 0.97440 | 0.966890-0.979881 | failed |
| checkpoint_ns | 1.03551 | 0.99905 | 0.998772-1.002305 | passed |
| mops_during | 1.30716 | 0.99386 | 0.990308-1.000997 | passed |
| recover_ns | 1.51610 | 1.00010 | 0.999053-1.000726 | passed |

Overall throughput still fails H5: its lower bound is 0.966890, below 0.98.
The other three H5 metrics and all four C++ metrics pass. The prior `de1c56f`
checkpoint comparison was never run, so these results do not isolate the cause
of the remaining regression to the new read condition.

Fmt, Clippy with -D warnings, Debug/Release (145 runtime and 11 compile-fail
tests each), seven existing Miri safety checks, and 13 release binaries x 30
rounds passed. The new atomic-read boundary regression passes natively. Miri
could not finish it: first the isolated REALTIME clock, then the existing
prefetch inline assembly were unsupported. Both outputs are retained; no
engine change was made to hide this coverage limitation.

Each of three checkpoint stress processes completed 33/200 rounds: 99 passes,
zero test failures, and three in-flight runs cancelled after H5 failed. The
600-run requirement and final acceptance remain incomplete. The second repair
remains local for user review; no third repair or automatic rollback followed.

Evidence in `.perf-baseline/pr1-premerge-second-20261029/` contains 108 resident
outputs, 228 checkpoint run/recovery outputs, SHA256-verified archives and
source/binary manifests, independent recomputation, and stop receipts. Earlier
failure evidence remains unchanged. Disk, hotread, variable-length, compaction,
F2, real-disk/O_DIRECT/io_uring and Azure scope did not expand.
