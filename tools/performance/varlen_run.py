#!/usr/bin/env python3
"""Run alternating variable-record benchmark pairs on a Linux benchmark host.

Usage: varlen_run.py OUTDIR CPP_BIN RUST_BIN LOG_ROOT [resident|disk] [IO_THREADS]
Only benchmark-local paths are supplied by the caller; no host information is saved.
"""
import hashlib
import json
import shutil
import statistics
import subprocess
import sys
from pathlib import Path
import affinity

out, cpp, rust, log_root = map(Path, sys.argv[1:5])
out.mkdir(parents=True, exist_ok=False)
binaries = {"cpp": cpp.resolve(), "rust": rust.resolve()}
(out / "hashes.json").write_text(json.dumps({name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}, indent=2))
(out / "affinity.json").write_text(json.dumps(affinity.manifest(), indent=2))
selected = sys.argv[5] if len(sys.argv) > 5 else None
assert selected in (None, "resident", "disk")
io_threads = int(sys.argv[6]) if len(sys.argv) > 6 else 8
assert io_threads in (1, 8)
(out / "measurement.json").write_text(json.dumps({
    "latency_scope": "end_to_end", "io_threads": io_threads, "rounds": 3,
    "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    "affinity_sha256": hashlib.sha256(Path(affinity.__file__).read_bytes()).hexdigest(),
}, indent=2))
summary = []
for scene, keys in (("resident", 8192), ("disk", 16_000_000)):
    if selected and selected != scene:
        continue
    for op in ("read", "upsert", "rmw"):
        for threads in (1, 4):
            case = f"{scene}/{op}/{threads}"
            runs = {"cpp": [], "rust": []}
            for round_id in range(3):
                for engine in (("cpp", "rust") if round_id % 2 == 0 else ("rust", "cpp")):
                    directory = log_root / f"raster-varlen-{scene}-{op}-{threads}-{engine}"
                    if directory.exists():
                        raise RuntimeError(f"benchmark directory already exists: {directory}")
                    directory.mkdir(parents=True)
                    label = f"{scene}-{op}-{threads}-r{round_id}-{engine}"
                    command = affinity.command(binaries[engine],
                        [op, threads, keys, 2_000_000, 64, str(directory) + "/", 8, 256],
                        engine, threads, io_threads)
                    (out / (label + ".command.json")).write_text(json.dumps(command))
                    result = subprocess.run(command, env=affinity.process_environment(),
                                            capture_output=True, text=True, timeout=1800)
                    (out / (label + ".stdout")).write_text(result.stdout)
                    (out / (label + ".stderr")).write_text(result.stderr)
                    result.check_returncode()
                    receipts = affinity.verify(result.stderr, threads)
                    (out / (label + ".affinity.json")).write_text(json.dumps(receipts, indent=2))
                    row = json.loads(result.stdout.strip().splitlines()[-1])
                    assert row["engine"] == engine and row["count"] == 2_000_000 and row["keys"] == keys
                    expected_samples = threads * sum((i & 63) == ((i // 64) & 63)
                                                    for i in range(2_000_000 // threads))
                    assert row["latency_scope"] == "end_to_end" and row["samples"] == expected_samples
                    assert 0 <= row["pending_samples"] <= min(row["samples"], row["pending"])
                    if engine == "rust":
                        assert row["io_threads"] == io_threads
                    if scene == "resident":
                        assert row["pending"] == 0, (case, engine, row)
                    elif op in ("read", "rmw"):
                        assert row["pending_samples"] > row["samples"] // 2, (case, engine, row)
                    runs[engine].append(row)
                    # Refuse to record timing results if any business result differs.
                    for field in ("checksum", "digest"):
                        assert len({r[field] for group in runs.values() for r in group}) == 1, (case, field, runs)
                    shutil.rmtree(directory)
            row = {"case": case}
            for engine in runs:
                row[engine] = {"mops": statistics.median(2_000_000 * 1000 / r["elapsed_ns"] for r in runs[engine]),
                               "p99_ns": statistics.median(r["p99_ns"] for r in runs[engine]),
                               "samples": statistics.median(r["samples"] for r in runs[engine]),
                               "pending_samples": statistics.median(r["pending_samples"] for r in runs[engine]),
                               "pending": statistics.median(r["pending"] for r in runs[engine])}
            row["throughput_ratio"] = row["rust"]["mops"] / row["cpp"]["mops"]
            row["p99_ratio"] = row["rust"]["p99_ns"] / row["cpp"]["p99_ns"]
            row["accepted"] = row["throughput_ratio"] >= 1 and row["p99_ratio"] <= 1
            summary.append(row)
            (out / "summary.json").write_text(json.dumps(summary, indent=2))
            print(json.dumps(row), flush=True)
if not all(row["accepted"] for row in summary):
    raise SystemExit("one or more performance gates failed; raw evidence retained")
