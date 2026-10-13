#!/usr/bin/env python3
"""Run alternating variable-record benchmark pairs on a Linux benchmark host.

Usage: varlen_run.py OUTDIR CPP_BIN RUST_BIN LOG_ROOT [resident|disk]
Only benchmark-local paths are supplied by the caller; no host information is saved.
"""
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

out, cpp, rust, log_root = map(Path, sys.argv[1:5])
out.mkdir(parents=True, exist_ok=False)
binaries = {"cpp": cpp.resolve(), "rust": rust.resolve()}
(out / "hashes.json").write_text(json.dumps({name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}, indent=2))
preload = os.environ.get("LD_PRELOAD", "")
if preload:
    libraries = [Path(name) for name in preload.replace(":", " ").split()]
    (out / "preload-hashes.json").write_text(json.dumps(
        {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in libraries}, indent=2))
selected = sys.argv[5] if len(sys.argv) > 5 else None
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
                    command = ["numactl", "--physcpubind=" + ("4" if threads == 1 else "4,5,6,7"), "--membind=0",
                               str(binaries[engine]), op, str(threads), str(keys), "2000000", "64", str(directory) + "/", "8", "256"]
                    result = subprocess.run(command, capture_output=True, text=True, timeout=1800)
                    (out / (label + ".stdout")).write_text(result.stdout)
                    (out / (label + ".stderr")).write_text(result.stderr)
                    result.check_returncode()
                    row = json.loads(result.stdout.strip().splitlines()[-1])
                    assert row["engine"] == engine and row["count"] == 2_000_000 and row["keys"] == keys
                    if scene == "resident":
                        assert row["pending"] == 0, (case, engine, row)
                    runs[engine].append(row)
                    # Refuse to record timing results if any business result differs.
                    for field in ("checksum", "digest"):
                        assert len({r[field] for group in runs.values() for r in group}) == 1, (case, field, runs)
                    shutil.rmtree(directory)
            row = {"case": case}
            for engine in runs:
                row[engine] = {"mops": statistics.median(2_000_000 * 1000 / r["elapsed_ns"] for r in runs[engine]),
                               "p99_ns": statistics.median(r["p99_ns"] for r in runs[engine]),
                               "pending": statistics.median(r["pending"] for r in runs[engine])}
            row["throughput_ratio"] = row["rust"]["mops"] / row["cpp"]["mops"]
            row["p99_ratio"] = row["rust"]["p99_ns"] / row["cpp"]["p99_ns"]
            row["accepted"] = row["throughput_ratio"] >= 1 and row["p99_ratio"] <= 1
            summary.append(row)
            (out / "summary.json").write_text(json.dumps(summary, indent=2))
            print(json.dumps(row), flush=True)
if not all(row["accepted"] for row in summary):
    raise SystemExit("one or more performance gates failed; raw evidence retained")
