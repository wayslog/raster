#!/usr/bin/env python3
"""Revalidate fixed workloads with mandatory role affinity and frozen binaries.

Usage: fixed_run.py MODE OUT CPP BASE CANDIDATE [--log-root /dev/shm]
MODE is memory, disk, or compact. Disk binaries must all use end-to-end samples.
Six balanced orders provide wall-clock medians. Resident cycles use six rounds,
or 24 for upsert/shared-hot/4, including process initialization on both sides.
"""
import argparse
import hashlib
import itertools
import json
import shutil
import statistics
import subprocess
import tempfile
from pathlib import Path

import affinity

COUNT = 2_000_000
KEYS = 16_000_000
STRIDE = 64


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("memory", "disk", "compact"))
    parser.add_argument("out", type=Path)
    for name in ("cpp", "base", "candidate"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--log-root", type=Path, default=Path("/dev/shm"))
    args = parser.parse_args()
    binaries = {name: getattr(args, name).resolve(strict=True) for name in ("cpp", "base", "candidate")}
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = {"binding": affinity.manifest(), "mode": args.mode, "rounds": 6,
                "binary_hashes": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()},
                "runner_hash": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "affinity_hash": hashlib.sha256(Path(affinity.__file__).read_bytes()).hexdigest()}
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    if args.mode == "memory":
        cases = [(op, dist, threads) for op in ("read", "upsert", "rmw")
                 for dist in ("uniform", "worker-hot", "shared-hot") for threads in (1, 4)]
    elif args.mode == "disk":
        cases = [(op, cache, threads) for op, cache in
                 (("read", 0), ("rmw", 0), ("hotread", 0), ("hotread", 256)) for threads in (1, 4)]
    else:
        cases = [("compact", 0, 4)]
    summary = []
    orders = list(itertools.permutations(binaries))
    for op, detail, threads in cases:
        case = f"{op}/{detail}/{threads}"
        tag = case.replace("/", "-")
        business = None

        def invoke(name, label, instrument=False):
            nonlocal business
            directory = None
            if args.mode == "memory":
                arguments = [op, detail, threads, COUNT, STRIDE]
                workers, io_threads = threads, 0
            else:
                directory = Path(tempfile.mkdtemp(prefix="raster-fixed-review-", dir=args.log_root))
                path = str(directory) + "/"
                arguments = ([op, threads, KEYS, COUNT, STRIDE, path, detail] if args.mode == "disk"
                             else [threads, KEYS, path])
                # Compaction's calling thread also does work on the coordinator CPU.
                workers, io_threads = (threads if args.mode == "disk" else threads - 1), 8
            command = affinity.command(binaries[name], arguments, "cpp" if name == "cpp" else "rust", workers, io_threads)
            if instrument:
                command = ["perf", "stat", "-x,", "-e", "instructions:u,cycles:u", "-o",
                           str(args.out / (label + ".perf")), "--", *command]
            (args.out / (label + ".command.json")).write_text(json.dumps(command))
            run = subprocess.run(command, env=affinity.process_environment(), capture_output=True, text=True, timeout=1800)
            (args.out / (label + ".stdout")).write_text(run.stdout)
            (args.out / (label + ".stderr")).write_text(run.stderr)
            run.check_returncode()
            roles = affinity.verify(run.stderr, workers)
            (args.out / (label + ".affinity.json")).write_text(json.dumps(roles, indent=2))
            row = json.loads(run.stdout.strip().splitlines()[-1])
            assert row["threads"] == threads and row["engine"] == ("cpp" if name == "cpp" else "rust")
            if args.mode == "compact":
                assert row["keys"] == KEYS and row["sum"] == row["expected"]
                actual = {field: row[field] for field in ("sum", "compacted_bytes", "size_before", "size_after")}
            else:
                assert row["count"] == COUNT
                actual = {field: row[field] for field in ("checksum", "digest", "samples")}
                if args.mode == "disk":
                    expected = threads * sum((i & 63) == ((i // 64) & 63) for i in range(COUNT // threads))
                    assert row["keys"] == KEYS and row["latency_scope"] == "end_to_end"
                    assert row["samples"] == expected and 0 < row["pending_samples"] <= row["samples"]
            if business is None:
                business = actual
            assert actual == business, (case, name, actual, business)
            if instrument:
                for line in (args.out / (label + ".perf")).read_text().splitlines():
                    values = line.split(",")
                    if len(values) > 2 and values[2] in ("instructions:u", "cycles:u"):
                        row[values[2]] = int(values[0])
                assert row["cycles:u"] > 0 and row["instructions:u"] > 0
            if directory:
                shutil.rmtree(directory)
            return row

        runs = {name: [] for name in binaries}
        for name in binaries:
            invoke(name, f"{tag}-warmup-{name}")
        for iteration, order in enumerate(orders):
            for name in order:
                runs[name].append(invoke(name, f"{tag}-round-{iteration}-{name}"))
        result = {"case": case}
        for name, rows in runs.items():
            if args.mode == "compact":
                result[name] = {"compact_ns": statistics.median(row["compact_ns"] for row in rows)}
            else:
                result[name] = {"mops": statistics.median(COUNT * 1000 / row["elapsed_ns"] for row in rows),
                                "p99_ns": statistics.median(row["p99_ns"] for row in rows),
                                "pending": statistics.median(row.get("pending", 0) for row in rows)}
        if args.mode == "memory":
            stat_runs = {name: [] for name in binaries}
            stat_rounds = 24 if (op, detail, threads) == ("upsert", "shared-hot", 4) else 6
            for iteration in range(stat_rounds):
                for name in orders[iteration % len(orders)]:
                    stat_runs[name].append(invoke(name, f"{tag}-stat-{iteration}-{name}", True))
            for name, rows in stat_runs.items():
                for metric in ("cycles:u", "instructions:u"):
                    result[name][metric] = statistics.median(row[metric] for row in rows) / COUNT
            result["cycles_ratio"] = result["candidate"]["cycles:u"] / result["base"]["cycles:u"]
        if args.mode == "compact":
            result["base_speed_ratio"] = result["base"]["compact_ns"] / result["candidate"]["compact_ns"]
            result["cpp_speed_ratio"] = result["cpp"]["compact_ns"] / result["candidate"]["compact_ns"]
        else:
            result["base_speed_ratio"] = result["candidate"]["mops"] / result["base"]["mops"]
            result["cpp_speed_ratio"] = result["candidate"]["mops"] / result["cpp"]["mops"]
            result["base_p99_ratio"] = result["candidate"]["p99_ns"] / result["base"]["p99_ns"]
            result["cpp_p99_ratio"] = result["candidate"]["p99_ns"] / result["cpp"]["p99_ns"]
        result["accepted"] = (result["base_speed_ratio"] >= .98 and result["cpp_speed_ratio"] >= 1
                              and result.get("cycles_ratio", 1) <= 1.03
                              and result.get("base_p99_ratio", 1) <= 1 / .98)
        summary.append(result)
        (args.out / "summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(result), flush=True)
    if not all(row["accepted"] for row in summary):
        raise SystemExit("fixed workload gate failed; raw evidence retained")


if __name__ == "__main__":
    main()
