#!/usr/bin/env python3
"""Alternate buffered F2 measurements on a single Linux CPU."""

import argparse
import json
from pathlib import Path
import statistics
import subprocess
import tempfile


WORKLOADS = (
    ("migrate", 1),
    ("resident", 1_000_000),
    ("index", 8192),
    ("both", 8192),
    ("rmw", 8192),
)
WARMUP_AND_PAIRS = 7


def ratios(workload, pairs):
    if workload == "migrate":
        return {
            field: statistics.median(pair["rust"][field] / pair["cpp"][field] for pair in pairs)
            for field in ("migration_ns", "checkpoint_ns", "recover_ns", "data_bytes", "index_bytes")
        }
    return {
        "throughput": statistics.median(
            pair["cpp"]["elapsed_ns"] / pair["rust"]["elapsed_ns"] for pair in pairs
        ),
        "p99": statistics.median(pair["rust"]["p99_ns"] / pair["cpp"]["p99_ns"] for pair in pairs),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("cpp", type=Path)
    parser.add_argument("rust", type=Path)
    parser.add_argument("--cpu", type=int, required=True)
    parser.add_argument("--log-root", type=Path, default=Path("/tmp"))
    args = parser.parse_args()
    binaries = {engine: getattr(args, engine).resolve(strict=True) for engine in ("cpp", "rust")}
    args.output.mkdir(parents=True, exist_ok=False)

    results = {}
    for workload, requests in WORKLOADS:
        pairs = []
        for index in range(WARMUP_AND_PAIRS):
            pair = {}
            order = ("cpp", "rust") if index % 2 == 0 else ("rust", "cpp")
            for engine in order:
                with tempfile.TemporaryDirectory(prefix="raster-f2-", dir=args.log_root) as root:
                    command = ["taskset", "-c", str(args.cpu), str(binaries[engine]), workload,
                               root, str(requests)]
                    run = subprocess.run(command, capture_output=True, text=True, timeout=300)
                label = args.output / f"{workload}-{engine}-{index}"
                label.with_suffix(".stdout").write_text(run.stdout)
                label.with_suffix(".stderr").write_text(run.stderr)
                run.check_returncode()
                pair[engine] = json.loads(run.stdout.splitlines()[-1])
            assert pair["cpp"]["checksum"] == pair["rust"]["checksum"]
            if workload != "migrate":
                expected = 0 if workload == "resident" else requests
                assert all(row["requests"] == requests and row["pending"] == expected
                           for row in pair.values())
            if index:
                pairs.append(pair)
        results[workload] = {"ratios": ratios(workload, pairs), "pairs": pairs}
        (args.output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
        print(workload, results[workload]["ratios"], flush=True)


if __name__ == "__main__":
    main()
