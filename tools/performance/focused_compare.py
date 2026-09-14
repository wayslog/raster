"""Diagnose a single-thread Rust regression with long, interleaved controls.

This diagnostic does not replace the complete C++ performance matrix.
"""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess

from cpu_profile import validate_output


LABELS = ("baseline", "candidate", "control")
ORDERS = tuple(itertools.permutations(LABELS))


def validate_case(case):
    parts = case.split("/")
    if (len(parts) != 3 or parts[0] not in ("read", "upsert", "rmw")
            or parts[1] not in ("uniform", "worker-hot", "shared-hot")
            or parts[2] != "1"):
        raise ValueError("focused comparison requires operation/distribution/1")
    return parts


def validate_measurement(output, case, count):
    row = validate_output(output, "rust", case, count)
    if row["samples"] != count // 64:
        raise ValueError("focused comparison requires every scheduled latency sample")
    return row


def summarize(entries):
    expected_order = [(round_index, name) for round_index, order in enumerate(ORDERS)
                      for name in order]
    if [(entry["round"], entry["name"]) for entry in entries] != expected_order:
        raise ValueError("missing, duplicated, or reordered measurement")
    rows = {name: [entry["result"] for entry in entries if entry["name"] == name]
            for name in LABELS}
    baseline_time = statistics.median(row["elapsed_ns"] for row in rows["baseline"])
    baseline_p99 = statistics.median(row["p99_ns"] for row in rows["baseline"])
    comparisons = {}
    for name in ("candidate", "control"):
        elapsed = statistics.median(row["elapsed_ns"] for row in rows[name])
        p99 = statistics.median(row["p99_ns"] for row in rows[name])
        throughput_ratio = baseline_time / elapsed
        p99_ratio = p99 / baseline_p99
        passed = throughput_ratio >= .98 and p99_ratio <= 1.10
        if name == "control":
            # An implausibly faster identical control is also unstable.
            passed = .98 <= throughput_ratio <= 1 / .98 and 1 / 1.10 <= p99_ratio <= 1.10
        comparisons[name] = {
            "throughput_ratio": throughput_ratio, "p99_ratio": p99_ratio,
            "passed": passed,
        }
    per_round = []
    for index in range(len(ORDERS)):
        baseline = rows["baseline"][index]
        per_round.append({"round": index, **{
            name: {"throughput_ratio": baseline["elapsed_ns"] / rows[name][index]["elapsed_ns"],
                   "p99_ratio": rows[name][index]["p99_ns"] / baseline["p99_ns"]}
            for name in ("candidate", "control")}})
    return {
        "comparisons": comparisons,
        "ratios_by_round": per_round,
        "passed": all(row["passed"] for row in comparisons.values()),
        "control_valid": comparisons["control"]["passed"],
        "scope": "Single-case diagnostic only; not complete C++ acceptance",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--count", type=int, default=20_000_000)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--allow-unpinned", action="store_true",
                        help="Allow local smoke validation on platforms without CPU affinity")
    args = parser.parse_args()
    try:
        parts = validate_case(args.case)
    except ValueError as error:
        parser.error(str(error))
    if args.count < 16_384 or args.count % 64:
        parser.error("count must be at least 16384 and divisible by 64")
    affinity = None
    available = None
    if hasattr(os, "sched_getaffinity") and hasattr(os, "sched_setaffinity"):
        available = sorted(os.sched_getaffinity(0))
        cpu = available[0] if args.cpu is None else args.cpu
        if cpu not in available:
            parser.error("requested CPU is not in the allowed affinity mask")
        os.sched_setaffinity(0, {cpu})
        affinity = sorted(os.sched_getaffinity(0))
        if affinity != [cpu]:
            raise RuntimeError("single-CPU affinity was not applied")
    elif not args.allow_unpinned or args.cpu is not None:
        parser.error("CPU affinity is unavailable; --allow-unpinned is for local smoke checks")
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve(),
                "control": args.baseline.resolve()}
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "protocol": 1, "scope": "Focused Rust component diagnosis, not full C++ acceptance",
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()),
        "platform": platform.platform(), "case": args.case, "count": args.count,
        "sample_stride": 64, "orders": ORDERS, "available_cpus": available,
        "inherited_cpu_affinity": affinity,
        "affinity_method": "Parent mask checked before every spawn; child processes and threads inherit it",
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for name, path in binaries.items()},
        "min_throughput_ratio": .98, "max_p99_ratio": 1.10,
        "control_throughput_range": [.98, 1 / .98],
        "control_p99_range": [1 / 1.10, 1.10],
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    entries = []
    orders = [(-1, LABELS), *enumerate(ORDERS)]
    for round_index, order in orders:
        for name in order:
            if affinity is not None and sorted(os.sched_getaffinity(0)) != affinity:
                raise RuntimeError("CPU affinity changed during the diagnostic")
            command = [str(binaries[name]), *parts, str(args.count), "64"]
            prefix = args.output / f"round-{round_index}-{name}"
            prefix.with_suffix(".command.json").write_text(json.dumps(command) + "\n")
            completed = subprocess.run(command, capture_output=True, text=True, timeout=180)
            prefix.with_suffix(".stdout").write_text(completed.stdout)
            prefix.with_suffix(".stderr").write_text(completed.stderr)
            if completed.returncode:
                raise RuntimeError(f"{prefix.name} failed with exit code {completed.returncode}")
            row = validate_measurement(completed.stdout, args.case, args.count)
            print(round_index, name, row["elapsed_ns"], row["p99_ns"], flush=True)
            if round_index >= 0:
                entries.append({"round": round_index, "name": name, "result": row})
                (args.output / "results.json").write_text(json.dumps(entries, indent=2) + "\n")
    summary = summarize(entries)
    (args.output / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2), flush=True)
    raise SystemExit(0 if summary["passed"] else 1)


if __name__ == "__main__":
    main()
