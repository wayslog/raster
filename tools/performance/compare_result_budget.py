"""Compare public Read workloads with retained, completed Pending results."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess

from focused_compare import LABELS, ORDERS, summarize


def validate_output(output, retained, count):
    row = json.loads(output)
    expected = {
        "engine": "rust", "case": "retained-results/read",
        "retained": retained, "count": count, "samples": count // 64,
        "checksum": count * 42, "retained_checksum": retained * 42,
        "last_accepted": retained + 1024 + count,
    }
    for key, value in expected.items():
        if type(row.get(key)) is not type(value) or row[key] != value:
            raise ValueError(f"result-budget output mismatch: {key}")
    for key in ("elapsed_ns", "p99_ns"):
        if type(row.get(key)) is not int or row[key] <= 0:
            raise ValueError(f"result-budget timing is invalid: {key}")
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-rust", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=2_000_000)
    parser.add_argument("--retained", type=int, nargs="+", default=[0, 32, 960])
    args = parser.parse_args()
    if not 16_384 <= args.count <= 100_000_000 or args.count % 64:
        parser.error("count must be 16384..100000000 and divisible by 64")
    if (any(not 0 <= n <= 4096 for n in args.retained)
            or len(set(args.retained)) != len(args.retained)):
        parser.error("retained counts must be unique integers in 0..4096")
    binaries = {"baseline": args.baseline_rust.resolve(), "candidate": args.rust.resolve(),
                "control": args.baseline_rust.resolve()}
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "protocol": 1, "scope": "Retained-result Rust diagnostic, not complete C++ acceptance",
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()),
        "platform": platform.platform(), "count": args.count, "retained": args.retained,
        "sample_stride": 64, "orders": ORDERS,
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for name, path in binaries.items()},
        "candidate_min_throughput_ratio": .98, "candidate_max_p99_ratio": 1.10,
        "control_throughput_range": [.98, 1 / .98],
        "control_p99_range": [1 / 1.10, 1.10],
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    comparisons = []
    for retained in args.retained:
        directory = args.output / f"retained-{retained}"
        directory.mkdir()
        entries = []
        for index, order in [(-1, LABELS), *enumerate(ORDERS)]:
            for name in order:
                prefix = directory / f"round-{index}-{name}"
                command = [str(binaries[name]), str(retained), str(args.count)]
                prefix.with_suffix(".command.json").write_text(json.dumps(command) + "\n")
                result = subprocess.run(command, capture_output=True, text=True, timeout=180)
                prefix.with_suffix(".stdout").write_text(result.stdout)
                prefix.with_suffix(".stderr").write_text(result.stderr)
                if result.returncode:
                    raise RuntimeError(f"{prefix.name} failed: exit {result.returncode}")
                row = validate_output(result.stdout, retained, args.count)
                if index >= 0:
                    entries.append({"round": index, "name": name, "result": row})
                    (directory / "results.json").write_text(json.dumps(entries, indent=2) + "\n")
        summary = summarize(entries)
        summary["scope"] = "Retained-result Rust diagnostic, not complete C++ acceptance"
        summary["retained"] = retained
        (directory / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
        comparisons.append(summary)
        (args.output / "comparison.json").write_text(json.dumps(comparisons, indent=2) + "\n")
        print(retained, json.dumps(summary["comparisons"]), flush=True)
    raise SystemExit(0 if all(row["passed"] for row in comparisons) else 1)


if __name__ == "__main__":
    main()
