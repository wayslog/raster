"""Compare direct-engine workloads against C++ or a preserved Rust baseline."""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

MASK = (1 << 64) - 1


def expected(operation, distribution, threads, count):
    visits = [0] * (threads * 256)
    per_worker = count // threads
    for worker in range(threads):
        if distribution == "shared-hot":
            visits[0] += per_worker
        elif distribution == "worker-hot":
            visits[worker * 256] = per_worker
        else:
            for offset in range(256):
                visits[worker * 256 + (offset * 17) % 256] = per_worker // 256 + (offset < per_worker % 256)
    digest = 0xcbf29ce484222325
    for n in visits:
        value = 7 + n if operation == "rmw" else 42 if n and operation == "upsert" else 7
        digest = ((digest ^ value) * 0x100000001b3) & MASK
    checksum = count * (7 if operation == "read" else 42)
    if operation == "rmw":
        checksum = sum(7 * n + n * (n + 1) // 2 for n in visits)
    return {"digest": digest, "checksum": checksum & MASK, "verified_keys": len(visits)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust", type=Path, required=True)
    reference = parser.add_mutually_exclusive_group(required=True)
    reference.add_argument("--cpp", type=Path)
    reference.add_argument("--baseline-rust", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=2_000_000)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--case", help="Optional operation/distribution/threads filter")
    parser.add_argument("--min-throughput", type=float, default=0.9)
    parser.add_argument("--max-p99", type=float, default=1.1)
    args = parser.parse_args()
    assert args.rounds >= 2 and args.count >= 16_384 and args.count % 4 == 0
    args.output.mkdir(parents=True, exist_ok=False)
    reference = "cpp" if args.cpp else "baseline"
    binaries = {reference: (args.cpp or args.baseline_rust).resolve(), "rust": args.rust.resolve()}
    metadata = {
        "protocol": 1, "created_unix": time.time(), "platform": platform.platform(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()),
        "cpu_count": os.cpu_count(), "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()},
        "count": args.count, "rounds": args.rounds, "sample_stride": 64,
        "reference": reference,
        "min_throughput_ratio": args.min_throughput, "max_p99_ratio": args.max_p99,
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    summaries = []
    for operation, distribution, threads in itertools.product(
        ("read", "upsert", "rmw"), ("uniform", "worker-hot", "shared-hot"), (1, 4)
    ):
        case = f"{operation}/{distribution}/{threads}"
        if args.case and args.case != case:
            continue
        rows = {name: [] for name in binaries}
        expected_state = expected(operation, distribution, threads, args.count)
        # One unmeasured warmup per engine precedes alternating AB/BA process pairs.
        for round_index in range(-1, args.rounds):
            order = (reference, "rust") if round_index % 2 == 0 else ("rust", reference)
            for name in order:
                command = [str(binaries[name]), operation, distribution, str(threads), str(args.count), "64"]
                completed = subprocess.run(command, capture_output=True, text=True, timeout=180)
                prefix = f"{operation}-{distribution}-{threads}-{round_index}-{name}"
                (args.output / f"{prefix}.stdout").write_text(completed.stdout)
                (args.output / f"{prefix}.stderr").write_text(completed.stderr)
                if completed.returncode:
                    raise RuntimeError(f"{prefix} failed: {completed.returncode}; inspect saved output")
                objects = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith('{"engine":')]
                assert len(objects) == 1, prefix
                row = objects[0]
                expected_engine = "cpp" if name == "cpp" else "rust"
                assert row["engine"] == expected_engine and row["operation"] == operation and row["distribution"] == distribution
                assert row["threads"] == threads and row["count"] == args.count
                assert row["elapsed_ns"] > 0 and row["p99_ns"] > 0 and row["samples"] >= 256
                assert all(row[key] == value for key, value in expected_state.items()), (prefix, row, expected_state)
                if round_index >= 0:
                    rows[name].append(row)
        throughput = statistics.median(r["elapsed_ns"] for r in rows[reference]) / statistics.median(r["elapsed_ns"] for r in rows["rust"])
        p99 = statistics.median(r["p99_ns"] for r in rows["rust"]) / statistics.median(r["p99_ns"] for r in rows[reference])
        passed = throughput >= args.min_throughput and p99 <= args.max_p99
        summaries.append({"case": case, "throughput_ratio": throughput, "p99_ratio": p99, "passed": passed, "rows": rows})
        (args.output / "comparison.json").write_text(json.dumps(summaries, indent=2) + "\n")
        print(f"{case}: throughput={throughput:.3f}, p99={p99:.3f}, {'PASS' if passed else 'FAIL'}", flush=True)
    assert summaries, "case filter did not match any scenario"
    raise SystemExit(0 if all(row["passed"] for row in summaries) else 1)


if __name__ == "__main__":
    main()
