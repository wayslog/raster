"""Compare original and fixed argv[0] using unchanged native Rust executables."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess

from cpu_profile import validate_output


DEFAULT_CASES = ("read/uniform/4", "read/shared-hot/4", "upsert/worker-hot/4")
ROLES = {
    "baseline-original": ("baseline", None),
    "candidate-original": ("candidate", None),
    "baseline-fixed": ("baseline", "parity"),
    "candidate-fixed": ("candidate", "parity"),
}
ORDERS = (
    tuple(ROLES),
    tuple(reversed(ROLES)),
    ("baseline-fixed", "candidate-fixed", "baseline-original", "candidate-original"),
    ("candidate-original", "baseline-original", "candidate-fixed", "baseline-fixed"),
)


def invoke(binary, arguments, process_name, prefix):
    """Change argv[0] without changing the program selected by exec."""
    command = [process_name or str(binary), *arguments]
    result = subprocess.run(command, executable=str(binary), capture_output=True, text=True, timeout=180)
    prefix.with_suffix(".stdout").write_text(result.stdout)
    prefix.with_suffix(".stderr").write_text(result.stderr)
    prefix.with_suffix(".launch.json").write_text(json.dumps({
        "executable": str(binary), "argv": command, "exit_code": result.returncode,
    }, indent=2) + "\n")
    if result.returncode:
        raise RuntimeError(f"{prefix.name} failed with exit code {result.returncode}")
    return result.stdout


def compare(rows, candidate, baseline):
    throughput = statistics.median(row["elapsed_ns"] for row in rows[baseline]) / statistics.median(
        row["elapsed_ns"] for row in rows[candidate])
    p99 = statistics.median(row["p99_ns"] for row in rows[candidate]) / statistics.median(
        row["p99_ns"] for row in rows[baseline])
    return {"throughput_ratio": throughput, "p99_ratio": p99,
            "passed": throughput >= .98 and p99 <= 1.10}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", default="")
    parser.add_argument("--count", type=int, default=2_000_000)
    parser.add_argument("--rounds", type=int, default=8)
    args = parser.parse_args()
    if args.count < 16_384 or args.count % 4 or args.rounds < 4 or args.rounds % 4:
        parser.error("count must be at least 16384 and divisible by four; rounds must be a positive multiple of four")
    cases = (args.case,) if args.case else DEFAULT_CASES
    for case in cases:
        parts = case.split("/")
        if (len(parts) != 3 or parts[0] not in ("read", "upsert", "rmw")
                or parts[1] not in ("uniform", "worker-hot", "shared-hot") or parts[2] not in ("1", "4")):
            parser.error("case must be operation/distribution/threads")
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    hashes = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "protocol": 1, "purpose": "Launch diagnosis only; successful collection is not parity acceptance",
        "variable": "argv[0]; executable bytes, workload arguments and output validation stay unchanged",
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "count": args.count, "rounds": args.rounds, "sample_stride": 64,
        "cases": cases, "orders": ORDERS, "binary_sha256": hashes,
        "argv0_lengths": {role: len(name or str(binaries[engine])) for role, (engine, name) in ROLES.items()},
        "driver_sha256": {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                          for name in ("launch_control.py", "cpu_profile.py", "compare.py")},
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    for case in cases:
        rows = {role: [] for role in ROLES}
        for round_index in range(-1, args.rounds):
            for role in ORDERS[round_index % len(ORDERS)]:
                engine, process_name = ROLES[role]
                prefix = args.output / (case.replace("/", "-") + f"-{round_index}-{role}")
                output = invoke(binaries[engine], [*case.split("/"), str(args.count), "64"], process_name, prefix)
                row = validate_output(output, "rust", case, args.count)
                if round_index >= 0:
                    rows[role].append(row)
        comparisons = {
            "original": compare(rows, "candidate-original", "baseline-original"),
            "fixed": compare(rows, "candidate-fixed", "baseline-fixed"),
            "baseline-argv0": compare(rows, "baseline-fixed", "baseline-original"),
            "candidate-argv0": compare(rows, "candidate-fixed", "candidate-original"),
        }
        results.append({"case": case, "comparisons": comparisons, "rows": rows})
        (args.output / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")
        print(case, json.dumps(comparisons), flush=True)
    assert hashes == {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}
    print("Launch controls collected with complete workload validation; this is not a parity verdict.")


if __name__ == "__main__":
    main()
