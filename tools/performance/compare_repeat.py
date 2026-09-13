"""Compare repeated, fully verified copies of the unchanged variable-memory trajectory."""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import platform
import statistics
import subprocess

TRACE_SHA256 = "ab0f3428395ebbd29c7f40c479b885b37e3387b473f928dc7636d198f94ce7ec"
KINDS = ["read", "upsert", "rmw", "delete"]


def validate(root, repetitions):
    result = json.loads((root / "summary.json").read_text())
    rows = list(csv.DictReader((root / "repetitions.csv").open()))
    assert result["protocol"] == 1 and result["repetitions"] == repetitions
    assert result["operations"] == repetitions * 16384
    assert result["verified_keys"] == repetitions * 2048
    assert result["pending"] == result["retries"] == 0
    assert result["elapsed_ns"] > 0 and result["overall_p99_ns"] > 0
    assert len(result["engine_ns"]) == len(result["p99_ns"]) == 4
    assert all(n > 0 for n in result["engine_ns"] + result["p99_ns"])
    assert len(rows) == repetitions
    for number, row in enumerate(rows):
        assert int(row["repetition"]) == number
        assert int(row["operations"]) == 16384 and int(row["verified_keys"]) == 2048
        assert int(row["pending"]) == int(row["retries"]) == 0
        assert int(row["elapsed_ns"]) > 0
    assert sum(int(row["elapsed_ns"]) for row in rows) == result["elapsed_ns"]
    for index, kind in enumerate(KINDS):
        assert sum(int(row[kind + "_ns"]) for row in rows) == result["engine_ns"][index]
    assert sum(result["engine_ns"]) <= result["elapsed_ns"]
    assert hashlib.sha256((root / "input.trace").read_bytes()).hexdigest() == TRACE_SHA256
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=32)
    args = parser.parse_args()
    assert 2 <= args.repetitions <= 512
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve(), "control": args.baseline.resolve()}
    metadata = {"protocol": 1, "platform": platform.platform(), "repetitions": args.repetitions,
                "trace_sha256": TRACE_SHA256, "min_throughput": .98, "max_p99": 1.10,
                "orders": list(itertools.permutations(binaries)),
                "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}}
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    pairs = []
    for number, order in enumerate(metadata["orders"]):
        pair = {"order": order, "samples": {}}
        for name in order:
            output = args.output / f"pair{number}-{name}"
            command = [str(binaries[name]), str(output), str(args.repetitions)]
            run = subprocess.run(command, text=True, capture_output=True, timeout=600)
            text = (run.stdout + run.stderr).replace(str(Path.cwd()), "<checkout>").replace(str(Path.home()), "<home>")
            (args.output / f"pair{number}-{name}.log").write_text(text)
            if run.returncode:
                raise RuntimeError(f"pair{number}-{name} failed with {run.returncode}")
            result = validate(output, args.repetitions)
            pair["samples"][name] = result
            print(number, name, result["elapsed_ns"], flush=True)
        pairs.append(pair)
        (args.output / "pairs.json").write_text(json.dumps(pairs, indent=2) + "\n")
    medians = {}
    for name in binaries:
        samples = [pair["samples"][name] for pair in pairs]
        medians[name] = {"elapsed_ns": statistics.median(row["elapsed_ns"] for row in samples),
                         "overall_p99_ns": statistics.median(row["overall_p99_ns"] for row in samples),
                         "engine_ns": [statistics.median(row["engine_ns"][kind] for row in samples) for kind in range(4)]}
    comparisons = {}
    for name in ["candidate", "control"]:
        throughput = medians["baseline"]["elapsed_ns"] / medians[name]["elapsed_ns"]
        p99 = medians[name]["overall_p99_ns"] / medians["baseline"]["overall_p99_ns"]
        comparisons[name] = {"throughput_ratio": throughput, "p99_ratio": p99,
                             "passed": throughput >= .98 and p99 <= 1.10,
                             "operation_time_ratios": {kind: medians[name]["engine_ns"][i] / medians["baseline"]["engine_ns"][i] for i, kind in enumerate(KINDS)}}
    result = {"medians": medians, "comparisons": comparisons}
    (args.output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(comparisons, indent=2))
    raise SystemExit(0 if all(r["passed"] for r in comparisons.values()) else 1)


if __name__ == "__main__":
    main()
