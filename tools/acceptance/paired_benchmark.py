"""Alternately execute two real-world benchmark binaries on the same host,Keep fixed inputs and per-round metrics;Not replacing tag attribution with cross-host noise."""
import argparse
import csv
import hashlib
import json
import itertools
import os
from pathlib import Path
import statistics
import subprocess

ORDERS = [("baseline", "candidate"), ("candidate", "baseline"), ("candidate", "baseline"), ("baseline", "candidate")]
DEFAULT_CASES = ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "bytes-hot-t1-memory-r1"]


def clean(text):
    return text.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", dest="cases")
    parser.add_argument("--interleaved-control", action="store_true", help="Interleave running of the new version and two copies of the same old binary in the same round,Balance positions in six orders")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {name: getattr(args, name).resolve() for name in ["baseline", "candidate"]}
    if args.interleaved_control:
        binaries["control"] = binaries["baseline"]
    orders = list(itertools.permutations(binaries)) if args.interleaved_control else ORDERS
    metadata = {name: {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in binaries.items()}
    (args.output / "binaries.json").write_text(json.dumps(metadata, indent=2))
    summaries = []
    for case in args.cases or DEFAULT_CASES:
        pairs = []
        expected_hash = None
        for number, order in enumerate(orders, 1):
            pair = {"pair": number, "order": list(order)}
            for side in order:
                output = args.output / f"{case}-pair{number}-{side}"
                env = {**os.environ, "RASTER_BENCH_CASE": case, "RASTER_BENCH_OUTPUT": str(output.resolve()), "CARGO_TERM_COLOR": "never", "RUST_BACKTRACE": "0"}
                result = subprocess.run([str(binaries[side])], env=env, capture_output=True, text=True, timeout=180)
                combined = result.stdout + result.stderr
                (args.output / f"{case}-pair{number}-{side}.txt").write_text(clean(combined))
                if result.returncode:
                    raise RuntimeError(f"{case} ordinal {number} Yes {side} business failure:{result.returncode}")
                rows = list(csv.DictReader((output / "matrix.csv").open()))
                if len(rows) != 1 or int(rows[0]["operation_count"]) != 16384:
                    raise RuntimeError("The benchmark did not fully execute the specified scenario")
                marker = f"benchmark {case} passed:16384 steps_four_operations,"
                if marker not in combined or "2048 keys_and_tombstone_validation" not in combined:
                    raise RuntimeError("Business and recovery verification not completed")
                row = rows[0]
                digest = hashlib.sha256((output / (row["input_id"] + ".trace")).read_bytes()).hexdigest()
                if expected_hash is not None and digest != expected_hash:
                    raise RuntimeError("Paired input bytes are inconsistent")
                expected_hash = digest
                pair[side] = row
                print(case, number, side, row["operations_per_second"], row["P99nanoseconds"], flush=True)
            pairs.append(pair)
            (args.output / (case + "-pairs.json")).write_text(json.dumps(pairs, ensure_ascii=False, indent=2))
        metrics = ["operations_per_second", "P99nanoseconds", "readP99nanoseconds", "writeP99nanoseconds", "RMWP99nanoseconds", "deleteP99nanoseconds", "post_workload_read_bytes", "post_workload_write_bytes"]
        medians = {side: {metric: statistics.median(float(pair[side][metric]) for pair in pairs) for metric in metrics} for side in binaries}
        throughput = medians["candidate"]["operations_per_second"] / medians["baseline"]["operations_per_second"]
        latency = medians["candidate"]["P99nanoseconds"] / medians["baseline"]["P99nanoseconds"]
        summaries.append({"case": case, "input_sha256": expected_hash, "medians": medians,
                          "throughput_ratio": throughput, "p99_ratio": latency,
                          "review_required": throughput < .75 or latency > 1.30})
        if args.interleaved_control:
            control_throughput = medians["control"]["operations_per_second"] / medians["baseline"]["operations_per_second"]
            control_latency = medians["control"]["P99nanoseconds"] / medians["baseline"]["P99nanoseconds"]
            summaries[-1].update(control_throughput_ratio=control_throughput, control_p99_ratio=control_latency,
                                 control_review_required=control_throughput < .75 or control_latency > 1.30)
        (args.output / "comparison.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    if any(row["review_required"] or row.get("control_review_required", False) for row in summaries):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
