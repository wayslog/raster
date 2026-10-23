#!/usr/bin/env python3
"""Frozen checkpoint control (64 pairs) or comparison (36 balanced rounds).

The control must pass before comparison starts. Every process uses explicit CPU
roles and verifies recovery. Raw output and failures are retained. No timings
are discarded except the two predeclared warmups per label.
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
import checkpoint_stats as stats

WORKLOAD = {"threads": 4, "keys": 4000000, "ops": 40000000, "io_threads": 8}
POLICY = {"control_pairs": 64, "comparison_rounds": 36, "warmups_per_label": 2,
          "seed": stats.SEED, "resamples": stats.RESAMPLES, "confidence": 0.95,
          "estimator": "median_paired_log_speed_ratio", "strata": "execution_order",
          "aa_width_limit": 0.02, "aa_interval": [stats.LOWER, stats.UPPER]}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("control", "compare"))
    parser.add_argument("out", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--cpp", type=Path)
    parser.add_argument("--control", type=Path)
    parser.add_argument("--log-root", type=Path, default=Path("/dev/shm"))
    args = parser.parse_args()
    base = args.baseline.resolve(strict=True)
    binding = affinity.manifest()
    code_hashes = {name: digest(Path(__file__).with_name(name)) for name in
                   ("checkpoint_run.py", "checkpoint_stats.py", "affinity.py")}
    if args.mode == "control":
        binaries = {"a": base, "b": base}
        engines = {"a": "rust", "b": "rust"}
    else:
        if not (args.candidate and args.cpp and args.control):
            parser.error("compare requires --candidate, --cpp and --control")
        control = json.loads((args.control / "summary.json").read_text())
        if not control["accepted"] or control["mode"] != "control":
            raise RuntimeError("same-binary control did not pass")
        if control["binding"] != binding or control["workload"] != WORKLOAD or control["policy"] != POLICY:
            raise RuntimeError("control measurement configuration differs")
        if control["binary_hashes"]["a"] != digest(base) or control["code_hashes"] != code_hashes:
            raise RuntimeError("baseline binary or runner changed after control")
        binaries = {"base": base, "candidate": args.candidate.resolve(strict=True), "cpp": args.cpp.resolve(strict=True)}
        engines = {"base": "rust", "candidate": "rust", "cpp": "cpp"}
    args.out.mkdir(parents=True, exist_ok=False)
    metadata = {"mode": args.mode, "binding": binding, "workload": WORKLOAD, "policy": POLICY,
                "binary_hashes": {name: digest(path) for name, path in binaries.items()}, "code_hashes": code_hashes}
    (args.out / "manifest.json").write_text(json.dumps(metadata, indent=2))
    environment = affinity.process_environment()
    threads, keys, ops = (WORKLOAD[k] for k in ("threads", "keys", "ops"))

    def invoke(label, arguments, stem, workers):
        command = affinity.command(binaries[label], arguments, engines[label], workers, WORKLOAD["io_threads"])
        (args.out / (stem + ".command.json")).write_text(json.dumps(command))
        run = subprocess.run(command, capture_output=True, text=True, env=environment, timeout=900)
        (args.out / (stem + ".stdout")).write_text(run.stdout)
        (args.out / (stem + ".stderr")).write_text(run.stderr)
        run.check_returncode()
        roles = affinity.verify(run.stderr, workers)
        (args.out / (stem + ".affinity.json")).write_text(json.dumps(roles, indent=2))
        return json.loads(run.stdout.strip().splitlines()[-1])

    def measure(label, stem):
        directory = Path(tempfile.mkdtemp(prefix="raster-checkpoint-review-", dir=args.log_root))
        path = str(directory) + "/"
        run = invoke(label, ["run", threads, keys, ops, path], stem + "-run", threads)
        if (run["threads"], run["keys"], run["ops"]) != (threads, keys, ops):
            raise RuntimeError("workload mismatch")
        recovered = invoke(label, ["recover", threads, keys, run["token"], path], stem + "-recover", 0)
        expected = keys * (keys - 1) // 2 + run["persisted_serials"]
        if recovered["persisted_serials"] != run["persisted_serials"] or recovered["sum"] != expected or recovered["expected"] != expected:
            raise RuntimeError("checkpoint recovery business check failed")
        row = {"mops": ops * 1000 / run["elapsed_ns"], "checkpoint_ns": run["checkpoint_ns"],
               "mops_during": run["ops_during_checkpoint"] * 1000 / run["checkpoint_ns"], "recover_ns": recovered["recover_ns"]}
        shutil.rmtree(directory)
        return row

    for warmup in range(POLICY["warmups_per_label"]):
        for label in binaries:
            measure(label, f"warmup-{warmup}-{label}")
            print(f"warmup {warmup + 1} {label}: verified", flush=True)
    pairs = []
    orders = [("a", "b"), ("b", "a")] if args.mode == "control" else list(itertools.permutations(binaries))
    rounds = POLICY["control_pairs"] if args.mode == "control" else POLICY["comparison_rounds"]
    for iteration in range(rounds):
        order = orders[iteration % len(orders)]
        rows = {label: measure(label, f"round-{iteration:02d}-{label}") for label in order}
        pairs.append({"round": iteration, "order": "/".join(order), "rows": rows})
        (args.out / "pairs.json").write_text(json.dumps(pairs, indent=2))
        print(f"{args.mode} {iteration + 1}/{rounds}: business and affinity verified", flush=True)
    if args.mode == "control":
        results = [stats.evaluate(pairs, "a", "b", control=True)]
    else:
        results = [stats.evaluate(pairs, "base", "candidate"), stats.evaluate(pairs, "cpp", "candidate", minimum=1.0)]
    medians = {label: {metric: statistics.median(pair["rows"][label][metric] for pair in pairs) for metric in stats.METRICS} for label in binaries}
    summary = {**metadata, "medians": medians, "comparisons": results, "accepted": all(result["accepted"] for result in results)}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary), flush=True)
    if not summary["accepted"]:
        raise SystemExit("checkpoint statistical gate failed; all evidence retained")


if __name__ == "__main__":
    main()
