#!/usr/bin/env python3
"""Check real output and timing of constrained cost diagnostics;Cannot be used to prove engine compatibility for removal protocols."""
import argparse
import csv
import hashlib
import itertools
import math
from pathlib import Path
import statistics

from audit_result_pool import require
from compat import canonicalize, read_json, resolve_path


def row(path, count):
    with path.open(newline="") as stream:
        rows = [canonicalize(row) for row in csv.DictReader(stream)]
    require(len(rows) == 1, "The diagnostic must output exactly one complete cycle")
    result = rows[0]
    require(result["operation_count"] == str(count) and result["warmup"] == "1000", "Diagnosing workload errors")
    require(int(result["Read results"]) == count + 1000 and int(result["Accept serial number"]) == count + 1001,
            "Final resident value or accept progress error")
    nanos, speed = float(result["elapsed_nanoseconds"]), float(result["operations_per_second"])
    require(math.isfinite(nanos) and nanos > 0 and math.isfinite(speed) and speed > 0, "Diagnostic timing is invalid")
    require(abs(speed - count * 1e9 / nanos) <= 1, "Throughput and timing mismatch")
    return result


def audit(root):
    manifest = read_json(root / "manifest.json")
    require(manifest["version"] == 1 and len(manifest["variant"]) == 8, "Wrong diagnostic version or combination number")
    for name, digest in manifest["fileSHA256"].items():
        path = resolve_path(root, name)
        require(path.resolve().is_relative_to(root.resolve()), "Illegal diagnostic path")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "Diagnosing hash inconsistencies:" + name)
    expected = list(itertools.product([False, True], repeat=3))
    flags = [(v["Remove email registration and logout"], v["Remove version table registration and waiting"], v["Remove active request counting"])
             for v in manifest["variant"]]
    require(flags == expected, "Eight protocol combinations missing or duplicated")
    base = "m0v0a0"
    reports = []
    for v, (mailbox, version, activity) in zip(manifest["variant"], expected):
        name = f"m{int(mailbox)}v{int(version)}a{int(activity)}"
        require(v["Name"] == name, "Combination name does not match")
        row(root / "paired" / f"smoke-{name}.csv", 1000000)
        if name == base:
            continue
        data = {n: [row(root / "paired" / f"{name}-{trial}-{n}.csv", 20000000) for trial in range(3)]
                for n in [base, name]}
        speeds = {n: statistics.median(float(r["operations_per_second"]) for r in rows) for n, rows in data.items()}
        nanos = {n: statistics.median(float(r["elapsed_nanoseconds"]) / int(r["operation_count"]) for r in rows)
                 for n, rows in data.items()}
        reports.append({**v, "samples_per_side": 3, "Median throughput": speeds, "median_nanoseconds_per_operation": nanos,
                        "throughput_ratio": speeds[name] / speeds[base], "Nanoseconds saved per operation": nanos[base] - nanos[name]})
    require(reports == read_json(root / "paired" / "comparison.json"), "Portfolio medians are inconsistent")
    data = {n: [row(root / "old-pair" / f"{trial}-{n}.csv", 20000000) for trial in range(3)]
            for n in [base, "old-p3"]}
    nanos = {n: statistics.median(float(r["elapsed_nanoseconds"]) / int(r["operation_count"]) for r in rows)
             for n, rows in data.items()}
    old = read_json(root / "old-pair" / "comparison.json")
    require(old["samples_per_side"] == 3 and old["median_nanoseconds_per_operation"] == nanos
            and old["current_vs_oldP3cost_ratio"] == nanos[base] / nanos["old-p3"], "old P3 Restricted comparison inconsistent")
    require(old["binarySHA256"][base] == manifest["variant"][0]["binarySHA256"]
            and old["binarySHA256"]["old-p3"] == manifest["supplementary_binarySHA256"]["old-p3"], "Old control binary inconsistent")
    for name in ["profile-baseline", "old-p3"]:
        row(root / "profiles" / f"{name}-profile-run.csv", 40000000)
        text = (root / "profiles" / f"{name}-sample.txt").read_text()
        require("Call graph:" in text and "Sort by top of stack" in text, "Sampling did not produce a call graph")
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Diagnostic archive directory")
    args = parser.parse_args()
    audit(args.directory)
    print("Diagnostic archive consistent:8 smoking,42 paired loops,6 compare_the_old_version_and2complete_value_check_after_sampling;It does not mean that the compatibility acceptance has been passed.")


if __name__ == "__main__":
    main()
