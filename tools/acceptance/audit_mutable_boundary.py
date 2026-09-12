#!/usr/bin/env python3
"""Paired Experiments Reviewing Variable Bounds Queries;Share existing matrix formats,Keep all original samples."""
import argparse
import hashlib
from pathlib import Path
import statistics

from audit_serial_precheck import audit
from audit_result_pool import read_rows, require
from compat import read_json


def audit_p3(root):
    manifest = read_json(root / "manifest.json")
    report = read_json(root / "p3/comparison.json")
    require(report["driveSHA256"] == hashlib.sha256((root / "driver.rs").read_bytes()).hexdigest(), "P3 Driver inconsistency")
    current = report["result"]["current"]
    require(current["commit"] == manifest["baseline_commit"], "P3 Candidate baseline does not match")
    require(current["candidate_patchSHA256"] == hashlib.sha256((root / "candidate.patch").read_bytes()).hexdigest(), "P3 Candidate patch does not match")
    require(current["binarySHA256"] == manifest["binarySHA256"]["candidate"], "P3 Candidate binary does not match")
    require(report["result"]["old"]["commit"] == "7f2f03b646e85b30c81f31405e3dc2325086da2a", "old P3 Submission does not match")
    axes = {("atomicu64", "single_key_hotspot", "1", str(n)) for n in range(1, 4)}
    values = {}
    for name in ["current", "old"]:
        rows = read_rows(root / "p3" / (name + ".csv"), axes, 200000)
        require(all(r["Deny retry"] == "0" for r in rows), "P3 Hotspot appears and refuses to retry")
        values[name] = {key: statistics.median(float(r[key]) for r in rows)
                        for key in ["operations_per_second", "P50nanoseconds", "P95nanoseconds", "P99nanoseconds"]}
        require(values[name] == report["result"][name]["median"], "P3 Median does not match")
    speed = values["current"]["operations_per_second"] / values["old"]["operations_per_second"]
    latency = values["current"]["P99nanoseconds"] / values["old"]["P99nanoseconds"]
    require(speed == report["throughput_ratio"] and latency == report["P99ratio"], "P3 The ratio does not match")
    require(int(speed < 0.75 or latency > 1.3) == report["exit_code"], "P3 The original review conclusion is inconsistent with")
    return report["exit_code"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Archive experiment directory")
    args = parser.parse_args()
    for row in audit(args.directory):
        print(row["layout"], row["distribution"], row["threads"], "throughput_ratio", row["throughput_ratio"], "P99ratio", row["P99ratio"])
    print("Original P3 The control data is consistent,Review exit code:", audit_p3(args.directory))
    print("Variable Boundary Archive Consistency:192 row complete matrix and 36 line supplement;does not mean old P3 Overall review completed.")


if __name__ == "__main__":
    main()
