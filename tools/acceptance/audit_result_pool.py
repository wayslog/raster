#!/usr/bin/env python3
"""Review the results of the budget pool experiment with raw data;Clear distinction between data consistency,Calibration fluctuations and performance review."""
import argparse
import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics

from compat import canonicalize, read_json, resolve_path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_rows(path, axes, count):
    with path.open(newline="") as stream:
        rows = [canonicalize(row) for row in csv.DictReader(stream)]
    require(len(rows) == len(axes) and {tuple(r[k] for k in ["layout", "distribution", "threads", "round"]) for r in rows} == axes,
            "Scene missing,Duplicate or inconsistent with fixed grouping:" + path.name)
    for row in rows:
        require(row["operation_count"] == str(count) and int(row["Deny retry"]) >= 0, "Operand or retry error")
        require(0 < int(row["P50nanoseconds"]) <= int(row["P95nanoseconds"]) <= int(row["P99nanoseconds"]), "Delay quantile error")
        require(math.isfinite(float(row["operations_per_second"])) and float(row["operations_per_second"]) > 0, "Throughput value error")
    return rows


def compare(data, metrics):
    require(all(len(rows) == 12 for rows in data.values()), "Each end must retain all 12 samples")
    medians = {name: {metric: statistics.median(float(row[metric]) for row in rows) for metric in metrics}
               for name, rows in data.items()}
    return {"samples_per_side": 12, "median": medians,
            "throughput_ratio": medians["candidate"]["operations_per_second"] / medians["baseline"]["operations_per_second"],
            "P99ratio": medians["candidate"]["P99nanoseconds"] / medians["baseline"]["P99nanoseconds"]}


def audit(root):
    manifest = read_json(root / "manifest.json")
    require(manifest["version"] == 1, "Unsupported archive version")
    for name, digest in manifest["fileSHA256"].items():
        path = resolve_path(root, name)
        require(path.resolve().is_relative_to(root.resolve()) and path.is_file(), "The archive path is illegal")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "Hashes are inconsistent:" + name)
    groups = list(itertools.product(["atomicu64", "variable_length32to512bytes"], ["256uniform_keys", "single_key_hotspot"], ["1", "4"]))
    axes = {(*group, str(round_)) for group in groups for round_ in range(1, 4)}
    data = {"baseline": [], "candidate": []}
    for trial in range(4):
        for name in data:
            data[name].extend(read_rows(root / "full" / f"{trial}-{name}.csv", axes, 200000))
    full = []
    for group in groups:
        selected = {name: [r for r in rows if tuple(r[k] for k in ["layout", "distribution", "threads"]) == group]
                    for name, rows in data.items()}
        full.append({**dict(zip(["layout", "distribution", "threads"], group)), **compare(selected, ["operations_per_second", "P99nanoseconds"])})
    require(full == read_json(root / "full" / "comparison.json"), "Complete matrix median does not match")
    focused = {}
    axes = {("variable_length32to512bytes", "single_key_hotspot", "4", str(round_)) for round_ in range(1, 4)}
    for folder, count in [("focused", 200000), ("long", 2000000)]:
        reports = {}
        for mode in ["calibrationAA", "reviewAB"]:
            data = {"baseline": [], "candidate": []}
            for trial in range(4):
                for name in data:
                    data[name].extend(read_rows(resolve_path(root / folder, f"{mode}-{trial}-{name}.csv"), axes, count))
            reports[mode] = compare(data, ["operations_per_second", "P99nanoseconds", "Deny retry"])
        require(reports == read_json(root / folder / "comparison.json"), "Orientation calibration median does not match")
        focused[folder] = reports
    for name, events, bytes_ in [("baseline", 400000, 4800000), ("candidate", 200000, 1600000)]:
        with (root / f"{name}-alloc.csv").open(newline="") as stream:
            rows = [canonicalize(row) for row in csv.DictReader(stream)]
        require(rows == [{"operation_count": "200000", "Assign events": str(events), "release_event": str(events), "allocate bytes": str(bytes_)}],
                "Assignment diagnostic does not match fixed expectations")
    return full, focused


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Archive experiment directory")
    args = parser.parse_args()
    full, focused = audit(args.directory)
    for row in full:
        if row["throughput_ratio"] < .75 or row["P99ratio"] > 1.3:
            print("Complete matrix triggers the original core line:", row["layout"], row["distribution"], row["threads"], row["throughput_ratio"], row["P99ratio"])
    for folder, reports in focused.items():
        for mode, row in reports.items():
            print(folder, mode, "throughput_ratio", row["throughput_ratio"], "P99ratio", row["P99ratio"])
    print("Data review and consistency:Keep full matrix,Independent calibration and comparison of two operating quantities;This conclusion does not mean P3 Performance acceptance passed.")


if __name__ == "__main__":
    main()
