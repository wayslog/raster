#!/usr/bin/env python3
"""Complete paired samples for review sequence number pre-check;Partial gains do not replace the original P3 Performance review."""
import argparse
import hashlib
import itertools
from pathlib import Path
import statistics

from audit_result_pool import compare, read_rows, require
from compat import read_json, resolve_path


def audit(root):
    manifest = read_json(root / "manifest.json")
    require(manifest["version"] == 1, "Archive version error")
    for name, digest in manifest["fileSHA256"].items():
        path = resolve_path(root, name)
        require(path.resolve().is_relative_to(root.resolve()), "The archive path is illegal")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "Hashes are inconsistent:" + name)
    groups = list(itertools.product(["atomicu64", "variable_length32to512bytes"], ["256uniform_keys", "single_key_hotspot"], ["1", "4"]))
    axes = {(*group, str(round_)) for group in groups for round_ in range(1, 4)}
    data = {"baseline": [], "candidate": []}
    for trial in range(4):
        for name in data:
            data[name].extend(read_rows(root / "full" / f"{trial}-{name}.csv", axes, 200000))
    reports = []
    for group in groups:
        selected = {name: [r for r in rows if tuple(r[k] for k in ["layout", "distribution", "threads"]) == group]
                    for name, rows in data.items()}
        reports.append({**dict(zip(["layout", "distribution", "threads"], group)),
                        **compare(selected, ["operations_per_second", "P99nanoseconds"])})
    require(reports == read_json(root / "full" / "comparison.json"), "Complete matrix median does not match")
    axes = {("atomicu64", "single_key_hotspot", "1", str(round_)) for round_ in range(1, 4)}
    for folder in ["round-1", "round-2"]:
        report = read_json(root / folder / "comparison.json")
        require(report["Real samples at each end"] == 9 and report["baseline_commit"] == manifest["baseline_commit"], "Hotspot comparison caliber does not match")
        require(report["binarySHA256"] == manifest["binarySHA256"], "The full matrix and hotspot supplement must use the same binary")
        require(report["driveSHA256"] == hashlib.sha256((root / "driver.rs").read_bytes()).hexdigest(), "Driver versions are inconsistent")
        medians = {}
        for name in data:
            rows = []
            for trial in range(3):
                rows.extend(read_rows(root / folder / f"{trial}-{name}.csv", axes, 200000))
            require(len(rows) == 9 and all(row["Deny retry"] == "0" for row in rows), "Hotspot sample is incomplete")
            medians[name] = {k: statistics.median(float(row[k]) for row in rows)
                             for k in ["operations_per_second", "P50nanoseconds", "P95nanoseconds", "P99nanoseconds"]}
        require(medians == report["median"], "Hot spot median does not match")
        for name, metric in [("throughput_ratio", "operations_per_second"), ("P99ratio", "P99nanoseconds")]:
            require(report[name] == medians["candidate"][metric] / medians["baseline"][metric], "Hotspot ratio does not match")
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Archive experiment directory")
    args = parser.parse_args()
    for row in audit(args.directory):
        print(row["layout"], row["distribution"], row["threads"], "throughput_ratio", row["throughput_ratio"], "P99ratio", row["P99ratio"])
    print("Archive data is consistent:complete matrix 192 Samples and hot spots supplement 36 samples;does not mean old P3 Overall performance acceptance passed.")


if __name__ == "__main__":
    main()
