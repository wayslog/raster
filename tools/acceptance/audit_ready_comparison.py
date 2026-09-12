#!/usr/bin/env python3
"""Review the hash of the archive write comparison,Full sample and median;Partial income cannot be regarded as P3 Overall experience."""
import argparse
import hashlib
import itertools
import math
from pathlib import Path
import statistics

from compat import read_json, read_rows, resolve_path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def audit(root):
    manifest = read_json(root / "manifest.json")
    require(manifest["version"] == 1 and manifest["Each set of samples at each end"] == 12, "Wrong control version or number of samples")
    for name, digest in manifest["fileSHA256"].items():
        require(Path(name).name == name, "The archive name cannot jump out of the directory")
        require(hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, "Archive hash mismatch:" + name)
    groups = list(itertools.product(["atomicu64", "variable_length32to512bytes"], ["256uniform_keys", "single_key_hotspot"], ["1", "4"]))
    axes = {(*group, str(round_)) for group in groups for round_ in range(1, 4)}
    data = {"baseline": [], "candidate": []}
    for trial in range(4):
        for name in data:
            rows = read_rows(resolve_path(root, f"{trial}-{name}.csv"))
            require(len(rows) == 24 and {tuple(row[k] for k in ["layout", "distribution", "threads", "round"]) for row in rows} == axes,
                    "Each execution must contain all 24 unique scene samples")
            for row in rows:
                require(row["operation_count"] == "200000" and int(row["Deny retry"]) >= 0, "Operand or retry count error")
                require(0 < int(row["P50nanoseconds"]) <= int(row["P95nanoseconds"]) <= int(row["P99nanoseconds"]), "Delay quantile error")
                speed = float(row["operations_per_second"])
                require(math.isfinite(speed) and speed > 0, "Throughput must be a finite positive number")
            data[name].extend(rows)
    result = []
    for layout, distribution, threads in groups:
        medians = {}
        for name, rows in data.items():
            samples = [r for r in rows if (r["layout"], r["distribution"], r["threads"]) == (layout, distribution, threads)]
            require(len(samples) == 12, "The original sample that triggered the review cannot be discarded")
            medians[name] = {metric: statistics.median(float(row[metric]) for row in samples)
                             for metric in ["operations_per_second", "P99nanoseconds"]}
        result.append({"layout": layout, "distribution": distribution, "threads": threads, "samples_per_side": 12,
                       "median": medians,
                       "throughput_ratio": medians["candidate"]["operations_per_second"] / medians["baseline"]["operations_per_second"],
                       "P99ratio": medians["candidate"]["P99nanoseconds"] / medians["baseline"]["P99nanoseconds"]})
    require(result == read_json(root / "comparison.json"), "Recalculated median is inconsistent with archived reports")
    for name, events, bytes_ in [("baseline", 600000, 20800000), ("candidate", 400000, 4800000)]:
        rows = read_rows(root / f"{name}-alloc.csv")
        require(rows == [{"operation_count": "200000", "Assign events": str(events), "release_event": str(events), "allocate bytes": str(bytes_)}],
                "This fixed allocation diagnosis does not match the original count")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Archive reference directory")
    args = parser.parse_args()
    audit(args.directory)
    print("Archive review passed:8 group,Each end per group 12 real samples;Assign events consistent with all medians,P3 Overall acceptance has not yet been completed.")


if __name__ == "__main__":
    main()
