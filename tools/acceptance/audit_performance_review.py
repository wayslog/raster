#!/usr/bin/env python3
"""Recalculation of complete performance review materials;Consistent data does not mean that the value returns to within the original review line.."""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import statistics

from audit_cost_attribution import row
from audit_result_pool import read_rows, require
from compat import canonicalize, canonicalize_text, read_json, resolve_path


def audit_p3(root, manifest):
    plan = read_json(root / "plan.json")
    require(plan["current commit"] == manifest["Current implementation"] and plan["old commit"] == manifest["oldP3"], "P3 Compare version error")
    require(plan["operation_count"] == 200000 and plan["Each set of samples at each end"] == 12, "P3 Workload is changed")
    require(plan["Throughput review line"] == 0.75 and plan["P99review_line"] == 1.3, "The original review line has been changed")
    require(plan["order"] == [["old", "current"], ["current", "old"], ["current", "old"], ["old", "current"]], "Wrong pairing order")
    require(plan["driveSHA256"] == hashlib.sha256((root / "driver.rs").read_bytes()).hexdigest(), "P3 Driver inconsistency")
    groups = list(itertools.product(["atomicu64", "variable_length32to512bytes"], ["256uniform_keys", "single_key_hotspot"], ["1", "4"]))
    axes = {(*group, str(n)) for group in groups for n in range(1, 4)}
    data = {name: [r for n in range(4) for r in read_rows(root / "full" / f"{n}-{name}.csv", axes, 200000)]
            for name in ["old", "current"]}
    require(len(list((root / "full").glob("*.csv"))) == 8, "P3 The original batch number does not match")
    reports = []
    for group in groups:
        medians = {}
        for name, rows in data.items():
            selected = [r for r in rows if tuple(r[k] for k in ["layout", "distribution", "threads"]) == group]
            require(len(selected) == 12, "P3 Sample missing")
            medians[name] = {k: statistics.median(float(r[k]) for r in selected)
                             for k in ["operations_per_second", "P50nanoseconds", "P95nanoseconds", "P99nanoseconds", "Deny retry"]}
        speed = medians["current"]["operations_per_second"] / medians["old"]["operations_per_second"]
        latency = medians["current"]["P99nanoseconds"] / medians["old"]["P99nanoseconds"]
        reports.append({**dict(zip(["layout", "distribution", "threads"], group)), "samples_per_side": 12, "median": medians,
                        "throughput_ratio": speed, "P99ratio": latency, "trigger review": speed < 0.75 or latency > 1.3})
    require(reports == read_json(root / "full/comparison.json"), "P3 Median or review conclusions are inconsistent")
    expected = {"page bytes": "33554432", "memory page": "4", "variable ratio": "0.9", "index bucket": "1024", "Cache enabled": "false",
                "cache capacity": "0", "automatic_compression": "false", "maintenance worker": "1", "Number of sessions": "96", "pending limit": "1024",
                "result limit": "1024", "Log pre-allocation": "false", "segment bytes": "1073741824"}
    for name in ["old", "current"]:
        with (root / (name + "-config.csv")).open(newline="") as source:
            settings = [canonicalize(row) for row in csv.DictReader(source)]
        require(len(settings) == 13 and {r["Project"]: r["value"] for r in settings} == expected, "Common default configuration is inconsistent")
        with (root / (name + "-alloc.csv")).open(newline="") as source:
            allocations = [canonicalize(row) for row in csv.DictReader(source)]
        require(allocations == [{"operation_count": "200000", "Assign events": "200000", "release_event": "200000", "allocate bytes": "1600000"}],
                "Synchronization hotspot allocation evidence does not match")
    return reports


def audit_cost(root, parent):
    manifest = read_json(root / "manifest.json")
    require(manifest["version"] == 1 and manifest["baseline_commit"] == parent["Current implementation"], "Wrong cost diagnostic version")
    flags = list(itertools.product([False, True], repeat=3))
    actual = [(v["Remove email registration and logout"], v["Remove version table registration and waiting"], v["Remove write entry phase observation"]) for v in manifest["variant"]]
    require(actual == flags, "Protocol combination missing or duplicated")
    require(len(list((root / "paired").glob("*.csv"))) == 50
            and len(list((root / "old-pair").glob("*.csv"))) == 6, "The number of restricted loops does not match")
    base = "m0v0o0"
    require((root / (base + ".patch")).read_bytes() == b"", "Cost baseline includes diagnostic modifications")
    reports = []
    for v, (mailbox, version, observation) in zip(manifest["variant"], flags):
        name = f"m{int(mailbox)}v{int(version)}o{int(observation)}"
        require(v["Name"] == name, "Diagnostic combination name does not match")
        row(root / "paired" / f"smoke-{name}.csv", 1000000)
        if name == base:
            continue
        data = {n: [row(root / "paired" / f"{name}-{trial}-{n}.csv", 20000000) for trial in range(3)]
                for n in [base, name]}
        speeds = {n: statistics.median(float(r["operations_per_second"]) for r in rows) for n, rows in data.items()}
        nanos = {n: statistics.median(float(r["elapsed_nanoseconds"]) / int(r["operation_count"]) for r in rows) for n, rows in data.items()}
        reports.append({**v, "samples_per_side": 3, "Median throughput": speeds, "median_nanoseconds_per_operation": nanos,
                        "throughput_ratio": speeds[name] / speeds[base], "Nanoseconds saved per operation": nanos[base] - nanos[name]})
    require(reports == read_json(root / "paired/comparison.json"), "Median agreement costs are inconsistent")
    nanos = {name: statistics.median(float(row(root / "old-pair" / f"{n}-{name}.csv", 20000000)["elapsed_nanoseconds"]) / 20000000
                                    for n in range(3)) for name in [base, "old-p3"]}
    old = read_json(root / "old-pair/comparison.json")
    require(old["samples_per_side"] == 3 and old["median_nanoseconds_per_operation"] == nanos and old["current_vs_oldP3cost_ratio"] == nanos[base] / nanos["old-p3"],
            "Restricted legacy comparison inconsistent")
    require(old["binarySHA256"][base] == manifest["variant"][0]["binarySHA256"]
            and old["binarySHA256"]["old-p3"] == manifest["supplementary_binarySHA256"]["old-p3"], "Restricted comparison binary inconsistency")
    cases = [
        ("m1v0o0", "engine::io_hub::tests::after_other_session_polling_the_results_will_remain_in_the_original_mailbox_and_cannot_be_collected_if_the_identity_is_wrong", "Err(Error::Busy)"),
        ("m0v1o0", "engine::version_permit::tests::the_new_version_can_be_executed_only_after_all_old_versions_have_been_exited_and_different_keys_do_not_block_each_other", "!new.ready().unwrap()"),
        ("m0v0o1", "engine::pending_read_tests::commits_and_polls_automatically_observe_versions_and_old_requests_remain_sharded", "on an `Err` value: Busy"),
    ]
    recorded = read_json(root / "protocols/comparison.json")
    require(recorded == [{"variant": n, "real test": t, "Normal exit code": 0, "Exit code after removal": 101} for n, t, _ in cases],
            "Must preserve true protocol failure")
    for name, test, failure in cases:
        normal = canonicalize_text((root / "protocols" / (name + "-normal.txt")).read_text())
        control = canonicalize_text((root / "protocols" / (name + "-control.txt")).read_text())
        require(test in normal and "test result: ok. 1 passed; 0 failed;" in normal, "Normal implementation does not actually pass protocol testing")
        require(test in control and "test result: FAILED. 0 passed; 1 failed;" in control and failure in control,
                "Diagnostic variant does not occur as expected protocol fails")
    return reports


def audit(root):
    manifest = read_json(root / "manifest.json")
    require(manifest["version"] == 1 and manifest["Raw throughput review line"] == 0.75 and manifest["originalP99review_line"] == 1.3,
            "Review version or original condition changes")
    for name, expected in manifest["fileSHA256"].items():
        file = resolve_path(root, name)
        require(file.resolve().is_relative_to(root.resolve()), "The archive path is illegal")
        require(hashlib.sha256(file.read_bytes()).hexdigest() == expected, "Archive hash error:" + name)
    return audit_p3(root / "p3", manifest), audit_cost(root / "cost", manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Complete review of archive directory")
    args = parser.parse_args()
    p3, _ = audit(args.directory)
    print("Materials consistent:192 Yukihara P3 control,56 restricted loop,Configure and assign output,Three sets of protocol counterexamples.")
    print("The original review line is still triggered:", sum(r["trigger review"] for r in p3), "group;Please see the supporting report for acceptance explanation..")


if __name__ == "__main__":
    main()
