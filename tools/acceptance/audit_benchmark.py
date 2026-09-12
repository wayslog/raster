#!/usr/bin/env python3
"""Independent verification of the complete performance matrix,Fixed input,success/Missing count and write amplification;No claims of performance regression passing."""
import argparse
import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
import statistics
import subprocess

SEED = 0x7261737465720001
KEYS = 2048
OPERATIONS = 16384
KINDS = ["read", "write", "RMW", "delete"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def initial_value(variable, ordinal):
    return bytes([ordinal % 256]) * ((ordinal % 16 + 1) * 32) if variable else (SEED + ordinal) % (1 << 64)


def payload(value):
    return len(value) if isinstance(value, bytes) else 8


def trace_audit(path, variable, hot, threads):
    raw = path.read_bytes()
    lines = raw.decode().splitlines()
    require(lines[0] == f"raster-trace 1 {SEED}", "Track header or seed error")
    require(len(lines) == 1 + KEYS + OPERATIONS, "Preload and business track length errors")
    per_key = KEYS // threads
    per_phase = OPERATIONS // threads // 4
    records = {}
    application = success = missing = cursor = 0
    for worker in range(threads):
        for serial in range(per_key + 4 * per_phase):
            fields = lines[cursor + 1].split()
            cursor += 1
            phase = -1 if serial < per_key else (serial - per_key) // per_phase
            i = serial if phase == -1 else (serial - per_key) % per_phase
            local = i if phase == -1 else 0 if hot else (i * 17 % (per_key // 2)) * 2 if phase == 3 else i * 17 % per_key
            key = worker * per_key + local
            require(fields[:3] == [str(worker), str(serial), key.to_bytes(8, "little").hex()], "threads,Serial number or key distribution error")
            op = fields[3:]
            expected_kind = "upsert" if phase == -1 else ["read", "upsert", "rmw", "delete"][phase]
            require(op[0] == expected_kind, "Wrong operation grouping or scaling")
            if expected_kind in ("upsert", "rmw"):
                data = op[1:] if expected_kind == "upsert" else op[2:]
                require(len(data) == 2 and data[0] == ("b" if variable else "u"), "Value encoding type error")
                value = bytes.fromhex(data[1]) if variable else int(data[1])
                expected = initial_value(variable, key if phase == -1 else worker * per_key + i + 7) if expected_kind == "upsert" else b"\xa5" if variable else 1
                require(value == expected, "Fixed value input changes")
                if expected_kind == "rmw":
                    require(op[1] == "1", "RMW Create option error")
                    old = records[key]
                    require(old is not None, "Fixed RMW stage should have old value")
                    records[key] = old + value if variable else (old + value) % (1 << 64)
                else:
                    records[key] = value
                application += payload(value)
                success += phase != -1
            elif expected_kind == "read":
                require(op == ["read", "1"] and records[key] is not None, "Fixed read phase or tombstone option bug")
                success += 1
            else:
                require(op == ["delete", "0"], "Delete option error")
                if records[key] is None:
                    missing += 1
                else:
                    records[key] = None
                    application += 8
                    success += 1
    require(len(records) == KEYS and success + missing == OPERATIONS, "Model results are incomplete")
    return {"inputSHA256": hashlib.sha256(raw).hexdigest(), "application_write_bytes": application,
            "success": success, "missing": missing, "tombstone": 0,
            "Recovery value": sum(v is not None for v in records.values()),
            "Restoring the number of tombstones": sum(v is None for v in records.values())}


def verify_matrix(root):
    with (root / "matrix.csv").open(newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames is not None and len(reader.fieldnames) == 49 and len(set(reader.fieldnames)) == 49,
                "CSV Column number or uniqueness error")
        rows = list(reader)
    axes = list(itertools.product(["atomicu64", "variable_bytes"], ["uniform", "per_thread_hotspot"], [1, 4], ["memory_only", "over_memory"], [1, 2, 3]))
    keys = [(r["layout"], r["distribution"], int(r["threads"]), r["storage"], int(r["round"])) for r in rows]
    require(len(rows) == 48 and set(keys) == set(axes), "must have all 48 group without duplication")
    require(not any(p.is_dir() for p in root.iterdir()), "Success matrix still has uncleaned composition directories")
    traces = {}
    for row, (layout, distribution, threads, storage, _) in zip(rows, keys):
        variable, hot = layout == "variable_bytes", distribution == "per_thread_hotspot"
        identity = f"{'bytes' if variable else 'u64'}-{'hot' if hot else 'uniform'}-t{threads}"
        require(row["input_id"] == identity, "Input ID does not match")
        if identity not in traces:
            traces[identity] = trace_audit(root / f"{identity}.trace", variable, hot, threads)
        expected = traces[identity]
        numbers = {name: int(value) for name, value in row.items() if name not in ["layout", "distribution", "storage", "input_id", "operations_per_second", "write_amplification_including_preload_checkpoint"]}
        require(all(n >= 0 for n in numbers.values()), "Count cannot be negative")
        require(numbers["seeds"] == SEED and numbers["Number of keys"] == KEYS and numbers["operation_count"] == OPERATIONS, "Fixed run parameter error")
        for name in ["application_write_bytes", "tombstone"]:
            require(numbers[name] == expected[name], f"{identity} of {name} Not consistent with the independent model")
        # This fixed input is completed first Read/Upsert/RMW,Delete normally;The only mutable return comes from duplicate removal.
        # The first deletion of active values must be successful.,Duplicate removal can be successful or missing,The total number of transactions remains exactly conserved.
        require(expected["success"] <= numbers["success"] <= expected["success"] + expected["missing"], "The number of successes exceeds the allowable range of ordinary blind deletion")
        require(0 <= numbers["missing"] <= expected["missing"] and numbers["success"] + numbers["missing"] == OPERATIONS, "The number is missing or the total number of transactions does not match")
        require(0 < numbers["P50nanoseconds"] <= numbers["P95nanoseconds"] <= numbers["P99nanoseconds"], "Quantile error")
        for kind in KINDS:
            require(numbers[f"{kind}operation_count"] == OPERATIONS // 4 and numbers[f"{kind}P99nanoseconds"] > 0, "Four operations sampling is incomplete")
            require(numbers[f"{kind}pending_count"] <= OPERATIONS // 4, "The number of pending transactions exceeds the number of transactions")
        for direction in ["read", "write"]:
            persistent = "written" if direction == "write" else direction
            require(
                numbers[f"Preloaded cumulative {direction} bytes"]
                <= numbers[f"post_workload_{direction}_bytes"]
                <= numbers[f"Persistent accumulated {persistent} bytes"],
                "cumulative I/O bytes moved backward",
            )
        require(numbers["Post-transaction resident page bytes"] <= numbers["window_bytes"], "Residency page exceeds budget")
        if storage == "memory_only":
            require(numbers["window_bytes"] == 64 * 1024 * 1024 and numbers["Post-business log span"] < numbers["window_bytes"], "memory-only window does not match")
            require(numbers["post_workload_read_bytes"] == numbers["post_workload_write_bytes"] == 0 and all(numbers[f"{kind}pending_count"] == 0 for kind in KINDS), "Pure memory business occurs I/O or hang")
        else:
            require(numbers["window_bytes"] == 64 * 1024 and numbers["Preloaded log span"] > numbers["window_bytes"], "The data does not exceed the fixed retention window")
            require(numbers["post_workload_read_bytes"] > 0 and numbers["post_workload_write_bytes"] > 0 and sum(numbers[f"{kind}pending_count"] for kind in KINDS) > 0, "Hypermemory path not really running")
        require(numbers["Resume reading bytes"] > 0 and numbers["Resume writing bytes"] > 0 and numbers["Recovery nanoseconds"] > 0 and numbers["checkpoint nanoseconds"] > 0, "Checkpoint or recovery measurement missing")
        require(all(numbers[f"{stage}RSSbytes"] > 0 for stage in ["pre_installed", "after_business", "after_checkpoint", "after_recovery"]), "RSS Missing samples")
        throughput, amplification = float(row["operations_per_second"]), float(row["write_amplification_including_preload_checkpoint"])
        require(math.isfinite(throughput) and throughput > 0 and math.isfinite(amplification), "Non-finite performance indicators")
        require(abs(amplification - numbers["Persistent accumulated written bytes"] / numbers["application_write_bytes"]) <= 0.00000051, "Write amplification calculation error")
    require(len(traces) == 8 and len(list(root.glob("*.trace"))) == 8, "There should be eight fixed inputs")
    medians = []
    for layout, distribution, threads, storage in itertools.product(["atomicu64", "variable_bytes"], ["uniform", "per_thread_hotspot"], [1, 4], ["memory_only", "over_memory"]):
        group = [r for r in rows if (r["layout"], r["distribution"], int(r["threads"]), r["storage"]) == (layout, distribution, threads, storage)]
        metrics = {name: statistics.median(float(r[name]) for r in group) for name in [
            "operations_per_second", "P50nanoseconds", "P95nanoseconds", "P99nanoseconds", "write_amplification_including_preload_checkpoint", "Recovery nanoseconds",
            "after_businessRSSbytes", "Post-transaction resident page bytes", "Persistent accumulated read bytes", "Persistent accumulated written bytes",
        ]}
        medians.append({"layout": layout, "distribution": distribution, "threads": threads, "storage": storage, **metrics})
    return {"complete scene": 48, "business operations": 48 * OPERATIONS, "Restore check key": 48 * KEYS,
            "input": traces, "Median of three rounds": medians, "Original performance rollback acceptance": "Not completed"}


def verify_native(root, sha, run_id):
    run = json.loads(subprocess.check_output(
        ["gh", "run", "view", run_id, "--json", "headSha,status,conclusion,jobs"],
        text=True, timeout=60,
    ))
    require(run["headSha"] == sha and run["status"] == "completed" and run["conclusion"] == "success",
            "Specifies that the submitted performance run must end successfully")
    require(len(run["jobs"]) == 2 and all(job["conclusion"] == "success" for job in run["jobs"]),
            "Both native performance tasks must both succeed")
    verified = []
    inputs = None
    for platform, target in [("ubuntu-24.04", "x86_64-unknown-linux-gnu"), ("macos-14", "aarch64-apple-darwin")]:
        folder = root / f"p9-benchmark-{platform}"
        env = (folder / "environment.txt").read_text()
        checks = (folder / "checks.txt").read_text()
        require(env.splitlines()[0] == sha and "release: 1.98.1" in env and f"host: {target}" in env,
                "Product submission,Toolchain or native target no match")
        require("features:all;mode:release;complete matrix:48;Each group of business operations:16384" in env,
                "Performance configuration mismatch")
        require(len(re.findall(r"test result: ok\. 3 passed; 0 failed; 0 ignored;", checks)) == 1,
                "Measurement,The three tests of fixed input and blind deletion contract must be actually passed.")
        expected_ids = {f"{layout}-{distribution}-t{threads}-{storage}-r{round_}"
                        for layout, distribution, threads, storage, round_ in itertools.product(
                            ["u64", "bytes"], ["uniform", "hot"], [1, 4], ["memory", "disk"], [1, 2, 3])}
        completed = re.findall(r"^benchmark (\S+) passed:16384 steps_four_operations,[14] session resume,2048 keys_and_tombstone_validation$", checks, re.M)
        require(len(completed) == 48 and set(completed) == expected_ids, "Group-by-group business and recovery output is incomplete")
        require(checks.count("Performance matrix business verification passed:48 group;") == 1 and "test result: FAILED" not in checks,
                "Missing final full matrix result")
        matrix = verify_matrix(folder / "matrix")
        require(matrix == json.loads((folder / "matrix/audit.json").read_text()),
                "The results of independent review after downloading are inconsistent with those of native saving.")
        require(inputs is None or inputs == matrix["input"], "Fixed input inconsistency between the two platforms")
        inputs = matrix["input"]
        verified.append({"Platform": platform, "target": target, **matrix})
    return {"sha": sha, "run": run, "verified": verified}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Complete matrix catalog")
    parser.add_argument("--sha", help="Complete submission of native download products SHA;and --run-id Same use")
    parser.add_argument("--run-id", help="Native performance workflow number;and --sha Same use")
    args = parser.parse_args()
    require(bool(args.sha) == bool(args.run_id), "Native auditing must specify both commit and run numbers")
    result = verify_native(args.directory, args.sha, args.run_id) if args.sha else verify_matrix(args.directory)
    with (args.directory / "audit.json").open("x") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    platforms = 2 if args.sha else 1
    print(f"Complete matrix reviewed:{48 * platforms} group,{48 * OPERATIONS * platforms} business operations,{48 * KEYS * platforms} recovery key verification;This matrix does not replace independent original performance regression review.")


if __name__ == "__main__":
    main()
