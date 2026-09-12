"""Review input for alternate measurements,order,business recovery,Binary Identity and Original Core Lines;The red value does not change to pass."""
import argparse
import csv
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import shutil
import statistics
import subprocess

from audit_benchmark import trace_audit, require
from compat import canonicalize, canonicalize_text, read_json

ORDERS = [("baseline", "candidate"), ("candidate", "baseline"), ("candidate", "baseline"), ("baseline", "candidate")]
METRICS = ["operations_per_second", "P99nanoseconds", "readP99nanoseconds", "writeP99nanoseconds", "RMWP99nanoseconds", "deleteP99nanoseconds", "post_workload_read_bytes", "post_workload_write_bytes"]
BASELINE = "dbe392884be723708a0e6bdc0011de83f493f917"
CANDIDATE = "a834f48b53865098c8325d81cc52934bb322c26d"


class TraceBytes:
    def __init__(self, data):
        self.data = data

    def read_bytes(self):
        return self.data


def verify(root):
    metadata = read_json(root / "metadata.json")
    run = metadata["run"]
    require(run["headSha"] == metadata["diagnostic_sha"] and run["status"] == "completed", "The run is not completed or the diagnostic submission does not match")
    require(len(run["jobs"]) == 1, "There must be a native matchmaking task")
    require(metadata["baseline"] == BASELINE and metadata["candidate"] == CANDIDATE, "Fixed core version mismatch")
    env = (root / "environment.txt").read_text()
    require(env.splitlines()[:3] == [metadata["diagnostic_sha"], CANDIDATE, BASELINE], "Construct core identity inconsistency")
    require("host: aarch64-apple-darwin" in env and "release: 1.98.1" in env, "Native tool chain does not match")
    require(set((root / "baseline-driver-files.txt").read_text().splitlines()) == {
        "examples/benchmark/main.rs", "examples/benchmark/scenario.rs", "tests/support/model.rs"
    }, "The old core can only replace three identical driver files")
    binaries = read_json(root / "binaries.json")
    group = metadata["group"]
    sides = ["baseline", "candidate", "control"] if group == "interleaved" else ["baseline", "candidate"]
    orders = list(itertools.permutations(sides)) if group == "interleaved" else ORDERS
    require(set(binaries) == set(sides), "Binary collection error")
    if group == "control":
        require(binaries["baseline"] == binaries["candidate"], "A/A Must use the same binary")
    else:
        require(binaries["baseline"] != binaries["candidate"], "A/B Must use different core binaries")
    if group == "interleaved":
        require(binaries["baseline"] == binaries["control"], "Same wheel control must reuse old binary")
    comparisons = read_json(root / "comparison.json")
    expected = {
        "focus": ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "bytes-hot-t1-memory-r1"],
        "control": ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "bytes-hot-t1-memory-r1"],
        "remaining": ["u64-uniform-t1-memory-r1", "u64-hot-t1-memory-r1", "u64-hot-t1-disk-r1", "bytes-uniform-t1-memory-r1", "bytes-hot-t4-disk-r1"],
        "interleaved": ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "u64-hot-t1-disk-r1"],
    }[group]
    require([item["case"] for item in comparisons] == expected, "Fixed scene is incomplete")
    count = 0
    for item in comparisons:
        case = item["case"]
        pairs = read_json(root / (case + "-pairs.json"))
        require(len(pairs) == len(orders), "Wrong number of pairs")
        identity = case.rsplit("-", 2)[0]
        raw = gzip.decompress((root / "inputs" / (identity + ".trace.gz")).read_bytes())
        require(hashlib.sha256(raw).hexdigest() == item["input_sha256"], "Input hash error")
        model = trace_audit(TraceBytes(raw), case.startswith("bytes-"), "-hot-" in case, 4 if "-t4-" in case else 1)
        for number, (pair, order) in enumerate(zip(pairs, orders), 1):
            require(pair["pair"] == number and pair["order"] == list(order), "Fixed order error")
            for side in sides:
                name = f"{case}-pair{number}-{side}"
                with (root / "runs" / (name + ".csv")).open() as stream:
                    rows = [canonicalize(row) for row in csv.DictReader(stream)]
                require(len(rows) == 1 and rows[0] == pair[side], "round by round CSV inconsistent with aggregation")
                row = rows[0]
                require(row["input_id"] == identity and int(row["operation_count"]) == 16384, "Input identifier or operand error")
                success, missing = int(row["success"]), int(row["missing"])
                require(model["success"] <= success <= 16384 and success + missing == 16384, "The business result exceeds the legal collection of blind deletion")
                require(int(row["application_write_bytes"]) == model["application_write_bytes"], "Write amplification denominator change")
                for kind in ["read", "write", "RMW", "delete"]:
                    require(int(row[kind + "operation_count"]) == 4096, "All four operations were not executed")
                log = canonicalize_text((root / "runs" / (name + ".txt")).read_text())
                require(f"benchmark {case} passed:16384 steps_four_operations," in log and "2048 keys_and_tombstone_validation" in log, "Business or recovery failed")
                require("panicked at" not in log, "Running panic")
                count += 1
        medians = {side: {metric: statistics.median(float(pair[side][metric]) for pair in pairs) for metric in METRICS} for side in sides}
        require(item["medians"] == medians, "Median error")
        tp = medians["candidate"]["operations_per_second"] / medians["baseline"]["operations_per_second"]
        p99 = medians["candidate"]["P99nanoseconds"] / medians["baseline"]["P99nanoseconds"]
        require(item["throughput_ratio"] == tp and item["p99_ratio"] == p99, "Version ratio error")
        require(item["review_required"] == (tp < .75 or p99 > 1.30), "The original review line has been changed")
        if group == "interleaved":
            tp = medians["control"]["operations_per_second"] / medians["baseline"]["operations_per_second"]
            p99 = medians["control"]["P99nanoseconds"] / medians["baseline"]["P99nanoseconds"]
            require(item["control_throughput_ratio"] == tp and item["control_p99_ratio"] == p99, "control ratio error")
            require(item["control_review_required"] == (tp < .75 or p99 > 1.30), "The control review line was changed")
    red = any(item["review_required"] or item.get("control_review_required", False) for item in comparisons)
    expected_conclusion = "failure" if red else "success"
    require(run["conclusion"] == expected_conclusion and run["jobs"][0]["conclusion"] == expected_conclusion, "The final state does not conform to the original value judgment")
    required_steps = {"Verify core identity and document native environment", "Both cores use the exact same benchmark driver", "Save all original pairing evidence"}
    steps = [step for step in run["jobs"][0]["steps"] if step["name"] in required_steps]
    require({step["name"] for step in steps} == required_steps and all(step["conclusion"] == "success" for step in steps), "Failed to build or save evidence")
    return {"group": group, "runs": count, "operations": count * 16384, "recovered_keys": count * 2048,
            "review_required_cases": [item["case"] for item in comparisons if item["review_required"]],
            "control_review_required_cases": [item["case"] for item in comparisons if item.get("control_review_required", False)]}


def collect(source, output, run_id, sha, group):
    run = json.loads(subprocess.check_output(["gh", "run", "view", run_id, "--json", "headSha,status,conclusion,jobs,url"], text=True))
    output.mkdir(parents=True, exist_ok=False)
    (output / "inputs").mkdir()
    (output / "runs").mkdir()
    for name in ["environment.txt", "baseline-driver-files.txt", "build.txt", "checks.txt"]:
        shutil.copyfile(source / name, output / name)
    group_file = source / "group.txt"
    if group_file.exists():
        require(group_file.read_text().strip() == group, "The scene group does not match")
    else:
        require((sha, group) in [("b1925d95de3627ab2b02c9fa8bc2da2c4117b8fc", "focus"), ("9777ed16bdd32b74bb21403e4213c6d599800939", "remaining")], "Scenario group is missing and is not a verified early driver")
    workflow = subprocess.check_output(["git", "show", sha + ":.github/workflows/benchmark.yml"], text=True)
    (output / "workflow.yml").write_text(workflow)
    results = source / "results"
    for file in results.glob("*.json"):
        shutil.copyfile(file, output / file.name)
    for folder in results.iterdir():
        if not folder.is_dir():
            continue
        shutil.copyfile(folder / "matrix.csv", output / "runs" / (folder.name + ".csv"))
        shutil.copyfile(results / (folder.name + ".txt"), output / "runs" / (folder.name + ".txt"))
        for trace in folder.glob("*.trace"):
            packed = output / "inputs" / (trace.name + ".gz")
            raw = trace.read_bytes()
            require(not packed.exists() or gzip.decompress(packed.read_bytes()) == raw, "input changes")
            packed.write_bytes(gzip.compress(raw, mtime=0))
    (output / "metadata.json").write_text(json.dumps({"run": run, "diagnostic_sha": sha, "baseline": BASELINE, "candidate": CANDIDATE, "group": group}, ensure_ascii=False, indent=2) + "\n")
    result = verify(output)
    (output / "audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--sha")
    parser.add_argument("--group", choices=["focus", "remaining", "control", "interleaved"])
    args = parser.parse_args()
    if args.output:
        require(all([args.run_id, args.sha, args.group]), "Need to run when collecting,Commit and scene groups")
        result = collect(args.directory, args.output, args.run_id, args.sha, args.group)
    else:
        result = verify(args.directory)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("Materials verified and passed;review_required The value review in is still red,Performance recovery cannot be inferred from this review.")


if __name__ == "__main__":
    main()
