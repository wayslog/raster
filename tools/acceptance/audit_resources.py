#!/usr/bin/env python3
"""Check the designated submitted dual-platform resource products;Mission success itself does not replace business and round-to-round thresholds."""
import argparse
import csv
import json
from pathlib import Path
import re
import subprocess


def require(condition, message):
    if not condition:
        raise ValueError(message)


def audit(root, sha, run_id):
    run = json.loads(subprocess.check_output(
        ["gh", "run", "view", run_id, "--json", "headSha,status,conclusion,jobs"],
        text=True, timeout=60,
    ))
    require(run["headSha"] == sha, "Run commit mismatch")
    require(run["status"] == "completed" and run["conclusion"] == "success", "The run has not ended successfully")
    require(len(run["jobs"]) == 2 and all(j["conclusion"] == "success" for j in run["jobs"]),
            "Both native tasks must succeed")
    budgets = {"Survival allocated bytes": 256 * 1024, "number of live allocations": 16,
               "file descriptor": 2, "threads": 1, "RSSbytes": 16 * 1024 * 1024}
    columns = ["round", "Survival allocated bytes", "number of live allocations", "Allocate peak bytes", "RSSbytes", "file descriptor", "threads", "milliseconds taken"]
    platforms = {"ubuntu-24.04": "x86_64-unknown-linux-gnu", "macos-14": "aarch64-apple-darwin"}
    verified = []
    for platform, target in platforms.items():
        folder = root / f"p9-resources-{platform}"
        environment = (folder / "environment.txt").read_text()
        checks = (folder / "checks.txt").read_text()
        require(environment.splitlines()[0] == sha and "release: 1.98.1" in environment,
                f"{platform} The environment or tool chain does not match the")
        require(f"host: {target}" in environment, f"{platform} of native target no match")
        require("features:all;mode:release;warmup:8;measurement:64" in environment, "Run configuration mismatch")
        require(len(re.findall(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", checks)) == 1,
                "The counting allocator test must actually run and pass")
        require(checks.count("Disk life cycle passed: 40 keys;") == 72, "The number of complete business cycles does not match")
        require(checks.count("Same-process resource cycle passed: warmup 8 rounds, measurement 64 rounds") == 1,
                "Missing final resource pass result")
        require("test result: FAILED" not in checks, "test contains failure")
        require(not list(folder.glob("cycle-*")), "Successful artifacts still contain uncleaned business directories")
        with (folder / "resources.csv").open(newline="") as stream:
            reader = csv.DictReader(stream)
            require(reader.fieldnames == columns, "Resources CSV Columns don't match")
            rows = [{key: int(value) for key, value in row.items()} for row in reader]
        require([row["round"] for row in rows] == list(range(65)), "resource rounds are missing or duplicated")
        require(all(value >= 0 for row in rows for value in row.values()), "Resource indicator is negative")
        baseline = rows[0]
        for row in rows[1:]:
            for metric, increase in budgets.items():
                require(row[metric] <= baseline[metric] + increase,
                        f"{platform} ordinal {row['round']} Round {metric} exceeds preset threshold")
        verified.append({
            "Platform": platform, "target": target, "Complete business cycle": 72, "Recovery times": 144,
            "Measurement takes milliseconds": sum(row["milliseconds taken"] for row in rows[1:]),
            "indicator": {key: {"baseline": baseline[key], "smallest": min(row[key] for row in rows[1:]),
                          "maximum": max(row[key] for row in rows[1:])}
                   for key in columns[1:-1]},
        })
    return {"sha": sha, "run": run, "verified": verified}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True, help="Complete submission SHA")
    parser.add_argument("--run-id", required=True, help="Resource workflow number")
    parser.add_argument("directory", type=Path, help="gh run download Download product catalog")
    args = parser.parse_args()
    result = audit(args.directory, args.sha, args.run_id)
    with (args.directory / "audit.json").open("x") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    print(json.dumps(result["verified"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
