"""Compare resident append and physical scans against a preserved Rust binary."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=4)
    args = parser.parse_args()
    assert args.rounds >= 2
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {name: getattr(args, name).resolve() for name in ("baseline", "candidate")}
    metadata = {
        "protocol": 1,
        "scope": "Same-host Rust regression comparison; not C++ parity acceptance",
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "rounds": args.rounds,
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest()
                          for name, path in binaries.items()},
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    summaries = []
    for count in (4096, 65536):
        scans = 8
        expected = (sum(key ^ 0x7261737465720001 for key in range(count)) * scans) % (1 << 64)
        rows = {name: [] for name in binaries}
        for number in range(-1, args.rounds):
            order = ("baseline", "candidate") if number % 2 == 0 else ("candidate", "baseline")
            for name in order:
                result = subprocess.run([str(binaries[name]), str(count), str(scans)],
                                        capture_output=True, text=True, timeout=120)
                prefix = f"{count}-{number}-{name}"
                for suffix, content in (("stdout", result.stdout), ("stderr", result.stderr)):
                    content = content.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")
                    (args.output / f"{prefix}.{suffix}").write_text(content)
                if result.returncode:
                    raise RuntimeError(f"{prefix}: exit {result.returncode}; inspect saved output")
                row = json.loads(result.stdout)
                assert row["records"] == count and row["scans"] == scans
                assert row["checksum"] == expected and row["samples"] == count * scans // 64
                assert all(row[key] > 0 for key in ("append_ns", "scan_ns", "p99_ns"))
                if number >= 0:
                    rows[name].append(row)
        median = {name: {key: statistics.median(row[key] for row in values)
                         for key in ("append_ns", "scan_ns", "p99_ns")}
                  for name, values in rows.items()}
        append = median["baseline"]["append_ns"] / median["candidate"]["append_ns"]
        scan = median["baseline"]["scan_ns"] / median["candidate"]["scan_ns"]
        p99 = median["candidate"]["p99_ns"] / median["baseline"]["p99_ns"]
        passed = append >= .98 and scan >= .98 and p99 <= 1.1
        summaries.append({"records": count, "scans": scans, "append_throughput_ratio": append,
                          "scan_throughput_ratio": scan, "p99_ratio": p99,
                          "passed": passed, "rows": rows})
        (args.output / "comparison.json").write_text(json.dumps(summaries, indent=2) + "\n")
        print(f"{count} records: append={append:.3f}, scan={scan:.3f}, p99={p99:.3f}, "
              f"{'PASS' if passed else 'FAIL'}", flush=True)
    raise SystemExit(0 if all(row["passed"] for row in summaries) else 1)


if __name__ == "__main__":
    main()
