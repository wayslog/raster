"""Collect native CPU profiles while preserving direct-engine output checks."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess

from compare import expected


DEFAULT_CASES = ("read/uniform/1", "read/uniform/4", "upsert/worker-hot/4")


def validate_output(output, engine, case, count):
    operation, distribution, threads = case.split("/")
    threads = int(threads)
    objects = [json.loads(line) for line in output.splitlines() if line.startswith('{"engine":')]
    if len(objects) != 1:
        raise ValueError("profile must contain exactly one engine result")
    row = objects[0]
    required = expected(operation, distribution, threads, count) | {
        "engine": engine, "operation": operation, "distribution": distribution,
        "threads": threads, "count": count,
    }
    if any(row.get(key) != value for key, value in required.items()):
        raise ValueError("profile workload result differs from the reference model")
    if any(row.get(key, 0) <= 0 for key in ("elapsed_ns", "p99_ns")) or row.get("samples", 0) < 256:
        raise ValueError("profile workload did not complete its measurements")
    return row


def run_saved(command, prefix, timeout=300):
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    prefix.with_suffix(".stdout").write_text(result.stdout)
    prefix.with_suffix(".stderr").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(f"{prefix.name} failed with exit code {result.returncode}")
    return result.stdout


def compress(path):
    destination = path.with_name(path.name + ".gz")
    with path.open("rb") as source, destination.open("wb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as zipped:
            shutil.copyfileobj(source, zipped)
    path.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--perf", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--cpp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", default="")
    parser.add_argument("--count", type=int, default=40_000_000)
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("native CPU collection requires Linux perf")
    if args.count < 16_384 or args.count % 4:
        parser.error("count must be at least 16384 and divisible by four")
    cases = (args.case,) if args.case else DEFAULT_CASES
    for case in cases:
        parts = case.split("/")
        if (len(parts) != 3 or parts[0] not in ("read", "upsert", "rmw")
                or parts[1] not in ("uniform", "worker-hot", "shared-hot")
                or parts[2] not in ("1", "4")):
            parser.error("case must be operation/distribution/threads")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {"cpp": args.cpp.resolve(), "rust": args.rust.resolve()}
    perf = (["sudo", "-n"] if args.sudo else []) + [str(args.perf.resolve())]
    os.environ["LC_ALL"] = "C"
    metadata = {
        "protocol": 1,
        "purpose": "CPU attribution only; profiled timings are excluded from acceptance",
        "scope": "Whole processes including setup and final verification; inspect worker call chains separately",
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "platform": platform.platform(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "perf_version": subprocess.check_output(perf + ["--version"], text=True).strip(),
        "event": "cpu-clock:u", "frequency": 499, "call_graph": "dwarf,8192",
        "count": args.count, "cases": cases,
        "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()},
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    for case in cases:
        for engine, binary in binaries.items():
            root = args.output / (case.replace("/", "-") + "-" + engine)
            root.mkdir()
            data = root / "perf.data"
            command = perf + ["record", "-e", metadata["event"], "-F", "499",
                              "--call-graph", metadata["call_graph"], "-o", str(data), "--",
                              str(binary), *case.split("/"), str(args.count), "64"]
            print(f"Profiling {case} {engine}", flush=True)
            (root / "command.json").write_text(json.dumps(command, indent=2) + "\n")
            output = run_saved(command, root / "record")
            row = validate_output(output, engine, case, args.count)
            run_saved(perf + ["report", "-i", str(data), "--stdio", "--no-children",
                              "--show-nr-samples", "--percent-limit", "0.5"], root / "self")
            run_saved(perf + ["report", "-i", str(data), "--stdio", "--children",
                              "--percent-limit", "1.0"], root / "callgraph")
            script = root / "stacks.txt"
            with script.open("wb") as output, (root / "stacks.stderr").open("wb") as errors:
                subprocess.run(perf + ["script", "-i", str(data), "--ns"],
                               stdout=output, stderr=errors, check=True, timeout=180)
            with script.open() as stream:
                samples = sum(bool(re.search(r"\bcpu-clock(?::u)?:", line)) for line in stream)
            if samples == 0:
                raise RuntimeError("perf produced no CPU samples")
            if args.sudo:
                subprocess.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(data)], check=True)
            raw_hash = hashlib.sha256(data.read_bytes()).hexdigest()
            compress(data)
            compress(script)
            results.append({"case": case, "engine": engine, "cpu_samples": samples,
                            "perf_data_sha256": raw_hash, "verified_result": row})
            (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"Verified {case} {engine}: {samples} CPU samples", flush=True)


if __name__ == "__main__":
    main()
