"""in native Linux Rigorous comparison of three types of checkpointing and hypermemory lifecycle;Any return difference fails."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from scenarios import small, large


def clean(text):
    return text.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")


def run(command, log, env=None):
    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=1200)
    except subprocess.TimeoutExpired as error:
        def decoded(value):
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
        log.write_text(clean(decoded(error.stdout) + decoded(error.stderr)))
        raise RuntimeError(f"Execute beyond default 1200 seconds,see {log.name}") from error
    text = result.stdout + result.stderr
    log.write_text(clean(text))
    if result.returncode:
        raise RuntimeError(f"Execution failed,exit_code {result.returncode},see {log.name}")
    return result.stdout


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def compare(left, right, differences):
    mismatches = []
    from itertools import zip_longest
    with left.open() as a, right.open() as b:
        for number, (rust, cpp) in enumerate(zip_longest(a, b), 1):
            if rust != cpp:
                mismatches.append({"line": number, "rust": rust, "cpp": cpp})
    differences.write_text(json.dumps(mismatches, ensure_ascii=False, indent=2))
    return len(mismatches)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=["all", "full", "pair", "large"], default="all")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("RASTER_UPSTREAM_"):
            env.pop(key)
    env.update(CARGO_TERM_COLOR="never", RUST_BACKTRACE="0", RASTER_UPSTREAM_TIMEOUT="600")
    # Both protocol decoding and services for large inputs use release mode.;The threshold is fixed before execution.
    built = run(["cargo", "test", "--locked", "--all-features", "--release", "--test", "p9_upstream", "--no-run", "--message-format=json"], output / "rust-build.txt", env)
    artifacts = [json.loads(line) for line in built.splitlines() if line.startswith("{")]
    executables = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact" and a.get("executable") and a["target"]["name"] == "p9_upstream"]
    if len(executables) != 1:
        raise RuntimeError("Rust The executor is not unique")
    summaries = []
    for name in ["full", "pair", "large"]:
        if args.case not in ("all", name):
            continue
        source = output / (name + ".trace")
        print(f"generate {name} trajectory", flush=True)
        details = (large if name == "large" else small)(source)
        details.update(case=name, trace_sha256=digest(source))
        (output / (name + "-input.json")).write_text(json.dumps(details, ensure_ascii=False, indent=2))
        kind = "pair" if name == "pair" else "full"
        rust_result, cpp_result = output / (name + ".rust.results"), output / (name + ".cpp.results")
        root = Path(tempfile.mkdtemp(prefix="raster-lifecycle-"))
        try:
            # Large tracks retain the same page window and business input;32 MiB segment avoidance 128 handle budget exhausted.
            env["RASTER_UPSTREAM_SEGMENT_BYTES"] = str(33554432 if name == "large" else 1048576)
            env["RASTER_UPSTREAM_TRACE"] = str(source)
            env["RASTER_UPSTREAM_RESULT"] = str(rust_result)
            env["RASTER_UPSTREAM_ROOT"] = str(root / "rust")
            env["RASTER_UPSTREAM_SPLIT"] = str(details["split"])
            env["RASTER_UPSTREAM_CHECKPOINT"] = kind
            print(f"{name}:Rust execute {details['steps']} step and resume", flush=True)
            rust_log = run([executables[0], "--exact", "the_same_ownership_trajectory_outputs_real_engine_results_for_upstream_comparison", "--nocapture"], output / (name + ".rust.log"), env)
            print(f"{name}:C++ Execute same input and restore", flush=True)
            cpp_log = run([str(args.cpp.resolve()), str(source), str(cpp_result), "--disk", str(root / "cpp"), "--split", str(details["split"]), "--checkpoint", kind], output / (name + ".cpp.log"))
        except Exception:
            print(f"Failed materials remain in this temporary directory {root.name}", flush=True)
            raise
        mismatches = compare(rust_result, cpp_result, output / (name + "-differences.json"))
        label = "Index+Log" if kind == "pair" else "Full"
        if f"Rust Trajectory boundary recovery passes:mode {label},3 sessions_resumed" not in rust_log or f"Upstream trajectory boundary recovery passes:mode {label},3 sessions_resumed" not in cpp_log:
            raise RuntimeError(f"{name} Missing evidence of actual recovery and three session continuations")
        pending = re.search(r"Four upstream operations Pending:\[(\d+),(\d+),(\d+),(\d+)\]", cpp_log)
        span = re.search(r"upstream log boundary:\[\d+,\d+\),span (\d+) bytes", cpp_log)
        if not pending or not span:
            raise RuntimeError("lack of reality Pending or span output")
        details.update(cpp_pending=list(map(int, pending.groups())), cpp_log_span_bytes=int(span.group(1)), differences=mismatches)
        if name == "large" and (details["cpp_log_span_bytes"] <= 2 * 256 * 1024 * 1024 or not details["cpp_pending"][0] or not details["cpp_pending"][2]):
            raise RuntimeError("The large trajectory does not reach the preset span or actual cold reading/RMW Pending threshold")
        summaries.append(details)
        (output / "lifecycle.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
        if mismatches:
            raise RuntimeError(f"{name} Yes {mismatches} Differences in results,Failed life cycle control")
        shutil.rmtree(root)
        print(f"{name}:Operation-by-operation results,Checkpoint recovery and session progress passed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(clean(f"Lifecycle comparison failed:{error}"), flush=True)
        raise SystemExit(1)
