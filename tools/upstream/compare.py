"""Generate fixed seed trajectories,Run the real Rust/C++ Execute the end and compare the output of each step."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

MASK = (1 << 64) - 1
SEEDS = [0, 1, 42, 0xABCDEF, MASK]


def generate(seed, count):
    state = seed

    def next_number():
        nonlocal state
        state = (state + 0x9E3779B97F4A7C15) & MASK
        value = state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
        return value ^ (value >> 31)

    serials = [0, 0, 0]
    lines = [f"raster-trace 1 {seed}"]
    for _ in range(count):
        session, key, choice, payload = next_number() % 3, next_number() % 8, next_number(), next_number()
        serials[session] += 1 + choice % 3
        key_text = bytes([key] * key).hex() or "-"
        value = f"u {payload}" if key % 2 == 0 else "b " + (payload.to_bytes(8, "little")[:payload % 9].hex() or "-")
        option = int(bool(choice & 4))
        operation = [f"read {option}", f"upsert {value}", f"rmw {option} {value}", f"delete {option}"][choice % 4]
        lines.append(f"{session} {serials[session]} {key_text} {operation}")
    return "\n".join(lines) + "\n"


def clean(text):
    return text.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")


def run(command, log, env=None, timeout=180):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=timeout)
    log.write_text(clean(result.stdout + result.stderr))
    if result.returncode:
        raise RuntimeError(f"Execution failed,exit_code {result.returncode},Log {log}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", type=Path, required=True, help="Already built upstream executor")
    parser.add_argument("--corrected-cpp", type=Path, help="Only make up the reference copy with mandatory tombstone protection;The original differences are still preserved")
    parser.add_argument("--output", type=Path, required=True, help="This new results directory")
    parser.add_argument("--disk", action="store_true", help="Both ends use native file backends")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("RASTER_UPSTREAM_"):
            env.pop(key)
    env["RUST_BACKTRACE"] = "0"
    env["CARGO_TERM_COLOR"] = "never"
    built = run(["cargo", "test", "--locked", "--all-features", "--test", "p9_upstream", "--no-run", "--message-format=json"], output / "rust-build.txt", env, 600)
    artifacts = [json.loads(line) for line in built.splitlines() if line.startswith("{")]
    executables = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact" and a.get("executable") and a["target"]["name"] == "p9_upstream"]
    if len(executables) != 1:
        raise RuntimeError("Unique not found Rust Track test program")
    cases = [("fixed boundaries", Path("tests/fixtures/p0.trace").read_text())]
    cases += [(f"random seed-{seed}", generate(seed, 1500)) for seed in SEEDS]
    summaries = []
    for name, trace in cases:
        source = output / (name + ".trace")
        source.write_text(trace)
        env["RASTER_UPSTREAM_GENERATED"] = "0" if name == "fixed boundaries" else "1"
        rust_result, cpp_result = output / (name + ".rust"), output / (name + ".cpp")
        env["RASTER_UPSTREAM_TRACE"] = str(source)
        env["RASTER_UPSTREAM_RESULT"] = str(rust_result)
        with tempfile.TemporaryDirectory(prefix="raster-random-") as root:
            command = [str(args.cpp.resolve()), str(source), str(cpp_result)]
            if args.disk:
                env["RASTER_UPSTREAM_ROOT"] = str(Path(root) / "rust")
                command += ["--disk", str(Path(root) / "cpp")]
            run([executables[0], "--exact", "the_same_ownership_trajectory_outputs_real_engine_results_for_upstream_comparison", "--nocapture"], output / (name + ".rust.log"), env)
            run(command, output / (name + ".cpp.log"))
            if args.corrected_cpp:
                corrected_result = output / (name + ".corrected.cpp")
                command = [str(args.corrected_cpp.resolve()), str(source), str(corrected_result)]
                if args.disk:
                    command += ["--disk", str(Path(root) / "corrected")]
                run(command, output / (name + ".corrected.cpp.log"))
        rust_lines, cpp_lines = rust_result.read_text().splitlines(), cpp_result.read_text().splitlines()
        differences = [{"line": i + 1, "rust": a, "cpp": b} for i, (a, b) in enumerate(zip(rust_lines, cpp_lines)) if a != b]
        if len(rust_lines) != len(cpp_lines):
            differences.append({"error": "The number of result rows is different", "rust_lines": len(rust_lines), "cpp_lines": len(cpp_lines)})
        summary = {"case": name, "backend": "file" if args.disk else "null", "trace_sha256": hashlib.sha256(trace.encode()).hexdigest(), "steps": len(trace.splitlines()) - 1, "differences": differences}
        if args.corrected_cpp:
            corrected_lines = corrected_result.read_text().splitlines()
            from itertools import zip_longest
            corrected_differences = [{"line": i + 1, "rust": a, "corrected_cpp": b}
                                     for i, (a, b) in enumerate(zip_longest(rust_lines, corrected_lines)) if a != b]
            summary["corrected_differences"] = corrected_differences
            summary["contract"] = "force_tombstone Keep accessible tombstones;The rest of the original algorithm remains unchanged"
        summaries.append(summary)
        (output / "comparison.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
        print(f"{name}:{summary['steps']} step,{len(differences)} Differences in results", flush=True)
    field = "corrected_differences" if args.corrected_cpp else "differences"
    if any(s[field] for s in summaries):
        raise RuntimeError("Upstream and Rust There is a difference;Itemized results saved,It is not yet possible to declare that the comparison has passed")
    print("The agreed contract is passed operation by operation,Original upstream diff left intact" if args.corrected_cpp else "Upstream and Rust All trajectories have consistent operation results")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(clean(f"Upstream control failed:{error}"), flush=True)
        raise SystemExit(1)
