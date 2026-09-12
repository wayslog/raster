"""Native upstream fixed boundary:Original return unchanged,Confirmed mandatory tombstone fix verified by independent reference copy."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def run(command, log, env=None):
    result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=180)
    content = result.stdout + result.stderr
    content = content.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")
    log.write_text(content)
    if result.returncode:
        raise RuntimeError(f"Environment detection exit code {result.returncode},see {log.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", type=Path, required=True)
    parser.add_argument("--corrected-cpp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    source = Path("tests/fixtures/p0.trace").resolve()
    expected = Path("tools/upstream/fixtures/p0.cpp.results").read_text()
    with tempfile.TemporaryDirectory(prefix="raster-upstream-probe-") as root:
        for mode in ("memory", "file"):
            result = output / (mode + ".cpp.results")
            command = [str(args.cpp.resolve()), str(source), str(result)]
            if mode == "file":
                command += ["--disk", str(Path(root) / "cpp-store")]
            run(command, output / (mode + ".cpp.log"))
            if result.read_text() != expected:
                raise RuntimeError(f"upstream {mode} Raw return does not match fixed observation,Please investigate again")
            result = output / (mode + ".corrected.cpp.results")
            command = [str(args.corrected_cpp.resolve()), str(source), str(result)]
            if mode == "file":
                command += ["--disk", str(Path(root) / "corrected-store")]
            run(command, output / (mode + ".corrected.cpp.log"))
            if result.read_text() != expected.replace("0 6 missing", "0 6 tombstone"):
                raise RuntimeError(f"Forced tombstone correction {mode} Fixed observation discrepancy")
        env = os.environ.copy()
        env["RASTER_UPSTREAM_TRACE"] = str(source)
        env["RASTER_UPSTREAM_RESULT"] = str(output / "fixed.rust.results")
        env["RASTER_UPSTREAM_ROOT"] = str(Path(root) / "rust-store")
        env["RASTER_UPSTREAM_GENERATED"] = "0"
        env.pop("RASTER_UPSTREAM_SPLIT", None)
        env.pop("RASTER_UPSTREAM_CHECKPOINT", None)
        run(["cargo", "test", "--locked", "--all-features", "--test", "p9_upstream", "--", "--nocapture"], output / "rust.log", env)
    rust = (output / "fixed.rust.results").read_text().splitlines()
    cpp = expected.splitlines()
    differences = [{"line": index + 1, "rust": a, "cpp": b} for index, (a, b) in enumerate(zip(rust, cpp)) if a != b]
    if len(rust) != len(cpp) or differences != [
        {"line": 6, "rust": "0 6 tombstone", "cpp": "0 6 missing"},
    ]:
        raise RuntimeError("Cross-implementation differences do not match documented fixed boundaries,Need investigation")
    summary = {"environment_probe": "passed", "compatibility_acceptance": "approved_contract_passed", "differences": differences,
               "correction": "force_tombstone Disable index elimination", "corrected_differences": []}
    (output / "probe.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print("Upstream execution environment baseline passed:Null and Linux libaio Returns are all consistent with fixed observations.")
    print("Fixed boundaries via confirmed contracts:Rust Consistent with mandatory tombstone reference fix;One difference from the original upstream remains.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Upstream environment detection failed:{error}")
        raise SystemExit(1)
