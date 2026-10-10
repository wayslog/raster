"""Build immutable engine snapshots with one shared, temporary benchmark driver."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import time


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def command(args, name, output, cwd=None, env=None, timeout=1500):
    write_json(output / (name + ".command.json"), args)
    started = time.monotonic()
    timed_out = False
    with (output / (name + ".stdout")).open("wb") as stdout, (output / (name + ".stderr")).open("wb") as stderr:
        child = subprocess.Popen(args, cwd=cwd, env=env, stdout=stdout, stderr=stderr,
                                 start_new_session=True)
        try:
            code = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(child.pid, signal.SIGKILL)
            code = child.wait()
    write_json(output / (name + ".result.json"), {
        "returncode": code, "timed_out": timed_out,
        "wall_seconds": time.monotonic() - started,
    })
    if code or timed_out:
        raise RuntimeError("Command failed; inspect " + name)


def verify_source(source, manifest):
    actual_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    actual_tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=source, text=True).strip()
    status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=source, text=True)
    if actual_head != manifest["commit"] or actual_tree != manifest["git_tree"] or status:
        raise RuntimeError("Source identity mismatch: " + str(source))
    for name, details in manifest["files"].items():
        path = source / name
        if path.stat().st_size != details["bytes"] or sha256(path) != details["sha256"]:
            raise RuntimeError("Source file identity mismatch: " + name)
    return {"commit": actual_head, "git_tree": actual_tree,
            "checked_files": len(manifest["files"]), "tracked_clean_before_overlay": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    tooling = workspace / "tools/s0_measurement"
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    expected_registration = (tooling / "preregistration.sha256").read_text().split()[0]
    if sha256(tooling / "preregistration.json") != expected_registration:
        raise RuntimeError("Preregistration hash mismatch")
    plan = json.loads((tooling / "preregistration.json").read_text())
    required_files = {
        ".github/workflows/benchmark.yml", "examples/parity.rs",
        "tools/performance/memory.cc", "tools/performance/build.sh", "Cargo.toml", "Cargo.lock",
        *["tools/s0_measurement/" + name for name in (
            "prepare.py", "package_evidence.py", "interleaved_w0.py", "affinity_exec.py", "overlay_driver.py")],
        *["tools/s0_measurement/baselines/" + name + "-files.json" for name in (
            "R-observed", "R-reference", "C-reference")],
    }
    if not required_files.issubset(plan["tool_sha256"]):
        raise RuntimeError("Required measurement files are absent from preregistration")
    for name, checksum in plan["tool_sha256"].items():
        if sha256(workspace / name) != checksum:
            raise RuntimeError("Frozen measurement file changed: " + name)
    shutil.copy2(tooling / "preregistration.json", output / "preregistration.json")
    identities = {}
    for role, label in (("candidate", "R-observed"), ("baseline", "R-reference"), ("cpp", "C-reference")):
        manifest_path = tooling / "baselines" / (label + "-files.json")
        manifest = json.loads(manifest_path.read_text())
        registered_source = plan["baselines"][label]
        if (sha256(manifest_path) != registered_source["manifest_sha256"]
                or any(manifest[field] != registered_source[field]
                       for field in ("commit", "git_tree", "file_count", "archive_sha256"))):
            raise RuntimeError("Source manifest differs from registered role: " + label)
        identities[role] = verify_source(workspace / "source" / role, manifest)
    write_json(output / "source-identity.json", identities)
    environment = {
        "platform": platform.platform(), "workflow_commit": os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"), "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "runner_name": os.environ.get("RUNNER_NAME"), "image_os": os.environ.get("ImageOS"),
        "image_version": os.environ.get("ImageVersion"), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "preregistration_sha256": sha256(tooling / "preregistration.json"),
        "rustflags": "--remap-path-prefix=<workspace>=worktree --remap-path-prefix=<home>=user_directory",
    }
    write_json(output / "environment.json", environment)
    for name, args_list in (("rust-version", ["rustc", "-Vv"]), ("cargo-version", ["cargo", "-V"]),
                            ("cpp-version", ["g++", "--version"]), ("cpu-topology", ["lscpu", "-J"]),
                            ("kernel", ["uname", "-srm"])):
        command(args_list, name, output, timeout=30)
    shutil.copy2("/proc/meminfo", output / "meminfo.txt")
    shutil.copy2("/etc/os-release", output / "os-release.txt")
    original = workspace / "examples/parity.rs"
    expected_original = plan["rust_driver_overlay"]["original_sha256"]
    if sha256(original) != expected_original:
        raise RuntimeError("Original benchmark driver changed")
    before = original.read_text()
    old = "let limit = Deadline(started + Duration::from_secs(120));"
    new = "let limit = Deadline(started + Duration::from_secs(600));"
    if before.count(old) != 1:
        raise RuntimeError("Expected exactly one benchmark business deadline")
    overlaid = before.replace(old, new)
    overlay_sha = hashlib.sha256(overlaid.encode()).hexdigest()
    if overlay_sha != plan["rust_driver_overlay"]["overlay_sha256"]:
        raise RuntimeError("Overlaid benchmark driver hash mismatch")
    write_json(output / "driver-overlay.json", {"original_sha256": expected_original,
                                                "overlay_sha256": overlay_sha, "old": old, "new": new})
    environment_vars = os.environ.copy()
    environment_vars["CARGO_TERM_COLOR"] = "never"
    environment_vars["RUST_BACKTRACE"] = "0"
    environment_vars.pop("CARGO_ENCODED_RUSTFLAGS", None)
    environment_vars["CXX"] = "g++"
    environment_vars["RUSTFLAGS"] = "--remap-path-prefix=" + str(workspace) + "=worktree --remap-path-prefix=" + str(Path.home()) + "=user_directory"
    for role in ("candidate", "baseline"):
        source = workspace / "source" / role
        (source / "examples/parity.rs").write_text(overlaid)
        command(["cargo", "build", "--locked", "--release", "--example", "parity"],
                "build-" + role, output, cwd=source, env=environment_vars)
    command(["sh", "tools/performance/build.sh", str(workspace / "source/cpp"),
             str(workspace / "build/cpp")], "build-cpp", output, cwd=workspace, env=environment_vars)
    binaries = {
        "candidate": workspace / "source/candidate/target/release/examples/parity",
        "baseline": workspace / "source/baseline/target/release/examples/parity",
        "cpp": workspace / "build/cpp/faster-memory",
    }
    (output / "binaries").mkdir()
    binary_details = {}
    for role, path in binaries.items():
        shutil.copy2(path, output / "binaries" / role)
        binary_details[role] = {"sha256": sha256(path), "bytes": path.stat().st_size}
    binary_details["control"] = binary_details["baseline"]
    write_json(output / "binary-identity.json", binary_details)
    for role, label in (("candidate", "R-observed"), ("baseline", "R-reference")):
        source = workspace / "source" / role
        frozen = json.loads((tooling / "baselines" / (label + "-files.json")).read_text())
        changed = [name for name, details in frozen["files"].items()
                   if sha256(source / name) != details["sha256"]]
        expected_changes = ["examples/parity.rs"]
        if changed != expected_changes:
            raise RuntimeError("Unexpected source mutation: " + repr(changed))
        identities[role]["changed_files_after_build"] = changed
    verify_source(workspace / "source/cpp", json.loads((tooling / "baselines/C-reference-files.json").read_text()))
    write_json(output / "source-identity.json", identities)
    actual_files = {}
    for role, label in (("candidate", "R-observed"), ("baseline", "R-reference"), ("cpp", "C-reference")):
        frozen = json.loads((tooling / "baselines" / (label + "-files.json")).read_text())
        actual_files[role] = {name: sha256(workspace / "source" / role / name)
                              for name in frozen["files"]}
    write_json(output / "source-files-after-build.json", actual_files)
    receipt = {"registration_sha256": expected_registration,
               "binary_sha256": {role: details["sha256"] for role, details in binary_details.items()},
               "source_identity": identities, "driver_overlay_sha256": overlay_sha}
    write_json(output / "build-receipt.json", receipt)
    (output / "build-receipt.sha256").write_text(sha256(output / "build-receipt.json") + "  build-receipt.json\n")
    print(json.dumps({"prepared": True, "binaries": binary_details}), flush=True)


if __name__ == "__main__":
    main()
