"""Run an explicitly listed failure and history matrix;Each item must match and actually pass a full test."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def clean(text):
    return text.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")


def run(command, path, env, timeout):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=timeout)
    output = result.stdout + result.stderr
    path.write_text(clean(output))
    if result.returncode:
        raise RuntimeError(f"command exit code {result.returncode},see {path.name}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="This new results directory")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    matrix = json.loads(Path("tools/acceptance/fault-matrix.json").read_text())
    if matrix["version"] != 1 or not matrix["use case"]:
        raise RuntimeError("Fault matrix version is wrong or empty")
    env = os.environ.copy()
    env["CARGO_TERM_COLOR"] = "never"
    env["RUST_BACKTRACE"] = "0"
    env["RASTER_HISTORY_OUTPUT"] = str((args.output / "histories.txt").resolve())
    for key in list(env):
        if key.startswith("RASTER_UPSTREAM_"):
            env.pop(key)
    command = ["cargo", "test", "--locked", "--all-features", "--no-run", "--message-format=json"]
    built = run(command, args.output / "build.txt", env, 600)
    executables = {}
    for line in built.splitlines():
        if not line.startswith("{"):
            continue
        artifact = json.loads(line)
        if artifact.get("reason") == "compiler-artifact" and artifact.get("executable") and artifact.get("profile", {}).get("test"):
            name = artifact["target"]["name"]
            if name in executables and executables[name] != artifact["executable"]:
                raise RuntimeError("The test program name is not unique")
            executables[name] = artifact["executable"]
    report = []
    ids = set()
    for case in matrix["use case"]:
        identity = case["id"]
        if not re.fullmatch(r"F\d{2}", identity) or identity in ids:
            raise RuntimeError("Use case number is invalid or duplicate")
        ids.add(identity)
        executable = executables[case["program"]]
        listing = run([executable, "--list"], args.output / (identity + "-list.txt"), env, 30)
        if listing.splitlines().count(case["test"] + ": test") != 1:
            raise RuntimeError(f"{identity} No unique match for actual test")
        print(f"{identity} {case['stage']}:start", flush=True)
        output = run([executable, "--exact", case["test"], "--nocapture"], args.output / (identity + ".txt"), env, 240)
        if not re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output):
            raise RuntimeError(f"{identity} Didn't actually pass a test,Cannot accept filtering as empty or ignored")
        report.append({**case, "result": "passed"})
        (args.output / "matrix.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"{identity}:passed", flush=True)
    histories = (args.output / "histories.txt").read_text().splitlines()
    if len(histories) != 32 or any(line.count("Event {") != 9 for line in histories):
        raise RuntimeError("Complete history missing 32 group or each group 9 events")
    print(f"Fault cross matrix passes:{len(report)} actual test entrance;Save also 32 Group complete history.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(clean(f"Matrix acceptance failed:{error}"), flush=True)
        raise SystemExit(1)
