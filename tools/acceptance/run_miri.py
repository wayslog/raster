#!/usr/bin/env python3
"""Opposite page,value layout,Cache and acceptance allocators run memory safety checks,Keep each original output."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="A new catalog of evidence")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                   "There are no changes committed": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True)),
                   "interpreter": subprocess.check_output(["cargo", "+nightly", "miri", "--version"], text=True).strip()}
    (args.output / "environment.json").write_text(json.dumps(environment, ensure_ascii=False, indent=2))
    forgotten = "log::page::tests::invalid_length_alignment_overflows_and_forgotten_ranges_are_safely_rejected"
    cases = [
        ("Page ranges and pre-allocation", ["--lib", "log::page::", "--", "--skip", forgotten], 7, ""),
        ("In-page value layout", ["--lib", "log::value::tests::"], 9, ""),
        ("Record life cycle", ["--lib", "log::tests::"], 14, ""),
        ("Cache byte area", ["--lib", "cache::arena::tests::"], 1, ""),
        ("Caching readers and elimination", ["--lib", "cache::tests::"], 5, ""),
        ("acceptance allocator", ["--test", "p9_allocator"], 1, ""),
        ("Result Budget Ownership", ["--lib", "engine::pending::result_budget_tests::"], 3, ""),
        # In this example, the lease was intentionally forgotten,Validation only blocks reclamation, not freeing active scope;Enable leak detection in other cases.
        ("intentional forgetting scope", ["--lib", forgotten, "--", "--exact"], 1, "-Zmiri-ignore-leaks"),
    ]
    results = []
    for name, arguments, expected, flags in cases:
        command = ["cargo", "+nightly", "miri", "test", "--locked", *arguments]
        env = {**os.environ, "MIRIFLAGS": flags, "CARGO_TERM_COLOR": "never", "RUST_BACKTRACE": "0"}
        completed = subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, timeout=600)
        output = completed.stdout.replace(str(Path.cwd()), "worktree").replace(str(Path.home()), "user_directory")
        (args.output / f"{name}.txt").write_text(output)
        counts = re.findall(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;", output)
        passed = completed.returncode == 0 and counts == [str(expected)]
        results.append({"Check": name, "command": command, "MIRIFLAGS": flags,
                        "Expected number of tests": expected, "exit_code": completed.returncode, "passed": passed})
        (args.output / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2))
        print(f"{name}:{'passed' if passed else 'failure'},expected {expected} item", flush=True)
        if not passed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
