"""Check native upstream commit,only correction,All random raw results and three sets of lifetimes,Do not ignore operation status."""
import argparse
import hashlib
from itertools import zip_longest
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "upstream"))
from compare import generate, SEEDS


def require(condition, message):
    if not condition:
        raise ValueError(message)


def differences(left, right, field="cpp"):
    return [{"line": i + 1, "rust": a, field: b}
            for i, (a, b) in enumerate(zip_longest(left.splitlines(), right.splitlines())) if a != b]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    run = json.loads(subprocess.check_output(["gh", "run", "view", args.run_id, "--json", "headSha,status,conclusion,jobs"], text=True))
    require(run["headSha"] == args.sha and run["status"] == "completed" and run["conclusion"] == "success", "Native run submission or final status does not match")
    require(len(run["jobs"]) == 1 and run["jobs"][0]["conclusion"] == "success", "Not all native tasks passed")
    root = args.directory
    environment = root / "p9-upstream-environment"
    text = (environment / "environment.txt").read_text()
    require(args.sha in text and "321d872eabda6a0345c8bd76419f89723ed864ae" in text and "release: 1.98.1" in text and "Linux" in text, "The identity of the native environment does not match")
    patch = json.loads((environment / "forced-tombstone.json").read_text())
    require(patch["changed_files"] == ["cc/src/core/faster.h"], "Reference correction beyond single file")
    require(patch["original_sha256"] == "4b17488f22d46d00d963bbf2279b4eefc6017819ddd3166eafb3eb1899588e68", "The upstream raw header file is not fixed content")
    require(patch["corrected_sha256"] == "2b959be3555ab2a3c424d8b5c4f80de59005289f44963e38602b6029b62c6587", "Fix is no longer the only mandatory tombstone protection condition")
    require(hashlib.sha256((environment / "forced-tombstone.diff").read_bytes()).hexdigest() == patch["patch_sha256"] == "1b45a8b986571b116ac3d44a967b0567d9004bd424117bdfeb8db9e947ca81ee", "Reference patch content does not match")
    probe = json.loads((environment / "probe.json").read_text())
    require(probe["environment_probe"] == "passed" and probe["compatibility_acceptance"] == "approved_contract_passed" and probe["corrected_differences"] == [], "Fixed boundaries without confirmation contract")
    require(probe["differences"] == [{"line": 6, "rust": "0 6 tombstone", "cpp": "0 6 missing"}], "Fixed original diff change")
    summaries = []
    for backend in ["null", "file"]:
        folder = environment / ("random-" + backend)
        cases = json.loads((folder / "comparison.json").read_text())
        expected = [("fixed boundaries", Path("tests/fixtures/p0.trace").read_text())]
        expected += [(f"random seed-{seed}", generate(seed, 1500)) for seed in SEEDS]
        require([c["case"] for c in cases] == [name for name, _ in expected], "Random scenes are incomplete or in the wrong order")
        for case, (name, source) in zip(cases, expected):
            require((folder / (name + ".trace")).read_text() == source and case["trace_sha256"] == hashlib.sha256(source.encode()).hexdigest(), "input trace or hash changed")
            require(case["backend"] == backend and case["steps"] == len(source.splitlines()) - 1, "Backend or step count does not match")
            rust = (folder / (name + ".rust")).read_text()
            cpp = (folder / (name + ".cpp")).read_text()
            corrected = (folder / (name + ".corrected.cpp")).read_text()
            require(len(rust.splitlines()) == len(source.splitlines()) and rust == corrected, "Rust Not consistent operation-by-operation with revised reference")
            require(case["corrected_differences"] == [] and case["differences"] == differences(rust, cpp), "The original diff contains omissions or errors")
        require(sum(c["steps"] for c in cases) == 7508, "Random total step error")
        summaries.append({"backend": backend, "number of steps": 7508, "original difference": sum(len(c["differences"]) for c in cases), "Fix differences": 0})
    folder = root / "p9-upstream-lifecycle" / "p9-lifecycle-evidence"
    lifecycle = json.loads((folder / "lifecycle.json").read_text())
    require([c["case"] for c in lifecycle] == ["full", "pair", "large"], "Incomplete life cycle")
    for case in lifecycle:
        name = case["case"]
        require(hashlib.sha256((folder / (name + ".trace")).read_bytes()).hexdigest() == case["trace_sha256"], "Lifecycle input hash does not match")
        rust = (folder / (name + ".rust.results")).read_bytes()
        cpp = (folder / (name + ".cpp.results")).read_bytes()
        require(rust == cpp and len(rust.splitlines()) - 1 == case["steps"] and case["differences"] == 0, "Lifecycle is inconsistent with original upstream")
        require(json.loads((folder / (name + "-differences.json")).read_text()) == [], "Lifecycle differences are not cleared")
    require(sum(c["steps"] for c in lifecycle) == 33976 and lifecycle[-1]["cpp_log_span_bytes"] > 512 * 1024 * 1024 and lifecycle[-1]["cpp_pending"][0] > 0 and lifecycle[-1]["cpp_pending"][2] > 0, "Insufficient coverage of hypermemory life cycle")
    (root / "audit.json").write_text(json.dumps({"sha": args.sha, "run": run, "random": summaries, "life cycle": lifecycle, "Reference correction": patch}, ensure_ascii=False, indent=2))
    print(json.dumps(summaries, ensure_ascii=False))
    print("Three sets of life cycles 33976 The steps are consistent with the original upstream step-by-step operation.")


if __name__ == "__main__":
    main()
