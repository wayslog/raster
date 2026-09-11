"""核对原生上游提交、唯一修正、全部随机原始结果和三组生命周期，不忽略操作状态。"""
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
    require(run["headSha"] == args.sha and run["status"] == "completed" and run["conclusion"] == "success", "原生运行提交或最终状态不符")
    require(len(run["jobs"]) == 1 and run["jobs"][0]["conclusion"] == "success", "原生任务没有全部通过")
    root = args.directory
    environment = root / "p9-upstream-environment"
    text = (environment / "environment.txt").read_text()
    require(args.sha in text and "321d872eabda6a0345c8bd76419f89723ed864ae" in text and "release: 1.98.1" in text and "Linux" in text, "原生环境身份不符")
    patch = json.loads((environment / "forced-tombstone.json").read_text())
    require(patch["changed_files"] == ["cc/src/core/faster.h"], "参考修正超出单文件")
    require(patch["original_sha256"] == "4b17488f22d46d00d963bbf2279b4eefc6017819ddd3166eafb3eb1899588e68", "上游原始头文件不是固定内容")
    require(patch["corrected_sha256"] == "2b959be3555ab2a3c424d8b5c4f80de59005289f44963e38602b6029b62c6587", "修正不再是唯一强制墓碑保护条件")
    require(hashlib.sha256((environment / "forced-tombstone.diff").read_bytes()).hexdigest() == patch["patch_sha256"] == "1b45a8b986571b116ac3d44a967b0567d9004bd424117bdfeb8db9e947ca81ee", "参考补丁内容不符")
    probe = json.loads((environment / "probe.json").read_text())
    require(probe["environment_probe"] == "passed" and probe["compatibility_acceptance"] == "approved_contract_passed" and probe["corrected_differences"] == [], "固定边界没有通过确认契约")
    require(probe["differences"] == [{"line": 6, "rust": "0 6 tombstone", "cpp": "0 6 missing"}], "固定原始差异改变")
    summaries = []
    for backend in ["null", "file"]:
        folder = environment / ("random-" + backend)
        cases = json.loads((folder / "comparison.json").read_text())
        expected = [("固定边界", Path("tests/fixtures/p0.trace").read_text())]
        expected += [(f"随机种子-{seed}", generate(seed, 1500)) for seed in SEEDS]
        require([c["case"] for c in cases] == [name for name, _ in expected], "随机场景不完整或次序错误")
        for case, (name, source) in zip(cases, expected):
            require((folder / (name + ".trace")).read_text() == source and case["trace_sha256"] == hashlib.sha256(source.encode()).hexdigest(), "输入轨迹或哈希改变")
            require(case["backend"] == backend and case["steps"] == len(source.splitlines()) - 1, "后端或步数不符")
            rust = (folder / (name + ".rust")).read_text()
            cpp = (folder / (name + ".cpp")).read_text()
            corrected = (folder / (name + ".corrected.cpp")).read_text()
            require(len(rust.splitlines()) == len(source.splitlines()) and rust == corrected, "Rust 与修正参考未逐操作一致")
            require(case["corrected_differences"] == [] and case["differences"] == differences(rust, cpp), "原始差异有遗漏或错误")
        require(sum(c["steps"] for c in cases) == 7508, "随机总步数错误")
        summaries.append({"后端": backend, "步数": 7508, "原始差异": sum(len(c["differences"]) for c in cases), "修正差异": 0})
    folder = root / "p9-upstream-lifecycle" / "p9-lifecycle-evidence"
    lifecycle = json.loads((folder / "lifecycle.json").read_text())
    require([c["case"] for c in lifecycle] == ["full", "pair", "large"], "生命周期不完整")
    for case in lifecycle:
        name = case["case"]
        require(hashlib.sha256((folder / (name + ".trace")).read_bytes()).hexdigest() == case["trace_sha256"], "生命周期输入哈希不符")
        rust = (folder / (name + ".rust.results")).read_bytes()
        cpp = (folder / (name + ".cpp.results")).read_bytes()
        require(rust == cpp and len(rust.splitlines()) - 1 == case["steps"] and case["differences"] == 0, "生命周期与原始上游不一致")
        require(json.loads((folder / (name + "-differences.json")).read_text()) == [], "生命周期差异没有清零")
    require(sum(c["steps"] for c in lifecycle) == 33976 and lifecycle[-1]["cpp_log_span_bytes"] > 512 * 1024 * 1024 and lifecycle[-1]["cpp_pending"][0] > 0 and lifecycle[-1]["cpp_pending"][2] > 0, "超内存生命周期覆盖不足")
    (root / "audit.json").write_text(json.dumps({"sha": args.sha, "run": run, "随机": summaries, "生命周期": lifecycle, "参考修正": patch}, ensure_ascii=False, indent=2))
    print(json.dumps(summaries, ensure_ascii=False))
    print("三组生命周期 33976 步与原始上游逐操作一致")


if __name__ == "__main__":
    main()
