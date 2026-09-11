"""审核交替测量的输入、顺序、业务恢复、二进制身份和原复核线；红色数值不改为通过。"""
import argparse
import csv
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import shutil
import statistics
import subprocess

from audit_benchmark import trace_audit, require

ORDERS = [("baseline", "candidate"), ("candidate", "baseline"), ("candidate", "baseline"), ("baseline", "candidate")]
METRICS = ["每秒操作", "P99纳秒", "读取P99纳秒", "写入P99纳秒", "RMWP99纳秒", "删除P99纳秒", "业务后累计读字节", "业务后累计写字节"]
BASELINE = "dbe392884be723708a0e6bdc0011de83f493f917"
CANDIDATE = "a834f48b53865098c8325d81cc52934bb322c26d"


class TraceBytes:
    def __init__(self, data):
        self.data = data

    def read_bytes(self):
        return self.data


def verify(root):
    metadata = json.loads((root / "metadata.json").read_text())
    run = metadata["run"]
    require(run["headSha"] == metadata["diagnostic_sha"] and run["status"] == "completed", "运行未完成或诊断提交不符")
    require(len(run["jobs"]) == 1, "必须有一个原生配对任务")
    require(metadata["baseline"] == BASELINE and metadata["candidate"] == CANDIDATE, "固定核心版本不符")
    env = (root / "environment.txt").read_text()
    require(env.splitlines()[:3] == [metadata["diagnostic_sha"], CANDIDATE, BASELINE], "构建核心身份不符")
    require("host: aarch64-apple-darwin" in env and "release: 1.98.1" in env, "原生工具链不符")
    require(set((root / "baseline-driver-files.txt").read_text().splitlines()) == {
        "examples/benchmark/main.rs", "examples/benchmark/scenario.rs", "tests/support/model.rs"
    }, "旧核心只能替换三个相同驱动文件")
    binaries = json.loads((root / "binaries.json").read_text())
    group = metadata["group"]
    sides = ["baseline", "candidate", "control"] if group == "interleaved" else ["baseline", "candidate"]
    orders = list(itertools.permutations(sides)) if group == "interleaved" else ORDERS
    require(set(binaries) == set(sides), "二进制集合错误")
    if group == "control":
        require(binaries["baseline"] == binaries["candidate"], "A/A 必须使用相同二进制")
    else:
        require(binaries["baseline"] != binaries["candidate"], "A/B 必须使用不同核心二进制")
    if group == "interleaved":
        require(binaries["baseline"] == binaries["control"], "同轮控制必须复用旧二进制")
    comparisons = json.loads((root / "comparison.json").read_text())
    expected = {
        "focus": ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "bytes-hot-t1-memory-r1"],
        "control": ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "bytes-hot-t1-memory-r1"],
        "remaining": ["u64-uniform-t1-memory-r1", "u64-hot-t1-memory-r1", "u64-hot-t1-disk-r1", "bytes-uniform-t1-memory-r1", "bytes-hot-t4-disk-r1"],
        "interleaved": ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "u64-hot-t1-disk-r1"],
    }[group]
    require([item["case"] for item in comparisons] == expected, "固定场景不完整")
    count = 0
    for item in comparisons:
        case = item["case"]
        pairs = json.loads((root / (case + "-pairs.json")).read_text())
        require(len(pairs) == len(orders), "配对数量错误")
        identity = case.rsplit("-", 2)[0]
        raw = gzip.decompress((root / "inputs" / (identity + ".trace.gz")).read_bytes())
        require(hashlib.sha256(raw).hexdigest() == item["input_sha256"], "输入哈希错误")
        model = trace_audit(TraceBytes(raw), case.startswith("bytes-"), "-hot-" in case, 4 if "-t4-" in case else 1)
        for number, (pair, order) in enumerate(zip(pairs, orders), 1):
            require(pair["pair"] == number and pair["order"] == list(order), "固定顺序错误")
            for side in sides:
                name = f"{case}-pair{number}-{side}"
                with (root / "runs" / (name + ".csv")).open() as stream:
                    rows = list(csv.DictReader(stream))
                require(len(rows) == 1 and rows[0] == pair[side], "逐轮 CSV 与汇总不一致")
                row = rows[0]
                require(row["输入标识"] == identity and int(row["操作数"]) == 16384, "输入标识或操作数错误")
                success, missing = int(row["成功"]), int(row["缺失"])
                require(model["成功"] <= success <= 16384 and success + missing == 16384, "业务结果超过盲删合法集合")
                require(int(row["应用写入字节"]) == model["应用写入字节"], "写放大分母变更")
                for kind in ["读取", "写入", "RMW", "删除"]:
                    require(int(row[kind + "操作数"]) == 4096, "四操作未全部执行")
                log = (root / "runs" / (name + ".txt")).read_text()
                require(f"基准 {case} 通过：16384 步四操作、" in log and "2048 个键与墓碑校验" in log, "业务或恢复未通过")
                require("panicked at" not in log, "运行出现恐慌")
                count += 1
        medians = {side: {metric: statistics.median(float(pair[side][metric]) for pair in pairs) for metric in METRICS} for side in sides}
        require(item["medians"] == medians, "中位数错误")
        tp = medians["candidate"]["每秒操作"] / medians["baseline"]["每秒操作"]
        p99 = medians["candidate"]["P99纳秒"] / medians["baseline"]["P99纳秒"]
        require(item["throughput_ratio"] == tp and item["p99_ratio"] == p99, "版本比值错误")
        require(item["review_required"] == (tp < .75 or p99 > 1.30), "原复核线被改变")
        if group == "interleaved":
            tp = medians["control"]["每秒操作"] / medians["baseline"]["每秒操作"]
            p99 = medians["control"]["P99纳秒"] / medians["baseline"]["P99纳秒"]
            require(item["control_throughput_ratio"] == tp and item["control_p99_ratio"] == p99, "控制比值错误")
            require(item["control_review_required"] == (tp < .75 or p99 > 1.30), "控制复核线被改变")
    red = any(item["review_required"] or item.get("control_review_required", False) for item in comparisons)
    expected_conclusion = "failure" if red else "success"
    require(run["conclusion"] == expected_conclusion and run["jobs"][0]["conclusion"] == expected_conclusion, "最终状态不符合原数值判定")
    required_steps = {"验证核心身份并记录原生环境", "两个核心使用完全相同的基准驱动", "保存全部原始配对证据"}
    steps = [step for step in run["jobs"][0]["steps"] if step["name"] in required_steps]
    require({step["name"] for step in steps} == required_steps and all(step["conclusion"] == "success" for step in steps), "构建或保存证据失败")
    return {"group": group, "runs": count, "operations": count * 16384, "recovered_keys": count * 2048,
            "review_required_cases": [item["case"] for item in comparisons if item["review_required"]],
            "control_review_required_cases": [item["case"] for item in comparisons if item.get("control_review_required", False)]}


def collect(source, output, run_id, sha, group):
    run = json.loads(subprocess.check_output(["gh", "run", "view", run_id, "--json", "headSha,status,conclusion,jobs,url"], text=True))
    output.mkdir(parents=True, exist_ok=False)
    (output / "inputs").mkdir()
    (output / "runs").mkdir()
    for name in ["environment.txt", "baseline-driver-files.txt", "build.txt", "checks.txt"]:
        shutil.copyfile(source / name, output / name)
    group_file = source / "group.txt"
    if group_file.exists():
        require(group_file.read_text().strip() == group, "场景组不符")
    else:
        require((sha, group) in [("b1925d95de3627ab2b02c9fa8bc2da2c4117b8fc", "focus"), ("9777ed16bdd32b74bb21403e4213c6d599800939", "remaining")], "缺失场景组且不是已核对的早期驱动")
    workflow = subprocess.check_output(["git", "show", sha + ":.github/workflows/benchmark.yml"], text=True)
    (output / "workflow.yml").write_text(workflow)
    results = source / "results"
    for file in results.glob("*.json"):
        shutil.copyfile(file, output / file.name)
    for folder in results.iterdir():
        if not folder.is_dir():
            continue
        shutil.copyfile(folder / "matrix.csv", output / "runs" / (folder.name + ".csv"))
        shutil.copyfile(results / (folder.name + ".txt"), output / "runs" / (folder.name + ".txt"))
        for trace in folder.glob("*.trace"):
            packed = output / "inputs" / (trace.name + ".gz")
            raw = trace.read_bytes()
            require(not packed.exists() or gzip.decompress(packed.read_bytes()) == raw, "输入发生变化")
            packed.write_bytes(gzip.compress(raw, mtime=0))
    (output / "metadata.json").write_text(json.dumps({"run": run, "diagnostic_sha": sha, "baseline": BASELINE, "candidate": CANDIDATE, "group": group}, ensure_ascii=False, indent=2) + "\n")
    result = verify(output)
    (output / "audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--sha")
    parser.add_argument("--group", choices=["focus", "remaining", "control", "interleaved"])
    args = parser.parse_args()
    if args.output:
        require(all([args.run_id, args.sha, args.group]), "采集时需要运行、提交和场景组")
        result = collect(args.directory, args.output, args.run_id, args.sha, args.group)
    else:
        result = verify(args.directory)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("材料核对通过；review_required 中的数值复核仍为红色，不能由本审核推导性能恢复。")


if __name__ == "__main__":
    main()
