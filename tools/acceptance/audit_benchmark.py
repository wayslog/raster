#!/usr/bin/env python3
"""独立核对完整性能矩阵、固定输入、成功/缺失计数及写放大口径；不宣称性能回退通过。"""
import argparse
import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
import statistics
import subprocess

SEED = 0x7261737465720001
KEYS = 2048
OPERATIONS = 16384
KINDS = ["读取", "写入", "RMW", "删除"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def initial_value(variable, ordinal):
    return bytes([ordinal % 256]) * ((ordinal % 16 + 1) * 32) if variable else (SEED + ordinal) % (1 << 64)


def payload(value):
    return len(value) if isinstance(value, bytes) else 8


def trace_audit(path, variable, hot, threads):
    raw = path.read_bytes()
    lines = raw.decode().splitlines()
    require(lines[0] == f"raster-trace 1 {SEED}", "轨迹头或种子错误")
    require(len(lines) == 1 + KEYS + OPERATIONS, "预装和业务轨迹长度错误")
    per_key = KEYS // threads
    per_phase = OPERATIONS // threads // 4
    records = {}
    application = success = missing = cursor = 0
    for worker in range(threads):
        for serial in range(per_key + 4 * per_phase):
            fields = lines[cursor + 1].split()
            cursor += 1
            phase = -1 if serial < per_key else (serial - per_key) // per_phase
            i = serial if phase == -1 else (serial - per_key) % per_phase
            local = i if phase == -1 else 0 if hot else (i * 17 % (per_key // 2)) * 2 if phase == 3 else i * 17 % per_key
            key = worker * per_key + local
            require(fields[:3] == [str(worker), str(serial), key.to_bytes(8, "little").hex()], "线程、序号或键分布错误")
            op = fields[3:]
            expected_kind = "upsert" if phase == -1 else ["read", "upsert", "rmw", "delete"][phase]
            require(op[0] == expected_kind, "操作分组或比例错误")
            if expected_kind in ("upsert", "rmw"):
                data = op[1:] if expected_kind == "upsert" else op[2:]
                require(len(data) == 2 and data[0] == ("b" if variable else "u"), "值编码类型错误")
                value = bytes.fromhex(data[1]) if variable else int(data[1])
                expected = initial_value(variable, key if phase == -1 else worker * per_key + i + 7) if expected_kind == "upsert" else b"\xa5" if variable else 1
                require(value == expected, "固定值输入发生变化")
                if expected_kind == "rmw":
                    require(op[1] == "1", "RMW 创建选项错误")
                    old = records[key]
                    require(old is not None, "固定 RMW 阶段应有旧值")
                    records[key] = old + value if variable else (old + value) % (1 << 64)
                else:
                    records[key] = value
                application += payload(value)
                success += phase != -1
            elif expected_kind == "read":
                require(op == ["read", "1"] and records[key] is not None, "固定读取阶段或墓碑选项错误")
                success += 1
            else:
                require(op == ["delete", "0"], "删除选项错误")
                if records[key] is None:
                    missing += 1
                else:
                    records[key] = None
                    application += 8
                    success += 1
    require(len(records) == KEYS and success + missing == OPERATIONS, "模型结果不完整")
    return {"输入SHA256": hashlib.sha256(raw).hexdigest(), "应用写入字节": application,
            "成功": success, "缺失": missing, "墓碑": 0,
            "恢复值数": sum(v is not None for v in records.values()),
            "恢复墓碑数": sum(v is None for v in records.values())}


def verify_matrix(root):
    with (root / "matrix.csv").open(newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames is not None and len(reader.fieldnames) == 49 and len(set(reader.fieldnames)) == 49,
                "CSV 列数量或唯一性错误")
        rows = list(reader)
    axes = list(itertools.product(["原子u64", "变长字节"], ["均匀", "每线程热点"], [1, 4], ["纯内存", "超内存"], [1, 2, 3]))
    keys = [(r["布局"], r["分布"], int(r["线程"]), r["存储"], int(r["轮次"])) for r in rows]
    require(len(rows) == 48 and set(keys) == set(axes), "必须有全部 48 组且没有重复")
    require(not any(p.is_dir() for p in root.iterdir()), "成功矩阵仍有未清理的合成目录")
    traces = {}
    for row, (layout, distribution, threads, storage, _) in zip(rows, keys):
        variable, hot = layout == "变长字节", distribution == "每线程热点"
        identity = f"{'bytes' if variable else 'u64'}-{'hot' if hot else 'uniform'}-t{threads}"
        require(row["输入标识"] == identity, "输入标识不匹配")
        if identity not in traces:
            traces[identity] = trace_audit(root / f"{identity}.trace", variable, hot, threads)
        expected = traces[identity]
        numbers = {name: int(value) for name, value in row.items() if name not in ["布局", "分布", "存储", "输入标识", "每秒操作", "写放大含预装检查点"]}
        require(all(n >= 0 for n in numbers.values()), "计数不能为负数")
        require(numbers["种子"] == SEED and numbers["键数"] == KEYS and numbers["操作数"] == OPERATIONS, "固定运行参数错误")
        for name in ["应用写入字节", "墓碑"]:
            require(numbers[name] == expected[name], f"{identity} 的 {name} 与独立模型不符")
        # 此固定输入先完成 Read/Upsert/RMW，再普通删除；唯一可变返回来自重复删除。
        # 活跃值首次删除必须成功，重复删除可成功或缺失，业务总数仍精确守恒。
        require(expected["成功"] <= numbers["成功"] <= expected["成功"] + expected["缺失"], "成功数超出普通盲删允许范围")
        require(0 <= numbers["缺失"] <= expected["缺失"] and numbers["成功"] + numbers["缺失"] == OPERATIONS, "缺失数或业务总数不符")
        require(0 < numbers["P50纳秒"] <= numbers["P95纳秒"] <= numbers["P99纳秒"], "分位数错误")
        for kind in KINDS:
            require(numbers[f"{kind}操作数"] == OPERATIONS // 4 and numbers[f"{kind}P99纳秒"] > 0, "四操作采样不完整")
            require(numbers[f"{kind}挂起数"] <= OPERATIONS // 4, "挂起数量超过业务数量")
        for direction in ["读", "写"]:
            require(numbers[f"预装累计{direction}字节"] <= numbers[f"业务后累计{direction}字节"] <= numbers[f"持久化累计{direction}字节"], "累计 I/O 字节倒退")
        require(numbers["业务后驻留页字节"] <= numbers["窗口字节"], "驻留页超过预算")
        if storage == "纯内存":
            require(numbers["窗口字节"] == 64 * 1024 * 1024 and numbers["业务后日志跨度"] < numbers["窗口字节"], "纯内存窗口不匹配")
            require(numbers["业务后累计读字节"] == numbers["业务后累计写字节"] == 0 and all(numbers[f"{kind}挂起数"] == 0 for kind in KINDS), "纯内存业务发生 I/O 或挂起")
        else:
            require(numbers["窗口字节"] == 64 * 1024 and numbers["预装日志跨度"] > numbers["窗口字节"], "数据没有超过固定驻留窗口")
            require(numbers["业务后累计读字节"] > 0 and numbers["业务后累计写字节"] > 0 and sum(numbers[f"{kind}挂起数"] for kind in KINDS) > 0, "超内存路径未真正运行")
        require(numbers["恢复读字节"] > 0 and numbers["恢复写字节"] > 0 and numbers["恢复纳秒"] > 0 and numbers["检查点纳秒"] > 0, "检查点或恢复测量缺失")
        require(all(numbers[f"{stage}RSS字节"] > 0 for stage in ["预装", "业务后", "检查点后", "恢复后"]), "RSS 采样缺失")
        throughput, amplification = float(row["每秒操作"]), float(row["写放大含预装检查点"])
        require(math.isfinite(throughput) and throughput > 0 and math.isfinite(amplification), "非有限性能指标")
        require(abs(amplification - numbers["持久化累计写字节"] / numbers["应用写入字节"]) <= 0.00000051, "写放大计算错误")
    require(len(traces) == 8 and len(list(root.glob("*.trace"))) == 8, "应有八份固定输入")
    medians = []
    for layout, distribution, threads, storage in itertools.product(["原子u64", "变长字节"], ["均匀", "每线程热点"], [1, 4], ["纯内存", "超内存"]):
        group = [r for r in rows if (r["布局"], r["分布"], int(r["线程"]), r["存储"]) == (layout, distribution, threads, storage)]
        metrics = {name: statistics.median(float(r[name]) for r in group) for name in [
            "每秒操作", "P50纳秒", "P95纳秒", "P99纳秒", "写放大含预装检查点", "恢复纳秒",
            "业务后RSS字节", "业务后驻留页字节", "持久化累计读字节", "持久化累计写字节",
        ]}
        medians.append({"布局": layout, "分布": distribution, "线程": threads, "存储": storage, **metrics})
    return {"完整场景": 48, "业务操作": 48 * OPERATIONS, "恢复校验键": 48 * KEYS,
            "输入": traces, "三轮中位数": medians, "原性能回退验收": "未完成"}


def verify_native(root, sha, run_id):
    run = json.loads(subprocess.check_output(
        ["gh", "run", "view", run_id, "--json", "headSha,status,conclusion,jobs"],
        text=True, timeout=60,
    ))
    require(run["headSha"] == sha and run["status"] == "completed" and run["conclusion"] == "success",
            "指定提交的性能运行必须成功结束")
    require(len(run["jobs"]) == 2 and all(job["conclusion"] == "success" for job in run["jobs"]),
            "两个原生性能任务必须全部成功")
    verified = []
    inputs = None
    for platform, target in [("ubuntu-24.04", "x86_64-unknown-linux-gnu"), ("macos-14", "aarch64-apple-darwin")]:
        folder = root / f"p9-benchmark-{platform}"
        env = (folder / "environment.txt").read_text()
        checks = (folder / "checks.txt").read_text()
        require(env.splitlines()[0] == sha and "release: 1.98.1" in env and f"host: {target}" in env,
                "制品提交、工具链或原生 target 不匹配")
        require("特性：all；模式：release；完整矩阵：48；每组业务操作：16384" in env,
                "性能配置不匹配")
        require(len(re.findall(r"test result: ok\. 3 passed; 0 failed; 0 ignored;", checks)) == 1,
                "计量、固定输入与盲删契约三个测试必须实际通过")
        expected_ids = {f"{layout}-{distribution}-t{threads}-{storage}-r{round_}"
                        for layout, distribution, threads, storage, round_ in itertools.product(
                            ["u64", "bytes"], ["uniform", "hot"], [1, 4], ["memory", "disk"], [1, 2, 3])}
        completed = re.findall(r"^基准 (\S+) 通过：16384 步四操作、[14] 个会话恢复、2048 个键与墓碑校验$", checks, re.M)
        require(len(completed) == 48 and set(completed) == expected_ids, "逐组业务及恢复输出不完整")
        require(checks.count("性能矩阵业务校验通过：48 组；") == 1 and "test result: FAILED" not in checks,
                "缺少最终完整矩阵结果")
        matrix = verify_matrix(folder / "matrix")
        require(matrix == json.loads((folder / "matrix/audit.json").read_text()),
                "下载后独立复核与原生保存结果不一致")
        require(inputs is None or inputs == matrix["输入"], "两平台固定输入不一致")
        inputs = matrix["输入"]
        verified.append({"平台": platform, "target": target, **matrix})
    return {"sha": sha, "run": run, "verified": verified}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="完整矩阵目录")
    parser.add_argument("--sha", help="原生下载制品的完整提交 SHA；与 --run-id 同用")
    parser.add_argument("--run-id", help="原生性能工作流编号；与 --sha 同用")
    args = parser.parse_args()
    require(bool(args.sha) == bool(args.run_id), "原生审计必须同时指定提交和运行编号")
    result = verify_native(args.directory, args.sha, args.run_id) if args.sha else verify_matrix(args.directory)
    with (args.directory / "audit.json").open("x") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    platforms = 2 if args.sha else 1
    print(f"完整矩阵已复核：{48 * platforms} 组，{48 * OPERATIONS * platforms} 次业务操作，{48 * KEYS * platforms} 个恢复键校验；本矩阵不代替独立的原性能回退复核。")


if __name__ == "__main__":
    main()
