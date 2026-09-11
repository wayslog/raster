#!/usr/bin/env python3
"""复核结果预算池实验的原始数据；明确区分数据一致、校准波动和性能复核。"""
import argparse
import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_rows(path, axes, count):
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    require(len(rows) == len(axes) and {tuple(r[k] for k in ["布局", "分布", "线程", "轮次"]) for r in rows} == axes,
            "场景缺失、重复或不符合固定分组：" + path.name)
    for row in rows:
        require(row["操作数"] == str(count) and int(row["拒绝重试"]) >= 0, "操作数或重试错误")
        require(0 < int(row["P50纳秒"]) <= int(row["P95纳秒"]) <= int(row["P99纳秒"]), "延迟分位数错误")
        require(math.isfinite(float(row["每秒操作"])) and float(row["每秒操作"]) > 0, "吞吐值错误")
    return rows


def compare(data, metrics):
    require(all(len(rows) == 12 for rows in data.values()), "每端必须保留全部 12 个样本")
    medians = {name: {metric: statistics.median(float(row[metric]) for row in rows) for metric in metrics}
               for name, rows in data.items()}
    return {"每端样本": 12, "中位数": medians,
            "吞吐比": medians["candidate"]["每秒操作"] / medians["baseline"]["每秒操作"],
            "P99比": medians["candidate"]["P99纳秒"] / medians["baseline"]["P99纳秒"]}


def audit(root):
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["版本"] == 1, "不支持的归档版本")
    for name, digest in manifest["文件SHA256"].items():
        path = root / name
        require(path.resolve().is_relative_to(root.resolve()) and path.is_file(), "归档路径非法")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "哈希不一致：" + name)
    groups = list(itertools.product(["原子u64", "变长32至512字节"], ["256键均匀", "单键热点"], ["1", "4"]))
    axes = {(*group, str(round_)) for group in groups for round_ in range(1, 4)}
    data = {"baseline": [], "candidate": []}
    for trial in range(4):
        for name in data:
            data[name].extend(read_rows(root / "full" / f"{trial}-{name}.csv", axes, 200000))
    full = []
    for group in groups:
        selected = {name: [r for r in rows if tuple(r[k] for k in ["布局", "分布", "线程"]) == group]
                    for name, rows in data.items()}
        full.append({**dict(zip(["布局", "分布", "线程"], group)), **compare(selected, ["每秒操作", "P99纳秒"])})
    require(full == json.loads((root / "full" / "comparison.json").read_text()), "完整矩阵中位数不符")
    focused = {}
    axes = {("变长32至512字节", "单键热点", "4", str(round_)) for round_ in range(1, 4)}
    for folder, count in [("focused", 200000), ("long", 2000000)]:
        reports = {}
        for mode in ["校准AA", "复核AB"]:
            data = {"baseline": [], "candidate": []}
            for trial in range(4):
                for name in data:
                    data[name].extend(read_rows(root / folder / f"{mode}-{trial}-{name}.csv", axes, count))
            reports[mode] = compare(data, ["每秒操作", "P99纳秒", "拒绝重试"])
        require(reports == json.loads((root / folder / "comparison.json").read_text()), "定向校准中位数不符")
        focused[folder] = reports
    for name, events, bytes_ in [("baseline", 400000, 4800000), ("candidate", 200000, 1600000)]:
        with (root / f"{name}-alloc.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        require(rows == [{"操作数": "200000", "分配事件": str(events), "释放事件": str(events), "分配字节": str(bytes_)}],
                "分配诊断与固定预期不符")
    return full, focused


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="归档实验目录")
    args = parser.parse_args()
    full, focused = audit(args.directory)
    for row in full:
        if row["吞吐比"] < .75 or row["P99比"] > 1.3:
            print("完整矩阵触发原复核线：", row["布局"], row["分布"], row["线程"], row["吞吐比"], row["P99比"])
    for folder, reports in focused.items():
        for mode, row in reports.items():
            print(folder, mode, "吞吐比", row["吞吐比"], "P99比", row["P99比"])
    print("数据复核一致：保留完整矩阵、两种操作量的独立校准与对照；此结论不代表 P3 性能验收通过。")


if __name__ == "__main__":
    main()
