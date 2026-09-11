#!/usr/bin/env python3
"""复核归档写入对照的哈希、完整样本和中位数；不能把局部收益当作 P3 总体验收。"""
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


def audit(root):
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["版本"] == 1 and manifest["每端每组样本"] == 12, "对照版本或样本数错误")
    for name, digest in manifest["文件SHA256"].items():
        require(Path(name).name == name, "归档名称不能跳出目录")
        require(hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, "归档哈希不匹配：" + name)
    groups = list(itertools.product(["原子u64", "变长32至512字节"], ["256键均匀", "单键热点"], ["1", "4"]))
    axes = {(*group, str(round_)) for group in groups for round_ in range(1, 4)}
    data = {"baseline": [], "candidate": []}
    for trial in range(4):
        for name in data:
            with (root / f"{trial}-{name}.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            require(len(rows) == 24 and {tuple(row[k] for k in ["布局", "分布", "线程", "轮次"]) for row in rows} == axes,
                    "每次执行必须包含全部 24 个唯一场景样本")
            for row in rows:
                require(row["操作数"] == "200000" and int(row["拒绝重试"]) >= 0, "操作数或重试计数错误")
                require(0 < int(row["P50纳秒"]) <= int(row["P95纳秒"]) <= int(row["P99纳秒"]), "延迟分位数错误")
                speed = float(row["每秒操作"])
                require(math.isfinite(speed) and speed > 0, "吞吐必须为有限正数")
            data[name].extend(rows)
    result = []
    for layout, distribution, threads in groups:
        medians = {}
        for name, rows in data.items():
            samples = [r for r in rows if (r["布局"], r["分布"], r["线程"]) == (layout, distribution, threads)]
            require(len(samples) == 12, "不能丢弃原始触发复核的样本")
            medians[name] = {metric: statistics.median(float(row[metric]) for row in samples)
                             for metric in ["每秒操作", "P99纳秒"]}
        result.append({"布局": layout, "分布": distribution, "线程": threads, "每端样本": 12,
                       "中位数": medians,
                       "吞吐比": medians["candidate"]["每秒操作"] / medians["baseline"]["每秒操作"],
                       "P99比": medians["candidate"]["P99纳秒"] / medians["baseline"]["P99纳秒"]})
    require(result == json.loads((root / "comparison.json").read_text()), "重算中位数与归档报告不一致")
    for name, events, bytes_ in [("baseline", 600000, 20800000), ("candidate", 400000, 4800000)]:
        with (root / f"{name}-alloc.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        require(rows == [{"操作数": "200000", "分配事件": str(events), "释放事件": str(events), "分配字节": str(bytes_)}],
                "本次固定分配诊断与原始计数不符")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="归档对照目录")
    args = parser.parse_args()
    audit(args.directory)
    print("归档复核通过：8 组，每端每组 12 个真实样本；分配事件和全部中位数一致，P3 总体验收仍未完成。")


if __name__ == "__main__":
    main()
