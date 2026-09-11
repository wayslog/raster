#!/usr/bin/env python3
"""复核可变边界查询的成对实验；共享既有矩阵格式，保留所有原始样本。"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

from audit_serial_precheck import audit
from audit_result_pool import read_rows, require


def audit_p3(root):
    manifest = json.loads((root / "manifest.json").read_text())
    report = json.loads((root / "p3/comparison.json").read_text())
    require(report["驱动SHA256"] == hashlib.sha256((root / "driver.rs").read_bytes()).hexdigest(), "P3 驱动不一致")
    current = report["结果"]["current"]
    require(current["提交"] == manifest["基线提交"], "P3 候选基线不符")
    require(current["候选补丁SHA256"] == hashlib.sha256((root / "candidate.patch").read_bytes()).hexdigest(), "P3 候选补丁不符")
    require(current["二进制SHA256"] == manifest["二进制SHA256"]["candidate"], "P3 候选二进制不符")
    require(report["结果"]["old"]["提交"] == "7f2f03b646e85b30c81f31405e3dc2325086da2a", "旧 P3 提交不符")
    axes = {("原子u64", "单键热点", "1", str(n)) for n in range(1, 4)}
    values = {}
    for name in ["current", "old"]:
        rows = read_rows(root / "p3" / (name + ".csv"), axes, 200000)
        require(all(r["拒绝重试"] == "0" for r in rows), "P3 热点出现拒绝重试")
        values[name] = {key: statistics.median(float(r[key]) for r in rows)
                        for key in ["每秒操作", "P50纳秒", "P95纳秒", "P99纳秒"]}
        require(values[name] == report["结果"][name]["中位数"], "P3 中位数不符")
    speed = values["current"]["每秒操作"] / values["old"]["每秒操作"]
    latency = values["current"]["P99纳秒"] / values["old"]["P99纳秒"]
    require(speed == report["吞吐比"] and latency == report["P99比"], "P3 比值不符")
    require(int(speed < 0.75 or latency > 1.3) == report["退出码"], "P3 原复核结论不符")
    return report["退出码"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="归档实验目录")
    args = parser.parse_args()
    for row in audit(args.directory):
        print(row["布局"], row["分布"], row["线程"], "吞吐比", row["吞吐比"], "P99比", row["P99比"])
    print("原 P3 对照数据一致，复核退出码：", audit_p3(args.directory))
    print("可变边界归档一致：192 行完整矩阵及 36 行补充；不代表旧 P3 总体复核完成。")


if __name__ == "__main__":
    main()
