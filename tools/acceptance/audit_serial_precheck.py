#!/usr/bin/env python3
"""复核序号预检查的完整成对样本；局部收益不替代原 P3 性能复核。"""
import argparse
import hashlib
import itertools
import json
from pathlib import Path
import statistics

from audit_result_pool import compare, read_rows, require


def audit(root):
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["版本"] == 1, "归档版本错误")
    for name, digest in manifest["文件SHA256"].items():
        path = root / name
        require(path.resolve().is_relative_to(root.resolve()), "归档路径非法")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "哈希不一致：" + name)
    groups = list(itertools.product(["原子u64", "变长32至512字节"], ["256键均匀", "单键热点"], ["1", "4"]))
    axes = {(*group, str(round_)) for group in groups for round_ in range(1, 4)}
    data = {"baseline": [], "candidate": []}
    for trial in range(4):
        for name in data:
            data[name].extend(read_rows(root / "full" / f"{trial}-{name}.csv", axes, 200000))
    reports = []
    for group in groups:
        selected = {name: [r for r in rows if tuple(r[k] for k in ["布局", "分布", "线程"]) == group]
                    for name, rows in data.items()}
        reports.append({**dict(zip(["布局", "分布", "线程"], group)),
                        **compare(selected, ["每秒操作", "P99纳秒"])})
    require(reports == json.loads((root / "full" / "comparison.json").read_text()), "完整矩阵中位数不符")
    axes = {("原子u64", "单键热点", "1", str(round_)) for round_ in range(1, 4)}
    for folder in ["round-1", "round-2"]:
        report = json.loads((root / folder / "comparison.json").read_text())
        require(report["每端真实样本"] == 9 and report["基线提交"] == manifest["基线提交"], "热点对照口径不符")
        require(report["二进制SHA256"] == manifest["二进制SHA256"], "完整矩阵与热点补充必须使用相同二进制")
        require(report["驱动SHA256"] == hashlib.sha256((root / "driver.rs").read_bytes()).hexdigest(), "驱动版本不一致")
        medians = {}
        for name in data:
            rows = []
            for trial in range(3):
                rows.extend(read_rows(root / folder / f"{trial}-{name}.csv", axes, 200000))
            require(len(rows) == 9 and all(row["拒绝重试"] == "0" for row in rows), "热点样本不完整")
            medians[name] = {k: statistics.median(float(row[k]) for row in rows)
                             for k in ["每秒操作", "P50纳秒", "P95纳秒", "P99纳秒"]}
        require(medians == report["中位数"], "热点中位数不符")
        for name, metric in [("吞吐比", "每秒操作"), ("P99比", "P99纳秒")]:
            require(report[name] == medians["candidate"][metric] / medians["baseline"][metric], "热点比值不符")
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="归档实验目录")
    args = parser.parse_args()
    for row in audit(args.directory):
        print(row["布局"], row["分布"], row["线程"], "吞吐比", row["吞吐比"], "P99比", row["P99比"])
    print("归档数据一致：完整矩阵 192 个样本及热点补充 36 个样本；不代表旧 P3 总体性能验收通过。")


if __name__ == "__main__":
    main()
