#!/usr/bin/env python3
"""核对受限成本诊断的真实输出与计时；不能用于证明移除协议的引擎兼容。"""
import argparse
import csv
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics

from audit_result_pool import require


def row(path, count):
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    require(len(rows) == 1, "诊断必须恰好输出一个完整循环")
    result = rows[0]
    require(result["操作数"] == str(count) and result["预热"] == "1000", "诊断工作量错误")
    require(int(result["读取结果"]) == count + 1000 and int(result["接受序号"]) == count + 1001,
            "最终驻留值或接受进度错误")
    nanos, speed = float(result["耗时纳秒"]), float(result["每秒操作"])
    require(math.isfinite(nanos) and nanos > 0 and math.isfinite(speed) and speed > 0, "诊断计时无效")
    require(abs(speed - count * 1e9 / nanos) <= 1, "吞吐与计时不匹配")
    return result


def audit(root):
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["版本"] == 1 and len(manifest["变体"]) == 8, "诊断版本或组合数错误")
    for name, digest in manifest["文件SHA256"].items():
        path = root / name
        require(path.resolve().is_relative_to(root.resolve()), "诊断路径非法")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, "诊断哈希不一致：" + name)
    expected = list(itertools.product([False, True], repeat=3))
    flags = [(v["移除邮箱登记与注销"], v["移除版本表登记与等待"], v["移除活动请求计数"])
             for v in manifest["变体"]]
    require(flags == expected, "八种协议组合缺失或重复")
    base = "m0v0a0"
    reports = []
    for v, (mailbox, version, activity) in zip(manifest["变体"], expected):
        name = f"m{int(mailbox)}v{int(version)}a{int(activity)}"
        require(v["名称"] == name, "组合名称不匹配")
        row(root / "paired" / f"smoke-{name}.csv", 1000000)
        if name == base:
            continue
        data = {n: [row(root / "paired" / f"{name}-{trial}-{n}.csv", 20000000) for trial in range(3)]
                for n in [base, name]}
        speeds = {n: statistics.median(float(r["每秒操作"]) for r in rows) for n, rows in data.items()}
        nanos = {n: statistics.median(float(r["耗时纳秒"]) / int(r["操作数"]) for r in rows)
                 for n, rows in data.items()}
        reports.append({**v, "每端样本": 3, "吞吐中位数": speeds, "每操作纳秒中位数": nanos,
                        "吞吐比": speeds[name] / speeds[base], "每操作节省纳秒": nanos[base] - nanos[name]})
    require(reports == json.loads((root / "paired" / "comparison.json").read_text()), "组合中位数不一致")
    data = {n: [row(root / "old-pair" / f"{trial}-{n}.csv", 20000000) for trial in range(3)]
            for n in [base, "old-p3"]}
    nanos = {n: statistics.median(float(r["耗时纳秒"]) / int(r["操作数"]) for r in rows)
             for n, rows in data.items()}
    old = json.loads((root / "old-pair" / "comparison.json").read_text())
    require(old["每端样本"] == 3 and old["每操作纳秒中位数"] == nanos
            and old["当前与旧P3成本比"] == nanos[base] / nanos["old-p3"], "旧 P3 受限对照不一致")
    require(old["二进制SHA256"][base] == manifest["变体"][0]["二进制SHA256"]
            and old["二进制SHA256"]["old-p3"] == manifest["补充二进制SHA256"]["old-p3"], "旧对照二进制不一致")
    for name in ["profile-baseline", "old-p3"]:
        row(root / "profiles" / f"{name}-profile-run.csv", 40000000)
        text = (root / "profiles" / f"{name}-sample.txt").read_text()
        require("Call graph:" in text and "Sort by top of stack" in text, "采样未产生调用图")
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="诊断归档目录")
    args = parser.parse_args()
    audit(args.directory)
    print("诊断归档一致：8 个冒烟、42 个成对循环、6 个旧版对照及2个采样后完整值检查；不代表兼容性验收通过。")


if __name__ == "__main__":
    main()
