#!/usr/bin/env python3
"""复算完整性能复核材料；数据一致不等于数值回到原复核线以内。"""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import statistics

from audit_cost_attribution import row
from audit_result_pool import read_rows, require


def audit_p3(root, manifest):
    plan = json.loads((root / "plan.json").read_text())
    require(plan["当前提交"] == manifest["当前实现"] and plan["旧提交"] == manifest["旧P3"], "P3 比较版本错误")
    require(plan["操作数"] == 200000 and plan["每端每组样本"] == 12, "P3 工作量被改变")
    require(plan["吞吐复核线"] == 0.75 and plan["P99复核线"] == 1.3, "原复核线被改变")
    require(plan["顺序"] == [["old", "current"], ["current", "old"], ["current", "old"], ["old", "current"]], "配对顺序错误")
    require(plan["驱动SHA256"] == hashlib.sha256((root / "driver.rs").read_bytes()).hexdigest(), "P3 驱动不一致")
    groups = list(itertools.product(["原子u64", "变长32至512字节"], ["256键均匀", "单键热点"], ["1", "4"]))
    axes = {(*group, str(n)) for group in groups for n in range(1, 4)}
    data = {name: [r for n in range(4) for r in read_rows(root / "full" / f"{n}-{name}.csv", axes, 200000)]
            for name in ["old", "current"]}
    require(len(list((root / "full").glob("*.csv"))) == 8, "P3 原始批次数不符")
    reports = []
    for group in groups:
        medians = {}
        for name, rows in data.items():
            selected = [r for r in rows if tuple(r[k] for k in ["布局", "分布", "线程"]) == group]
            require(len(selected) == 12, "P3 样本缺失")
            medians[name] = {k: statistics.median(float(r[k]) for r in selected)
                             for k in ["每秒操作", "P50纳秒", "P95纳秒", "P99纳秒", "拒绝重试"]}
        speed = medians["current"]["每秒操作"] / medians["old"]["每秒操作"]
        latency = medians["current"]["P99纳秒"] / medians["old"]["P99纳秒"]
        reports.append({**dict(zip(["布局", "分布", "线程"], group)), "每端样本": 12, "中位数": medians,
                        "吞吐比": speed, "P99比": latency, "触发复核": speed < 0.75 or latency > 1.3})
    require(reports == json.loads((root / "full/comparison.json").read_text()), "P3 中位数或复核结论不一致")
    expected = {"页字节": "33554432", "内存页": "4", "可变比例": "0.9", "索引桶": "1024", "缓存启用": "false",
                "缓存容量": "0", "自动压缩": "false", "维护工作者": "1", "会话数": "96", "挂起限额": "1024",
                "结果限额": "1024", "日志预分配": "false", "段字节": "1073741824"}
    for name in ["old", "current"]:
        with (root / (name + "-config.csv")).open(newline="") as source:
            settings = list(csv.DictReader(source))
        require(len(settings) == 13 and {r["项目"]: r["值"] for r in settings} == expected, "共同默认配置不一致")
        with (root / (name + "-alloc.csv")).open(newline="") as source:
            allocations = list(csv.DictReader(source))
        require(allocations == [{"操作数": "200000", "分配事件": "200000", "释放事件": "200000", "分配字节": "1600000"}],
                "同步热点分配证据不符")
    return reports


def audit_cost(root, parent):
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["版本"] == 1 and manifest["基线提交"] == parent["当前实现"], "成本诊断版本错误")
    flags = list(itertools.product([False, True], repeat=3))
    actual = [(v["移除邮箱登记与注销"], v["移除版本表登记与等待"], v["移除写入入口阶段观察"]) for v in manifest["变体"]]
    require(actual == flags, "协议组合缺失或重复")
    require(len(list((root / "paired").glob("*.csv"))) == 50
            and len(list((root / "old-pair").glob("*.csv"))) == 6, "受限循环数量不符")
    base = "m0v0o0"
    require((root / (base + ".patch")).read_bytes() == b"", "成本基线包含诊断修改")
    reports = []
    for v, (mailbox, version, observation) in zip(manifest["变体"], flags):
        name = f"m{int(mailbox)}v{int(version)}o{int(observation)}"
        require(v["名称"] == name, "诊断组合名字不符")
        row(root / "paired" / f"smoke-{name}.csv", 1000000)
        if name == base:
            continue
        data = {n: [row(root / "paired" / f"{name}-{trial}-{n}.csv", 20000000) for trial in range(3)]
                for n in [base, name]}
        speeds = {n: statistics.median(float(r["每秒操作"]) for r in rows) for n, rows in data.items()}
        nanos = {n: statistics.median(float(r["耗时纳秒"]) / int(r["操作数"]) for r in rows) for n, rows in data.items()}
        reports.append({**v, "每端样本": 3, "吞吐中位数": speeds, "每操作纳秒中位数": nanos,
                        "吞吐比": speeds[name] / speeds[base], "每操作节省纳秒": nanos[base] - nanos[name]})
    require(reports == json.loads((root / "paired/comparison.json").read_text()), "协议成本中位数不一致")
    nanos = {name: statistics.median(float(row(root / "old-pair" / f"{n}-{name}.csv", 20000000)["耗时纳秒"]) / 20000000
                                    for n in range(3)) for name in [base, "old-p3"]}
    old = json.loads((root / "old-pair/comparison.json").read_text())
    require(old["每端样本"] == 3 and old["每操作纳秒中位数"] == nanos and old["当前与旧P3成本比"] == nanos[base] / nanos["old-p3"],
            "受限旧版对照不一致")
    require(old["二进制SHA256"][base] == manifest["变体"][0]["二进制SHA256"]
            and old["二进制SHA256"]["old-p3"] == manifest["补充二进制SHA256"]["old-p3"], "受限对照二进制不一致")
    cases = [
        ("m1v0o0", "engine::io_hub::tests::其他会话轮询后结果留在原邮箱且错身份不能收取", "Err(Error::Busy)"),
        ("m0v1o0", "engine::version_permit::tests::旧版本全部退出后新版本才可执行且不同键互不阻塞", "!new.ready().unwrap()"),
        ("m0v0o1", "engine::pending_read_tests::提交和轮询自动观察版本且旧请求保持原切分", "on an `Err` value: Busy"),
    ]
    recorded = json.loads((root / "protocols/comparison.json").read_text())
    require(recorded == [{"变体": n, "真实测试": t, "正常退出码": 0, "移除后退出码": 101} for n, t, _ in cases],
            "必须保留真实协议失败")
    for name, test, failure in cases:
        normal = (root / "protocols" / (name + "-normal.txt")).read_text()
        control = (root / "protocols" / (name + "-control.txt")).read_text()
        require(test in normal and "test result: ok. 1 passed; 0 failed;" in normal, "正常实现未实际通过协议测试")
        require(test in control and "test result: FAILED. 0 passed; 1 failed;" in control and failure in control,
                "诊断变体未出现预期协议失败")
    return reports


def audit(root):
    manifest = json.loads((root / "manifest.json").read_text())
    require(manifest["版本"] == 1 and manifest["原始吞吐复核线"] == 0.75 and manifest["原始P99复核线"] == 1.3,
            "复核版本或原始条件改变")
    for name, expected in manifest["文件SHA256"].items():
        file = root / name
        require(file.resolve().is_relative_to(root.resolve()), "归档路径非法")
        require(hashlib.sha256(file.read_bytes()).hexdigest() == expected, "归档哈希错误：" + name)
    return audit_p3(root / "p3", manifest), audit_cost(root / "cost", manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="完整复核归档目录")
    args = parser.parse_args()
    p3, _ = audit(args.directory)
    print("材料一致：192 行原 P3 对照、56 个受限循环、配置与分配输出、三组协议反例。")
    print("仍触发原复核线：", sum(r["触发复核"] for r in p3), "组；验收解释见配套报告。")


if __name__ == "__main__":
    main()
