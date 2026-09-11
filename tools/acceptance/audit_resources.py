#!/usr/bin/env python3
"""核对指定提交的双平台资源制品；任务成功本身不代替业务和逐轮门槛。"""
import argparse
import csv
import json
from pathlib import Path
import re
import subprocess


def require(condition, message):
    if not condition:
        raise ValueError(message)


def audit(root, sha, run_id):
    run = json.loads(subprocess.check_output(
        ["gh", "run", "view", run_id, "--json", "headSha,status,conclusion,jobs"],
        text=True, timeout=60,
    ))
    require(run["headSha"] == sha, "运行提交不匹配")
    require(run["status"] == "completed" and run["conclusion"] == "success", "运行尚未成功结束")
    require(len(run["jobs"]) == 2 and all(j["conclusion"] == "success" for j in run["jobs"]),
            "两个原生任务必须全部成功")
    budgets = {"存活分配字节": 256 * 1024, "存活分配数": 16,
               "文件描述符": 2, "线程": 1, "RSS字节": 16 * 1024 * 1024}
    columns = ["轮次", "存活分配字节", "存活分配数", "分配峰值字节", "RSS字节", "文件描述符", "线程", "耗时毫秒"]
    platforms = {"ubuntu-24.04": "x86_64-unknown-linux-gnu", "macos-14": "aarch64-apple-darwin"}
    verified = []
    for platform, target in platforms.items():
        folder = root / f"p9-resources-{platform}"
        environment = (folder / "environment.txt").read_text()
        checks = (folder / "checks.txt").read_text()
        require(environment.splitlines()[0] == sha and "release: 1.98.1" in environment,
                f"{platform} 的环境或工具链不匹配")
        require(f"host: {target}" in environment, f"{platform} 的原生 target 不匹配")
        require("特性：all；模式：release；预热：8；测量：64" in environment, "运行配置不匹配")
        require(len(re.findall(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", checks)) == 1,
                "计数分配器测试必须实际运行且通过")
        require(checks.count("磁盘生命周期通过：40 个键") == 72, "完整业务循环次数不匹配")
        require(checks.count("同进程资源循环通过：预热 8 轮，测量 64 轮") == 1,
                "缺少最终资源通过结果")
        require("test result: FAILED" not in checks, "测试包含失败")
        require(not list(folder.glob("cycle-*")), "成功制品仍包含未清理的业务目录")
        with (folder / "resources.csv").open(newline="") as stream:
            reader = csv.DictReader(stream)
            require(reader.fieldnames == columns, "资源 CSV 列不匹配")
            rows = [{key: int(value) for key, value in row.items()} for row in reader]
        require([row["轮次"] for row in rows] == list(range(65)), "资源轮次缺失或重复")
        require(all(value >= 0 for row in rows for value in row.values()), "资源指标为负数")
        baseline = rows[0]
        for row in rows[1:]:
            for metric, increase in budgets.items():
                require(row[metric] <= baseline[metric] + increase,
                        f"{platform} 第 {row['轮次']} 轮的 {metric} 超过预设门槛")
        verified.append({
            "平台": platform, "target": target, "完整业务循环": 72, "恢复次数": 144,
            "测量耗时毫秒": sum(row["耗时毫秒"] for row in rows[1:]),
            "指标": {key: {"基线": baseline[key], "最小": min(row[key] for row in rows[1:]),
                          "最大": max(row[key] for row in rows[1:])}
                   for key in columns[1:-1]},
        })
    return {"sha": sha, "run": run, "verified": verified}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True, help="完整提交 SHA")
    parser.add_argument("--run-id", required=True, help="资源工作流编号")
    parser.add_argument("directory", type=Path, help="gh run download 下载的制品目录")
    args = parser.parse_args()
    result = audit(args.directory, args.sha, args.run_id)
    with (args.directory / "audit.json").open("x") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    print(json.dumps(result["verified"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
