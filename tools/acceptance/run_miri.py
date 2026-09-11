#!/usr/bin/env python3
"""对页、值布局、缓存和验收分配器运行内存安全检查，保留每项原始输出。"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="全新的证据目录")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {"提交": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                   "有未提交变更": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True)),
                   "解释器": subprocess.check_output(["cargo", "+nightly", "miri", "--version"], text=True).strip()}
    (args.output / "environment.json").write_text(json.dumps(environment, ensure_ascii=False, indent=2))
    forgotten = "log::page::tests::无效长度对齐溢出与遗忘范围均安全拒绝"
    cases = [
        ("页范围及预分配", ["--lib", "log::page::", "--", "--skip", forgotten], 6, ""),
        ("页内值布局", ["--lib", "log::value::tests::"], 7, ""),
        ("记录生命周期", ["--lib", "log::tests::"], 12, ""),
        ("缓存字节区", ["--lib", "cache::arena::tests::"], 1, ""),
        ("缓存读者及淘汰", ["--lib", "cache::tests::"], 5, ""),
        ("验收分配器", ["--test", "p9_allocator"], 1, ""),
        # 本例故意遗忘租约，验证只阻碍回收而非释放活跃范围；其余案例开启泄漏检测。
        ("故意遗忘范围", ["--lib", forgotten, "--", "--exact"], 1, "-Zmiri-ignore-leaks"),
    ]
    results = []
    for name, arguments, expected, flags in cases:
        command = ["cargo", "+nightly", "miri", "test", "--locked", *arguments]
        env = {**os.environ, "MIRIFLAGS": flags, "CARGO_TERM_COLOR": "never", "RUST_BACKTRACE": "0"}
        completed = subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, timeout=600)
        output = completed.stdout.replace(str(Path.cwd()), "工作树").replace(str(Path.home()), "用户目录")
        (args.output / f"{name}.txt").write_text(output)
        counts = re.findall(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;", output)
        passed = completed.returncode == 0 and counts == [str(expected)]
        results.append({"检查": name, "命令": command, "MIRIFLAGS": flags,
                        "预期测试数": expected, "退出码": completed.returncode, "通过": passed})
        (args.output / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2))
        print(f"{name}：{'通过' if passed else '失败'}，预期 {expected} 项", flush=True)
        if not passed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
