"""运行明确列出的故障与历史矩阵；每项必须匹配并实际通过一个完整测试。"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def clean(text):
    return text.replace(str(Path.cwd()), "工作树").replace(str(Path.home()), "用户目录")


def run(command, path, env, timeout):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=timeout)
    output = result.stdout + result.stderr
    path.write_text(clean(output))
    if result.returncode:
        raise RuntimeError(f"命令退出码 {result.returncode}，见 {path.name}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="本次全新结果目录")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    matrix = json.loads(Path("tools/acceptance/fault-matrix.json").read_text())
    if matrix["版本"] != 1 or not matrix["用例"]:
        raise RuntimeError("故障矩阵版本错误或为空")
    env = os.environ.copy()
    env["CARGO_TERM_COLOR"] = "never"
    env["RUST_BACKTRACE"] = "0"
    env["RASTER_HISTORY_OUTPUT"] = str((args.output / "histories.txt").resolve())
    for key in list(env):
        if key.startswith("RASTER_UPSTREAM_"):
            env.pop(key)
    command = ["cargo", "test", "--locked", "--all-features", "--no-run", "--message-format=json"]
    built = run(command, args.output / "build.txt", env, 600)
    executables = {}
    for line in built.splitlines():
        if not line.startswith("{"):
            continue
        artifact = json.loads(line)
        if artifact.get("reason") == "compiler-artifact" and artifact.get("executable") and artifact.get("profile", {}).get("test"):
            name = artifact["target"]["name"]
            if name in executables and executables[name] != artifact["executable"]:
                raise RuntimeError("测试程序名字不唯一")
            executables[name] = artifact["executable"]
    report = []
    ids = set()
    for case in matrix["用例"]:
        identity = case["编号"]
        if not re.fullmatch(r"F\d{2}", identity) or identity in ids:
            raise RuntimeError("用例编号无效或重复")
        ids.add(identity)
        executable = executables[case["程序"]]
        listing = run([executable, "--list"], args.output / (identity + "-list.txt"), env, 30)
        if listing.splitlines().count(case["测试"] + ": test") != 1:
            raise RuntimeError(f"{identity} 没有唯一匹配实际测试")
        print(f"{identity} {case['阶段']}：开始", flush=True)
        output = run([executable, "--exact", case["测试"], "--nocapture"], args.output / (identity + ".txt"), env, 240)
        if not re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output):
            raise RuntimeError(f"{identity} 未实际通过一个测试，不能接受过滤为空或忽略")
        report.append({**case, "结果": "通过"})
        (args.output / "matrix.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"{identity}：通过", flush=True)
    histories = (args.output / "histories.txt").read_text().splitlines()
    if len(histories) != 32 or any(line.count("Event {") != 9 for line in histories):
        raise RuntimeError("完整历史缺少 32 组或每组 9 个事件")
    print(f"故障交叉矩阵通过：{len(report)} 个实际测试入口；另保存 32 组完整历史。", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(clean(f"矩阵验收失败：{error}"), flush=True)
        raise SystemExit(1)
