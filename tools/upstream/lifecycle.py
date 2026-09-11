"""在原生 Linux 严格比较三种检查点与超内存生命周期；任何返回差异均失败。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from scenarios import small, large


def clean(text):
    return text.replace(str(Path.cwd()), "工作树").replace(str(Path.home()), "用户目录")


def run(command, log, env=None):
    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=1200)
    except subprocess.TimeoutExpired as error:
        def decoded(value):
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
        log.write_text(clean(decoded(error.stdout) + decoded(error.stderr)))
        raise RuntimeError(f"执行超过预设 1200 秒，见 {log.name}") from error
    text = result.stdout + result.stderr
    log.write_text(clean(text))
    if result.returncode:
        raise RuntimeError(f"执行失败，退出码 {result.returncode}，见 {log.name}")
    return result.stdout


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def compare(left, right, differences):
    mismatches = []
    from itertools import zip_longest
    with left.open() as a, right.open() as b:
        for number, (rust, cpp) in enumerate(zip_longest(a, b), 1):
            if rust != cpp:
                mismatches.append({"line": number, "rust": rust, "cpp": cpp})
    differences.write_text(json.dumps(mismatches, ensure_ascii=False, indent=2))
    return len(mismatches)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=["all", "full", "pair", "large"], default="all")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("RASTER_UPSTREAM_"):
            env.pop(key)
    env.update(CARGO_TERM_COLOR="never", RUST_BACKTRACE="0", RASTER_UPSTREAM_TIMEOUT="600")
    # 大输入的协议解码与业务均使用发布模式；门槛在执行前固定。
    built = run(["cargo", "test", "--locked", "--all-features", "--release", "--test", "p9_upstream", "--no-run", "--message-format=json"], output / "rust-build.txt", env)
    artifacts = [json.loads(line) for line in built.splitlines() if line.startswith("{")]
    executables = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact" and a.get("executable") and a["target"]["name"] == "p9_upstream"]
    if len(executables) != 1:
        raise RuntimeError("Rust 执行器不唯一")
    summaries = []
    for name in ["full", "pair", "large"]:
        if args.case not in ("all", name):
            continue
        source = output / (name + ".trace")
        print(f"生成 {name} 轨迹", flush=True)
        details = (large if name == "large" else small)(source)
        details.update(case=name, trace_sha256=digest(source))
        (output / (name + "-input.json")).write_text(json.dumps(details, ensure_ascii=False, indent=2))
        kind = "pair" if name == "pair" else "full"
        rust_result, cpp_result = output / (name + ".rust.results"), output / (name + ".cpp.results")
        root = Path(tempfile.mkdtemp(prefix="raster-lifecycle-"))
        try:
            # 大轨迹保留同样的页窗口和业务输入；32 MiB 段避免 128 个句柄预算耗尽。
            env["RASTER_UPSTREAM_SEGMENT_BYTES"] = str(33554432 if name == "large" else 1048576)
            env["RASTER_UPSTREAM_TRACE"] = str(source)
            env["RASTER_UPSTREAM_RESULT"] = str(rust_result)
            env["RASTER_UPSTREAM_ROOT"] = str(root / "rust")
            env["RASTER_UPSTREAM_SPLIT"] = str(details["split"])
            env["RASTER_UPSTREAM_CHECKPOINT"] = kind
            print(f"{name}：Rust 执行 {details['steps']} 步并恢复", flush=True)
            rust_log = run([executables[0], "--exact", "同一拥有型轨迹输出真实引擎结果供上游对照", "--nocapture"], output / (name + ".rust.log"), env)
            print(f"{name}：C++ 执行相同输入并恢复", flush=True)
            cpp_log = run([str(args.cpp.resolve()), str(source), str(cpp_result), "--disk", str(root / "cpp"), "--split", str(details["split"]), "--checkpoint", kind], output / (name + ".cpp.log"))
        except Exception:
            print(f"失败材料保留在本次临时目录 {root.name}", flush=True)
            raise
        mismatches = compare(rust_result, cpp_result, output / (name + "-differences.json"))
        label = "Index+Log" if kind == "pair" else "Full"
        if f"Rust 轨迹边界恢复通过：模式 {label}，3 个会话续接" not in rust_log or f"上游轨迹边界恢复通过：模式 {label}，3 个会话续接" not in cpp_log:
            raise RuntimeError(f"{name} 缺少实际恢复和三个会话续接证据")
        pending = re.search(r"上游四操作 Pending：\[(\d+),(\d+),(\d+),(\d+)\]", cpp_log)
        span = re.search(r"上游日志边界：\[\d+,\d+\)，跨度 (\d+) 字节", cpp_log)
        if not pending or not span:
            raise RuntimeError("缺少真实 Pending 或跨度输出")
        details.update(cpp_pending=list(map(int, pending.groups())), cpp_log_span_bytes=int(span.group(1)), differences=mismatches)
        if name == "large" and (details["cpp_log_span_bytes"] <= 2 * 256 * 1024 * 1024 or not details["cpp_pending"][0] or not details["cpp_pending"][2]):
            raise RuntimeError("大轨迹没有达到预设跨度或实际冷读/RMW Pending 门槛")
        summaries.append(details)
        (output / "lifecycle.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
        if mismatches:
            raise RuntimeError(f"{name} 有 {mismatches} 处结果差异，未通过生命周期对照")
        shutil.rmtree(root)
        print(f"{name}：逐操作结果、检查点恢复及会话进度通过", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(clean(f"生命周期对照失败：{error}"), flush=True)
        raise SystemExit(1)
