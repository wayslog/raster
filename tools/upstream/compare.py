"""生成固定种子轨迹，分别运行真实 Rust/C++ 执行端并比较每一步输出。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

MASK = (1 << 64) - 1
SEEDS = [0, 1, 42, 0xABCDEF, MASK]


def generate(seed, count):
    state = seed

    def next_number():
        nonlocal state
        state = (state + 0x9E3779B97F4A7C15) & MASK
        value = state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
        return value ^ (value >> 31)

    serials = [0, 0, 0]
    lines = [f"raster-trace 1 {seed}"]
    for _ in range(count):
        session, key, choice, payload = next_number() % 3, next_number() % 8, next_number(), next_number()
        serials[session] += 1 + choice % 3
        key_text = bytes([key] * key).hex() or "-"
        value = f"u {payload}" if key % 2 == 0 else "b " + (payload.to_bytes(8, "little")[:payload % 9].hex() or "-")
        option = int(bool(choice & 4))
        operation = [f"read {option}", f"upsert {value}", f"rmw {option} {value}", f"delete {option}"][choice % 4]
        lines.append(f"{session} {serials[session]} {key_text} {operation}")
    return "\n".join(lines) + "\n"


def clean(text):
    return text.replace(str(Path.cwd()), "工作树").replace(str(Path.home()), "用户目录")


def run(command, log, env=None, timeout=180):
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=timeout)
    log.write_text(clean(result.stdout + result.stderr))
    if result.returncode:
        raise RuntimeError(f"执行失败，退出码 {result.returncode}，日志 {log}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", type=Path, required=True, help="已经构建的上游执行器")
    parser.add_argument("--corrected-cpp", type=Path, help="仅补上强制墓碑保护的参考副本；仍保存原始差异")
    parser.add_argument("--output", type=Path, required=True, help="本次全新结果目录")
    parser.add_argument("--disk", action="store_true", help="两端均使用原生文件后端")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("RASTER_UPSTREAM_"):
            env.pop(key)
    env["RUST_BACKTRACE"] = "0"
    env["CARGO_TERM_COLOR"] = "never"
    built = run(["cargo", "test", "--locked", "--all-features", "--test", "p9_upstream", "--no-run", "--message-format=json"], output / "rust-build.txt", env, 600)
    artifacts = [json.loads(line) for line in built.splitlines() if line.startswith("{")]
    executables = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact" and a.get("executable") and a["target"]["name"] == "p9_upstream"]
    if len(executables) != 1:
        raise RuntimeError("未找到唯一 Rust 轨迹测试程序")
    cases = [("固定边界", Path("tests/fixtures/p0.trace").read_text())]
    cases += [(f"随机种子-{seed}", generate(seed, 1500)) for seed in SEEDS]
    summaries = []
    for name, trace in cases:
        source = output / (name + ".trace")
        source.write_text(trace)
        env["RASTER_UPSTREAM_GENERATED"] = "0" if name == "固定边界" else "1"
        rust_result, cpp_result = output / (name + ".rust"), output / (name + ".cpp")
        env["RASTER_UPSTREAM_TRACE"] = str(source)
        env["RASTER_UPSTREAM_RESULT"] = str(rust_result)
        with tempfile.TemporaryDirectory(prefix="raster-random-") as root:
            command = [str(args.cpp.resolve()), str(source), str(cpp_result)]
            if args.disk:
                env["RASTER_UPSTREAM_ROOT"] = str(Path(root) / "rust")
                command += ["--disk", str(Path(root) / "cpp")]
            run([executables[0], "--exact", "同一拥有型轨迹输出真实引擎结果供上游对照", "--nocapture"], output / (name + ".rust.log"), env)
            run(command, output / (name + ".cpp.log"))
            if args.corrected_cpp:
                corrected_result = output / (name + ".corrected.cpp")
                command = [str(args.corrected_cpp.resolve()), str(source), str(corrected_result)]
                if args.disk:
                    command += ["--disk", str(Path(root) / "corrected")]
                run(command, output / (name + ".corrected.cpp.log"))
        rust_lines, cpp_lines = rust_result.read_text().splitlines(), cpp_result.read_text().splitlines()
        differences = [{"line": i + 1, "rust": a, "cpp": b} for i, (a, b) in enumerate(zip(rust_lines, cpp_lines)) if a != b]
        if len(rust_lines) != len(cpp_lines):
            differences.append({"error": "结果行数不同", "rust_lines": len(rust_lines), "cpp_lines": len(cpp_lines)})
        summary = {"case": name, "backend": "file" if args.disk else "null", "trace_sha256": hashlib.sha256(trace.encode()).hexdigest(), "steps": len(trace.splitlines()) - 1, "differences": differences}
        if args.corrected_cpp:
            corrected_lines = corrected_result.read_text().splitlines()
            from itertools import zip_longest
            corrected_differences = [{"line": i + 1, "rust": a, "corrected_cpp": b}
                                     for i, (a, b) in enumerate(zip_longest(rust_lines, corrected_lines)) if a != b]
            summary["corrected_differences"] = corrected_differences
            summary["contract"] = "force_tombstone 保留可达墓碑；其余原始算法不变"
        summaries.append(summary)
        (output / "comparison.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
        print(f"{name}：{summary['steps']} 步，{len(differences)} 处结果差异", flush=True)
    field = "corrected_differences" if args.corrected_cpp else "differences"
    if any(s[field] for s in summaries):
        raise RuntimeError("上游与 Rust 存在差异；已保存逐项结果，尚不能声明对照通过")
    print("约定契约逐操作通过，原始上游差异完整保留" if args.corrected_cpp else "上游与 Rust 的全部轨迹逐操作结果一致")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(clean(f"上游对照失败：{error}"), flush=True)
        raise SystemExit(1)
