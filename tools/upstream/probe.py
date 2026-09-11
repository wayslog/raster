"""原生 Linux 上游执行环境基线；明确保留已观察差异，不代表跨实现验收。"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def run(command, log, env=None):
    result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=180)
    content = result.stdout + result.stderr
    content = content.replace(str(Path.cwd()), "工作树").replace(str(Path.home()), "用户目录")
    log.write_text(content)
    if result.returncode:
        raise RuntimeError(f"环境探测退出码 {result.returncode}，见 {log.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    source = Path("tests/fixtures/p0.trace").resolve()
    expected = Path("tools/upstream/fixtures/p0.cpp.results").read_text()
    with tempfile.TemporaryDirectory(prefix="raster-upstream-probe-") as root:
        for mode in ("memory", "file"):
            result = output / (mode + ".cpp.results")
            command = [str(args.cpp.resolve()), str(source), str(result)]
            if mode == "file":
                command += ["--disk", str(Path(root) / "cpp-store")]
            run(command, output / (mode + ".cpp.log"))
            if result.read_text() != expected:
                raise RuntimeError(f"上游 {mode} 原始返回与固定观察不符，请重新调查")
        env = os.environ.copy()
        env["RASTER_UPSTREAM_TRACE"] = str(source)
        env["RASTER_UPSTREAM_RESULT"] = str(output / "fixed.rust.results")
        env["RASTER_UPSTREAM_ROOT"] = str(Path(root) / "rust-store")
        env["RASTER_UPSTREAM_GENERATED"] = "0"
        env.pop("RASTER_UPSTREAM_SPLIT", None)
        env.pop("RASTER_UPSTREAM_CHECKPOINT", None)
        run(["cargo", "test", "--locked", "--all-features", "--test", "p9_upstream", "--", "--nocapture"], output / "rust.log", env)
    rust = (output / "fixed.rust.results").read_text().splitlines()
    cpp = expected.splitlines()
    differences = [{"line": index + 1, "rust": a, "cpp": b} for index, (a, b) in enumerate(zip(rust, cpp)) if a != b]
    if len(rust) != len(cpp) or differences != [
        {"line": 6, "rust": "0 6 tombstone", "cpp": "0 6 missing"},
        {"line": 8, "rust": "0 8 missing", "cpp": "0 8 deleted"},
    ]:
        raise RuntimeError("跨实现差异与已记录的固定边界不符，需要调查")
    summary = {"environment_probe": "passed", "compatibility_acceptance": "not_completed", "differences": differences}
    (output / "probe.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print("上游执行环境基线通过：Null 与 Linux libaio 返回均符合固定观察。")
    print("兼容性验收未完成：两处 Rust/C++ 删除返回差异完整保留，尚待契约决定。")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"上游环境探测失败：{error}")
        raise SystemExit(1)
