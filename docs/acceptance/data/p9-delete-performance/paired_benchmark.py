"""在同一主机交替执行两个真实基准二进制，保留固定输入和每轮指标；不以跨主机噪声替代代码归因。"""
import argparse
import csv
import hashlib
import json
import itertools
import os
from pathlib import Path
import statistics
import subprocess

ORDERS = [("baseline", "candidate"), ("candidate", "baseline"), ("candidate", "baseline"), ("baseline", "candidate")]
DEFAULT_CASES = ["u64-hot-t4-disk-r1", "bytes-uniform-t1-disk-r1", "bytes-hot-t1-memory-r1"]


def clean(text):
    return text.replace(str(Path.cwd()), "工作树").replace(str(Path.home()), "用户目录")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", dest="cases")
    parser.add_argument("--interleaved-control", action="store_true", help="同轮交错运行新版本和两份相同旧二进制，以六种顺序平衡位置")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {name: getattr(args, name).resolve() for name in ["baseline", "candidate"]}
    if args.interleaved_control:
        binaries["control"] = binaries["baseline"]
    orders = list(itertools.permutations(binaries)) if args.interleaved_control else ORDERS
    metadata = {name: {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in binaries.items()}
    (args.output / "binaries.json").write_text(json.dumps(metadata, indent=2))
    summaries = []
    for case in args.cases or DEFAULT_CASES:
        pairs = []
        expected_hash = None
        for number, order in enumerate(orders, 1):
            pair = {"pair": number, "order": list(order)}
            for side in order:
                output = args.output / f"{case}-pair{number}-{side}"
                env = {**os.environ, "RASTER_BENCH_CASE": case, "RASTER_BENCH_OUTPUT": str(output.resolve()), "CARGO_TERM_COLOR": "never", "RUST_BACKTRACE": "0"}
                result = subprocess.run([str(binaries[side])], env=env, capture_output=True, text=True, timeout=180)
                combined = result.stdout + result.stderr
                (args.output / f"{case}-pair{number}-{side}.txt").write_text(clean(combined))
                if result.returncode:
                    raise RuntimeError(f"{case} 第 {number} 对 {side} 业务失败：{result.returncode}")
                rows = list(csv.DictReader((output / "matrix.csv").open()))
                if len(rows) != 1 or int(rows[0]["操作数"]) != 16384:
                    raise RuntimeError("基准没有完整执行指定场景")
                marker = f"基准 {case} 通过：16384 步四操作、"
                if marker not in combined or "2048 个键与墓碑校验" not in combined:
                    raise RuntimeError("没有完成业务及恢复校验")
                row = rows[0]
                digest = hashlib.sha256((output / (row["输入标识"] + ".trace")).read_bytes()).hexdigest()
                if expected_hash is not None and digest != expected_hash:
                    raise RuntimeError("配对输入字节不一致")
                expected_hash = digest
                pair[side] = row
                print(case, number, side, row["每秒操作"], row["P99纳秒"], flush=True)
            pairs.append(pair)
            (args.output / (case + "-pairs.json")).write_text(json.dumps(pairs, ensure_ascii=False, indent=2))
        metrics = ["每秒操作", "P99纳秒", "读取P99纳秒", "写入P99纳秒", "RMWP99纳秒", "删除P99纳秒", "业务后累计读字节", "业务后累计写字节"]
        medians = {side: {metric: statistics.median(float(pair[side][metric]) for pair in pairs) for metric in metrics} for side in binaries}
        throughput = medians["candidate"]["每秒操作"] / medians["baseline"]["每秒操作"]
        latency = medians["candidate"]["P99纳秒"] / medians["baseline"]["P99纳秒"]
        summaries.append({"case": case, "input_sha256": expected_hash, "medians": medians,
                          "throughput_ratio": throughput, "p99_ratio": latency,
                          "review_required": throughput < .75 or latency > 1.30})
        if args.interleaved_control:
            control_throughput = medians["control"]["每秒操作"] / medians["baseline"]["每秒操作"]
            control_latency = medians["control"]["P99纳秒"] / medians["baseline"]["P99纳秒"]
            summaries[-1].update(control_throughput_ratio=control_throughput, control_p99_ratio=control_latency,
                                 control_review_required=control_throughput < .75 or control_latency > 1.30)
        (args.output / "comparison.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    if any(row["review_required"] or row.get("control_review_required", False) for row in summaries):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
