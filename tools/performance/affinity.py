"""Explicit CPU roles for frozen benchmark drivers; no host identifiers."""
import hashlib
import os
import re
from pathlib import Path

WORKERS = (4, 5, 6, 7)
COORDINATOR = 8
IO = 9


def pin_library():
    value = os.environ.get("RASTER_BENCH_PIN_LIBRARY")
    if not value:
        raise RuntimeError("RASTER_BENCH_PIN_LIBRARY must name the required binding library")
    path = Path(value).resolve(strict=True)
    return path


def manifest():
    path = pin_library()
    return {"library": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "worker_cpus": list(WORKERS), "coordinator_cpu": COORDINATOR, "io_cpu": IO}


def command(binary, args, engine, threads, io_threads=8):
    if not 0 <= threads <= len(WORKERS):
        raise ValueError("unsupported worker count")
    library = pin_library()
    cpus = ",".join(map(str, WORKERS[:threads] + (COORDINATOR, IO)))
    first = io_threads if engine == "rust" else 0
    # Preload only after numactl established the full allowed mask.
    return ["numactl", "--physcpubind=" + cpus, "--membind=0", "env",
            "LD_PRELOAD=" + str(library),
            "RASTER_BENCH_COORDINATOR_CPU=" + str(COORDINATOR),
            "RASTER_BENCH_IO_CPU=" + str(IO),
            "RASTER_BENCH_WORKER_FIRST=" + str(first),
            "RASTER_BENCH_WORKER_COUNT=" + str(threads),
            "RASTER_BENCH_WORKER_CPUS=" + ",".join(map(str, WORKERS[:threads])),
            "RASTER_BENCH_IO_THREADS=" + str(io_threads),
            str(binary), *map(str, args)]


def process_environment():
    env = os.environ.copy()
    # Never let an inherited preload run inside Python, env or numactl.
    env.pop("LD_PRELOAD", None)
    return env


def verify(stderr, threads):
    if "RASTER_AFFINITY error=" in stderr:
        raise RuntimeError("binding library reported an error")
    rows = re.findall(r"^RASTER_AFFINITY role=(\w+) index=(\d+) cpu=(\d+) tid=(\d+)$", stderr, re.M)
    main = [row for row in rows if row[0] == "coordinator"]
    workers = [row for row in rows if row[0] == "worker"]
    if len(main) != 1 or int(main[0][2]) != COORDINATOR:
        raise RuntimeError("missing or incorrect coordinator affinity")
    expected = {(str(i), str(cpu)) for i, cpu in enumerate(WORKERS[:threads])}
    if len(workers) != threads or {(r[1], r[2]) for r in workers} != expected:
        raise RuntimeError("missing or incorrect worker affinity")
    if len({r[3] for r in rows}) != len(rows):
        raise RuntimeError("one thread was assigned multiple roles")
    if any(int(r[2]) != IO for r in rows if r[0] == "auxiliary"):
        raise RuntimeError("auxiliary thread overlaps a workload CPU")
    complete = f"RASTER_AFFINITY complete workers={threads} expected={threads} failures=0"
    if stderr.splitlines().count(complete) != 1:
        raise RuntimeError("missing binding completion receipt")
    return [{"role": r[0], "index": int(r[1]), "cpu": int(r[2]), "tid": int(r[3])} for r in rows]
