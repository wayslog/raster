"""拥有型生命周期轨迹；提前固定规模，输出同一份输入供两种真实引擎执行。"""
from pathlib import Path


class Writer:
    def __init__(self, path, seed):
        self.file = Path(path).open("w")
        self.file.write(f"raster-trace 1 {seed}\n")
        self.serials = [0, 0, 0]
        self.steps = 0
        self.payload_bytes = 0

    def emit(self, key, operation, session=None):
        session = self.steps % 3 if session is None else session
        serial = self.serials[session]
        self.serials[session] += 3
        self.file.write(f"{session} {serial} {key.hex() or '-'} {operation}\n")
        self.steps += 1

    def put(self, key, value, session=None):
        if isinstance(value, int):
            text = f"u {value}"
            self.payload_bytes += 8
        else:
            text = "b " + (value.hex() or "-")
            self.payload_bytes += len(value)
        self.emit(key, "upsert " + text, session)

    def close(self, split):
        self.file.close()
        return {"steps": self.steps, "split": split, "upsert_payload_bytes": self.payload_bytes}


def small(path):
    writer = Writer(path, 9001)
    keys = [b"" if i == 0 else bytes([255, i, 0]) for i in range(32)]
    for i, key in enumerate(keys):
        writer.put(key, ((1 << 64) - 1 - i) if i % 2 == 0 else bytes([i]) * (1024 + i))
    for i, key in enumerate(keys):
        writer.emit(key, "rmw 1 " + ("u 33" if i % 2 == 0 else "b 0001ff"))
    writer.emit(keys[1], "delete 0")
    writer.emit(keys[1], "read 1")
    writer.emit(keys[2], "delete 1")
    writer.emit(keys[2], "read 0")
    writer.emit(b"never", "rmw 0 u 7")
    writer.emit(b"new-tombstone", "delete 1")
    writer.emit(b"new-tombstone", "read 1")
    split = writer.steps
    for key in keys:
        writer.emit(key, "read 0")
    writer.emit(b"new-tombstone", "read 1")
    writer.emit(keys[1], "rmw 0 b ff")
    writer.emit(keys[1], "rmw 1 b ff")
    writer.emit(keys[2], "rmw 1 u 9")
    writer.put(keys[3], b"")
    writer.emit(keys[3], "rmw 1 b 1122334455")
    for key in keys:
        writer.emit(key, "read 0")
    return writer.close(split)


def large(path):
    writer = Writer(path, 9002)
    cold = [b"cold" + i.to_bytes(4, "little") for i in range(12)]
    for i, key in enumerate(cold):
        writer.put(key, bytes([i]) * (16384 + i))
    # 32768 次完整追加的负载本身已有 512 MiB；记录头和定向 RMW 继续扩大跨度。
    # 上游日志内存固定 256 MiB，不能用很小的 Rust 页窗口冒充上游磁盘分支。
    for i in range(32768):
        key = b"hot" + (i % 256).to_bytes(4, "little")
        writer.put(key, bytes([i % 251]) * (16384 + i % 3 * 8))
        if i % 128 == 0:
            writer.emit(key, "rmw 1 b 01020304")
        if i >= 256 and i % 256 == 0:
            writer.emit(key, "delete 0")
            writer.emit(key, "read 1")
            writer.put(key, bytes([i % 251]) * 16416)
    writer.emit(cold[0], "read 0")
    writer.emit(cold[1], "rmw 1 b 010203")
    split = writer.steps
    for key in cold:
        writer.emit(key, "read 0")
    writer.emit(cold[2], "delete 0")
    writer.emit(cold[2], "read 1")
    writer.put(cold[3], b"after-recovery")
    writer.emit(cold[4], "rmw 0 b ff")
    writer.emit(b"absent", "rmw 0 b ff")
    writer.emit(b"forced", "delete 1")
    writer.emit(b"forced", "read 1")
    for i in range(256):
        writer.emit(b"hot" + i.to_bytes(4, "little"), "read 0")
    return writer.close(split)
