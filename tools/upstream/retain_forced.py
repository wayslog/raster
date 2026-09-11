"""在构建副本中仅补上强制墓碑保护条件；保存完整差异及哈希，不改固定上游检出。"""
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import sys


def prepare(source, destination):
    source, destination = Path(source), Path(destination)
    shutil.copytree(source / "cc/src", destination)
    header = destination / "core/faster.h"
    original = header.read_text()
    old = "if(hash_index_.IsSync() && expected_entry.address() == address) {"
    new = "if(!force_tombstone && hash_index_.IsSync() && expected_entry.address() == address) {"
    if original.count(old) != 1:
        raise RuntimeError("上游删除优化条件不唯一，拒绝生成参考修正")
    corrected = original.replace(old, new)
    header.write_text(corrected)
    patch = "".join(difflib.unified_diff(
        original.splitlines(True), corrected.splitlines(True),
        fromfile="原始上游/cc/src/core/faster.h", tofile="强制墓碑修正/cc/src/core/faster.h"))
    (destination.parent / "forced-tombstone.diff").write_text(patch)
    (destination.parent / "forced-tombstone.json").write_text(json.dumps({
        "contract": "force_tombstone 禁止索引消除",
        "changed_files": ["cc/src/core/faster.h"],
        "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "corrected_sha256": hashlib.sha256(corrected.encode()).hexdigest(),
        "patch_sha256": hashlib.sha256(patch.encode()).hexdigest(),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    prepare(*sys.argv[1:])
