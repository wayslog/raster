"""Only add mandatory tombstone protection conditions in the build copy;Save full difference and hash,Do not change fixed upstream checkout."""
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
        raise RuntimeError("Upstream deletion optimization conditions are not unique,Refuse to generate reference fixes")
    corrected = original.replace(old, new)
    header.write_text(corrected)
    patch = "".join(difflib.unified_diff(
        original.splitlines(True), corrected.splitlines(True),
        fromfile="original upstream/cc/src/core/faster.h", tofile="Forced tombstone fix/cc/src/core/faster.h"))
    (destination.parent / "forced-tombstone.diff").write_text(patch)
    (destination.parent / "forced-tombstone.json").write_text(json.dumps({
        "contract": "force_tombstone Disable index elimination",
        "changed_files": ["cc/src/core/faster.h"],
        "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "corrected_sha256": hashlib.sha256(corrected.encode()).hexdigest(),
        "patch_sha256": hashlib.sha256(patch.encode()).hexdigest(),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    prepare(*sys.argv[1:])
