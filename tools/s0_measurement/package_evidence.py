"""Write a manifest for one original runner artifact without rewriting evidence."""
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
files = {}
for path in sorted(root.rglob("*")):
    if not path.is_file() or path == root / "manifest.json":
        continue
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    files[str(path.relative_to(root))] = {"sha256": value.hexdigest(), "bytes": path.stat().st_size}
(root / "manifest.json").write_text(json.dumps({"files": files}, indent=2) + "\n")
print(json.dumps({"artifact_files": len(files)}))
