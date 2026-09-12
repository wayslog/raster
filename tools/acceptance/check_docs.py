"""Check local links, module/API coverage, the fixed toolchain, and archive hashes."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote

DOCUMENTS = ["README.md", "CONTEXT.md", "docs/README.md", "tools/acceptance/README.md"]


def document_paths():
    """Discover document files without embedding localized filenames."""
    return DOCUMENTS[:3] + sorted(str(path) for path in Path("docs").rglob("*.md")) + [DOCUMENTS[3]]


def archive_hashes(manifest):
    """Return the hash table while accepting historical metadata key names."""
    if isinstance(manifest.get("fileSHA256"), dict):
        return manifest["fileSHA256"]
    candidates = [value for value in manifest.values() if isinstance(value, dict)]
    for candidate in candidates:
        if candidate and all(isinstance(key, str) and isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                             for key, value in candidate.items()):
            return candidate
    if all(isinstance(key, str) and isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
           for key, value in manifest.items()):
        return manifest
    raise AssertionError("archive manifest does not contain a SHA-256 table")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    files = set(filter(None, subprocess.check_output(["git", "ls-files", "-z"], text=True).split("\0")))
    files.update(filter(None, subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], text=True).split("\0")))
    missing, private = [], []
    links = 0
    documents = document_paths()
    for name in documents:
        p = Path(name)
        for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", p.read_text()):
            target = target.strip().strip("<>")
            if "://" in target or target.startswith("#") or target.startswith("mailto:"):
                continue
            path = unquote(target.split("#")[0])
            if not path:
                continue
            links += 1
            if not (p.parent / path).exists():
                missing.append([name, target])
    for name in sorted(files):
        p = Path(name)
        if not p.is_file():
            continue
        raw = p.read_bytes()
        if p.suffix == ".gz":
            raw = gzip.decompress(raw)
        if re.search(rb"/Users/[^/\s]+/repo/[^/\s]+/", raw):
            private.append(name)
    assert not missing, missing
    assert not private, private
    lib = Path("src/lib.rs").read_text()
    modules = re.findall(r"^(?:pub )?mod (\w+);", lib, re.M)
    assert len(modules) == 18 and len(re.findall(r"^pub mod ", lib, re.M)) == 6
    for module in modules:
        assert Path("src", module, "mod.rs").exists()
        assert any(module in path.read_text() for path in Path("docs").glob("*.md"))
    capability_document = next(path for path in Path("docs/acceptance").glob("*.md") if "| A01 |" in path.read_text())
    capabilities = sorted(set(re.findall(r"\| (A\d\d) \|", capability_document.read_text())))
    assert capabilities == [f"A{i:02}" for i in range(1, 29)]
    metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], text=True))
    package = metadata["packages"][0]
    assert package["rust_version"] == "1.98.1" and package["edition"] == "2024"
    assert 'channel = "1.98.1"' in Path("rust-toolchain.toml").read_text()
    methods = {}
    for name in ["RasterKV", "Builder", "Session", "Ticket", "Maintenance", "MaintenanceTicket", "RecordScanner"]:
        paths = list(Path("target/doc/raster/api").glob(f"*/struct.{name}.html"))
        assert len(paths) == 1
        html = re.split(r'id="(?:trait|auto-trait|blanket)-implementations"', paths[0].read_text())[0]
        methods[name] = sorted(set(re.findall(r'id="method\.([^".]+)"', html)))
        assert methods[name]
    archives = {}
    for group in ["p9-delete", "p9-delete-performance", "p9-performance-review"]:
        root = Path("docs/acceptance/data", group)
        manifest = archive_hashes(json.loads((root / "manifest.json").read_text()))
        for name, digest in manifest.items():
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, (group, name)
        archives[group] = len(manifest)
    result = {"base": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "scope": "Current checkout and pending documents",
              "documents": documents, "relative_links": links, "missing_links": missing,
              "private_path_hits": private, "checked_files": len(files), "modules": modules,
              "capabilities": capabilities, "rust_version": package["rust_version"], "edition": package["edition"],
              "dependencies": [{"name": dep["name"], "req": dep["req"], "optional": dep["optional"]} for dep in package["dependencies"]],
              "archive_hashes": archives, "rustdoc_inherent_methods": methods}
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"Document check passed: {links} local links, 18 modules, A01-A28, "
          f"{sum(map(len, methods.values()))} inherent methods, and archive hashes; private path hits: 0.")


if __name__ == "__main__":
    main()
