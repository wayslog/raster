"""核对第一期当前文档的本地链接、模块/API 清单、固定工具链和归档哈希。"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote

DOCUMENTS = [
    "README.md", "CONTEXT.md", "docs/README.md",
    "docs/05-RasterKV第一阶段与支持库选型.md", "docs/09-RasterKV模块设计.md",
    "docs/10-RasterKV公开接口.md", "docs/11-RasterKV内存与状态协议.md",
    "docs/13-模块基础骨架.md", "docs/14-RasterKV第一期实现计划地图.md",
    "docs/15-配置与诊断.md", "docs/16-公开接口使用流程.md",
    "docs/acceptance/功能对应表.md", "docs/acceptance/一期验收报告.md",
    "docs/acceptance/P9.1删除契约.md", "docs/acceptance/P9.2性能复核结论.md",
    "docs/acceptance/P9.2删除变更性能复核.md", "docs/acceptance/P9.3文档核对记录.md",
    "tools/acceptance/README.md",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    files = set(filter(None, subprocess.check_output(["git", "ls-files", "-z"], text=True).split("\0")))
    files.update(filter(None, subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], text=True).split("\0")))
    missing, private = [], []
    links = 0
    for name in DOCUMENTS:
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
        if re.search(rb"/Users/[^/\s]+/repo/rust/", raw):
            private.append(name)
    assert not missing, missing
    assert not private, private
    lib = Path("src/lib.rs").read_text()
    modules = re.findall(r"^(?:pub )?mod (\w+);", lib, re.M)
    assert len(modules) == 18 and len(re.findall(r"^pub mod ", lib, re.M)) == 6
    for module in modules:
        assert Path("src", module, "mod.rs").exists()
        assert module in Path("docs/13-模块基础骨架.md").read_text()
    capabilities = sorted(set(re.findall(r"\| (A\d\d) \|", Path("docs/acceptance/功能对应表.md").read_text())))
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
        manifest = json.loads((root / "manifest.json").read_text())
        manifest = manifest.get("文件SHA256", manifest)
        for name, digest in manifest.items():
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, (group, name)
        archives[group] = len(manifest)
    result = {"base": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "scope": "当前检出及待提交文档；运行实现 a834f48b53865098c8325d81cc52934bb322c26d",
              "documents": DOCUMENTS, "relative_links": links, "missing_links": missing,
              "private_path_hits": private, "checked_files": len(files), "modules": modules,
              "capabilities": capabilities, "rust_version": package["rust_version"], "edition": package["edition"],
              "dependencies": [{"name": dep["name"], "req": dep["req"], "optional": dep["optional"]} for dep in package["dependencies"]],
              "archive_hashes": archives, "rustdoc_inherent_methods": methods}
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"文档检查通过：{links} 个本地链接、18 个模块、A01—A28、{sum(map(len, methods.values()))} 个固有方法及归档哈希；私人路径零命中。")


if __name__ == "__main__":
    main()
