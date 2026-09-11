"""通过已编译的公开示例验证目录保留、拒绝覆盖与错误退出；只操作本次临时目录。"""
import hashlib
from pathlib import Path
import subprocess
import tempfile


def invoke(binary, *arguments, success):
    result = subprocess.run(
        [str(binary), *map(str, arguments)], capture_output=True, text=True, timeout=180
    )
    if (result.returncode == 0) != success:
        raise AssertionError(f"示例退出码不符合预期：{result.returncode}\n{result.stdout}\n{result.stderr}")
    return result


def content(root):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def main():
    binary = Path("target/release/examples/disk_lifecycle").resolve()
    if not binary.is_file():
        raise AssertionError("请先以 release 模式构建 disk_lifecycle 示例")
    with tempfile.TemporaryDirectory(prefix="raster-example-check-") as directory:
        base = Path(directory)
        sentinel = base / "保留.txt"
        sentinel.write_bytes(b"raster-existing-directory")
        before = content(base)
        invoke(binary, base, success=False)
        if content(base) != before:
            raise AssertionError("已有目录的内容被修改")
        invalid = base / "参数错误不创建"
        invoke(binary, invalid, "多余参数", success=False)
        if invalid.exists():
            raise AssertionError("参数错误提前创建了存储目录")
        store = base / "显式新存储"
        completed = invoke(binary, store, success=True)
        if "磁盘生命周期通过" not in completed.stdout or not store.is_dir():
            raise AssertionError("显式目录流程没有完整执行或目录被自动删除")
        retained = content(store)
        if not retained:
            raise AssertionError("完成后没有保留存储材料")
        invoke(binary, store, success=False)
        if content(store) != retained:
            raise AssertionError("再次启动覆盖了已有检查点或数据")
    print("示例目录保护通过：已有目录拒绝、参数错误无创建、显式目录保留、二次运行内容不变。")


if __name__ == "__main__":
    main()
