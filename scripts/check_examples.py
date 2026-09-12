"""Verify directory retention with compiled public examples,Rejection of coverage and error exit;Only operate this temporary directory."""
import hashlib
from pathlib import Path
import subprocess
import tempfile


def invoke(binary, *arguments, success):
    result = subprocess.run(
        [str(binary), *map(str, arguments)], capture_output=True, text=True, timeout=180
    )
    if (result.returncode == 0) != success:
        raise AssertionError(f"Example exit code not as expected:{result.returncode}\n{result.stdout}\n{result.stderr}")
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
        raise AssertionError("Please start with release Schema building disk_lifecycle Example")
    with tempfile.TemporaryDirectory(prefix="raster-example-check-") as directory:
        base = Path(directory)
        sentinel = base / "Reserve.txt"
        sentinel.write_bytes(b"raster-existing-directory")
        before = content(base)
        invoke(binary, base, success=False)
        if content(base) != before:
            raise AssertionError("The contents of an existing directory have been modified")
        invalid = base / "Parameter error does not create"
        invoke(binary, invalid, "redundant parameters", success=False)
        if invalid.exists():
            raise AssertionError("Parameter error creates storage directory in advance")
        store = base / "Explicit new storage"
        completed = invoke(binary, store, success=True)
        if "Disk life cycle passed" not in completed.stdout or not store.is_dir():
            raise AssertionError("The explicit directory process is not fully executed or the directory is automatically deleted")
        retained = content(store)
        if not retained:
            raise AssertionError("No storage material retained after completion")
        invoke(binary, store, success=False)
        if content(store) != retained:
            raise AssertionError("Restarting overwrites existing checkpoints or data")
    print("Example directory protection passed: existing directories are rejected, parameter errors create nothing, explicit directories are retained, and the second run leaves content unchanged.")


if __name__ == "__main__":
    main()
