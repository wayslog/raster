"""Exercise real process selection, argv[0], and retained launch failures."""
from pathlib import Path
import tempfile
import unittest

from launch_control import invoke


class LaunchControlTests(unittest.TestCase):
    def test_argv0_changes_without_selecting_a_different_executable(self):
        binary = Path("/bin/sh").resolve()
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "launch"
            arguments = ["-c", 'printf "%s\\n" "$0"']
            self.assertEqual(invoke(binary, arguments, None, prefix).strip(), str(binary))
            self.assertEqual(invoke(binary, arguments, "parity-test-label", prefix).strip(), "parity-test-label")

    def test_nonzero_exit_retains_stderr_and_rejects_the_run(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "failed"
            with self.assertRaises(RuntimeError):
                invoke(Path("/bin/sh").resolve(), ["-c", "echo injected-launch-failure >&2; exit 7"], "parity", prefix)
            self.assertIn("injected-launch-failure", prefix.with_suffix(".stderr").read_text())
            self.assertIn('"exit_code": 7', prefix.with_suffix(".launch.json").read_text())


if __name__ == "__main__":
    unittest.main()
