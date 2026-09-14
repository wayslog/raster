"""Reject incomplete or corrupt direct-engine output before accepting a profile."""
import json
import unittest

from compare import expected
from cpu_profile import validate_output


class ProfileOutputTests(unittest.TestCase):
    def setUp(self):
        self.row = expected("rmw", "shared-hot", 4, 16_384) | {
            "engine": "rust", "operation": "rmw", "distribution": "shared-hot",
            "threads": 4, "count": 16_384, "elapsed_ns": 10, "p99_ns": 2, "samples": 256,
        }

    def validate(self, output):
        return validate_output(output, "rust", "rmw/shared-hot/4", 16_384)

    def output(self):
        return json.dumps({"engine": self.row["engine"], **self.row}, separators=(",", ":"))

    def test_complete_result_matches_model(self):
        self.assertEqual(self.validate("diagnostic output\n" + self.output()), self.row)

    def test_model_and_workload_mismatches_are_rejected(self):
        for key in ("checksum", "digest", "verified_keys", "count", "threads"):
            with self.subTest(key=key):
                self.row[key] += 1
                with self.assertRaises(ValueError):
                    self.validate(self.output())
                self.row[key] -= 1
        self.row["engine"] = "cpp"
        with self.assertRaises(ValueError):
            self.validate(self.output())

    def test_missing_duplicate_and_incomplete_results_are_rejected(self):
        for output in ("", self.output() + "\n" + self.output()):
            with self.assertRaises(ValueError):
                self.validate(output)
        self.row["samples"] = 0
        with self.assertRaises(ValueError):
            self.validate(self.output())


if __name__ == "__main__":
    unittest.main()
