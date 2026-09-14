import json
import unittest

from compare_result_budget import validate_output


class ResultBudgetValidationTests(unittest.TestCase):
    def output(self):
        return {
            "engine": "rust", "case": "retained-results/read", "retained": 960,
            "count": 16_384, "samples": 256, "checksum": 16_384 * 42,
            "retained_checksum": 960 * 42, "last_accepted": 960 + 1024 + 16_384,
            "elapsed_ns": 1_000_000, "p99_ns": 100,
        }

    def test_validates_both_timed_and_retained_results(self):
        row = self.output()
        self.assertEqual(validate_output(json.dumps(row), 960, 16_384), row)
        for field in ("count", "retained", "samples", "checksum", "retained_checksum",
                      "last_accepted", "elapsed_ns", "p99_ns"):
            with self.subTest(field=field):
                broken = {**row, field: 0}
                with self.assertRaises(ValueError):
                    validate_output(json.dumps(broken), 960, 16_384)

    def test_rejects_missing_fields_wrong_workloads_and_invalid_types(self):
        row = self.output()
        for field in row:
            with self.subTest(missing=field):
                broken = row.copy()
                del broken[field]
                with self.assertRaises(ValueError):
                    validate_output(json.dumps(broken), 960, 16_384)
        for field, value in [("engine", "cpp"), ("case", "read"), ("count", 16_384.0),
                             ("elapsed_ns", True), ("p99_ns", -1)]:
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    validate_output(json.dumps({**row, field: value}), 960, 16_384)


if __name__ == "__main__":
    unittest.main()
