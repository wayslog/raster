"""Protect focused diagnostics against incomplete evidence and invalid controls."""
import copy
import json
import unittest

from compare import expected
from focused_compare import ORDERS, summarize, validate_case, validate_measurement


class FocusedComparisonTests(unittest.TestCase):
    def output(self):
        return {"engine": "rust", "operation": "upsert", "distribution": "shared-hot",
                "threads": 1, "count": 16_384, "elapsed_ns": 1000, "p99_ns": 100,
                "samples": 256, **expected("upsert", "shared-hot", 1, 16_384)}

    def entries(self):
        return [{"round": index, "name": name, "result": self.output()}
                for index, order in enumerate(ORDERS) for name in order]

    def test_wrong_business_results_and_missing_latency_samples_are_rejected(self):
        valid = self.output()
        self.assertEqual(validate_measurement(json.dumps(valid), "upsert/shared-hot/1", 16_384), valid)
        for field in ("checksum", "digest", "verified_keys", "count", "samples"):
            damaged = valid | {field: valid[field] + 1}
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_measurement(json.dumps(damaged), "upsert/shared-hot/1", 16_384)

    def test_each_round_and_control_position_must_be_present_exactly_once(self):
        entries = self.entries()
        self.assertTrue(summarize(entries)["passed"])
        for damaged in (entries[:-1], entries + entries[-1:], entries[1:] + entries[:1]):
            with self.assertRaises(ValueError):
                summarize(damaged)

    def test_a_failing_control_cannot_be_hidden_by_a_faster_candidate(self):
        for field, value in (("elapsed_ns", 1100), ("elapsed_ns", 800), ("p99_ns", 80)):
            entries = self.entries()
            for entry in entries:
                if entry["name"] == "candidate":
                    entry["result"]["elapsed_ns"] = 500
                elif entry["name"] == "control":
                    entry["result"][field] = value
            result = summarize(entries)
            self.assertTrue(result["comparisons"]["candidate"]["passed"])
            with self.subTest(field=field, value=value):
                self.assertFalse(result["control_valid"])
                self.assertFalse(result["passed"])

    def test_candidate_throughput_and_latency_keep_the_existing_thresholds(self):
        for field, value in (("elapsed_ns", 1021), ("p99_ns", 111)):
            entries = copy.deepcopy(self.entries())
            for entry in entries:
                if entry["name"] == "candidate":
                    entry["result"][field] = value
            result = summarize(entries)
            self.assertTrue(result["control_valid"])
            self.assertFalse(result["comparisons"]["candidate"]["passed"])

    def test_a_multi_thread_case_cannot_be_silently_pinned_to_one_cpu(self):
        for case in ("upsert/shared-hot/4", "delete/uniform/1", "upsert/unknown/1", ""):
            with self.assertRaises(ValueError):
                validate_case(case)


if __name__ == "__main__":
    unittest.main()
