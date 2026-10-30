"""Statistical guardrails for the checkpoint acceptance procedure."""
import unittest
import checkpoint_stats as stats


def pairs(speedups):
    rows = []
    for i, speed in enumerate(speedups):
        reference = {"mops": 10.0, "mops_during": 8.0, "checkpoint_ns": 500000000, "recover_ns": 200000000}
        candidate = {k: value * speed if k.startswith("mops") else value / speed for k, value in reference.items()}
        rows.append({"order": "a/b" if i % 2 == 0 else "b/a", "rows": {"a": reference, "b": candidate}})
    return rows


class CheckpointStatisticsTests(unittest.TestCase):
    def test_control_requires_precision_not_just_a_similar_point_estimate(self):
        result = stats.evaluate(pairs([0.90, 0.90, 1.10, 1.10] * 16), "a", "b", control=True)
        self.assertFalse(result["accepted"])
        self.assertGreater(result["metrics"]["checkpoint_ns"]["width"], 0.02)

    def test_duration_and_throughput_have_the_same_speed_direction(self):
        result = stats.evaluate(pairs([1.25] * 36), "a", "b")
        self.assertTrue(result["accepted"])
        for metric in stats.METRICS:
            self.assertAlmostEqual(result["metrics"][metric]["speed_ratio"], 1.25)

    def test_precise_regression_is_rejected(self):
        self.assertFalse(stats.evaluate(pairs([0.97] * 36), "a", "b")["accepted"])
        self.assertTrue(stats.evaluate(pairs([1.0] * 64), "a", "b", control=True)["accepted"])


if __name__ == "__main__":
    unittest.main()
