"""Predeclared paired bootstrap for checkpoint controls and noninferiority."""
import math
import random
import statistics

SEED = 20261014
RESAMPLES = 20000
METRICS = ("mops", "checkpoint_ns", "mops_during", "recover_ns")
LOWER = 0.98
UPPER = 1 / LOWER


def interval(pairs, metric, reference, candidate):
    groups = {}
    for pair in pairs:
        left = pair["rows"][reference][metric]
        right = pair["rows"][candidate][metric]
        if min(left, right) <= 0:
            raise ValueError("checkpoint measurements must be positive")
        # Express every metric as speed: a smaller duration is an improvement.
        ratio = right / left if metric.startswith("mops") else left / right
        groups.setdefault(pair["order"], []).append(math.log(ratio))
    if not groups:
        raise ValueError("no measurements")
    values = [value for group in groups.values() for value in group]
    rng = random.Random(SEED)
    samples = []
    for _ in range(RESAMPLES):
        sample = [value for group in groups.values() for value in rng.choices(group, k=len(group))]
        samples.append(statistics.median(sample))
    samples.sort()

    def quantile(p):
        position = p * (len(samples) - 1)
        at = int(position)
        return math.exp(samples[at] + (samples[min(at + 1, len(samples) - 1)] - samples[at]) * (position - at))

    low, high = quantile(0.025), quantile(0.975)
    return {"speed_ratio": math.exp(statistics.median(values)), "low": low, "high": high,
            "width": high - low, "pairs": len(pairs)}


def evaluate(pairs, reference, candidate, control=False, minimum=LOWER):
    metrics = {metric: interval(pairs, metric, reference, candidate) for metric in METRICS}
    if control:
        accepted = all(row["low"] >= LOWER and row["high"] <= UPPER and row["width"] < 0.02 for row in metrics.values())
    else:
        accepted = all(row["low"] >= minimum for row in metrics.values())
    return {"reference": reference, "candidate": candidate, "metrics": metrics, "accepted": accepted}
