"""Observed sample statistics. Failed observations are never completion times."""

import math
import hashlib
from collections import Counter


def summarize(attempts):
    counts = Counter(a["outcome"] for a in attempts)
    samples = sorted(a["elapsed_ms"] for a in attempts if a["outcome"] == "completed")
    quantiles = {}
    for name, fraction in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)]:
        rank = math.ceil(len(samples) * fraction)
        quantiles[name] = {"rank": rank, "ms": samples[rank - 1] if rank else None}
    return {
        "attempted": len(attempts),
        "outcomes": dict(counts),
        "conditional_success_sample": {
            "n": len(samples), "quantiles": quantiles,
            "max_ms": samples[-1] if samples else None,
            "population_tail_claim": False,
        },
        "right_censored": [
            {"completion_lower_bound_ms": a["elapsed_ms"], "operation": a["operation"]}
            for a in attempts if a["outcome"] in ["timed_out", "cancelled_unknown"]
        ],
    }


def expected_burst(length, seed):
    cycle = bytes((i + seed) % 256 for i in range(256))
    return cycle * (length // 256) + cycle[:length % 256]


class ByteOracle:
    """Compare original bytes online, preserving both mismatch and Gap evidence."""

    def __init__(self, expected=b"", cursor=0):
        self.expected = bytearray(expected)
        self.cursor = cursor
        self.verified_bytes = 0
        self.errors = []
        self.gaps = []
        self.observed_hash = hashlib.sha256()

    def expect(self, data):
        self.expected.extend(data)

    def observe(self, start, end, data):
        if end - start != len(data) or start != self.cursor:
            self.errors.append({"type": "range", "cursor": self.cursor, "start": start, "end": end, "length": len(data)})
            return False
        if data != self.expected[start:end] or end > len(self.expected):
            self.errors.append({"type": "bytes", "start": start, "end": end})
            return False
        self.cursor = end
        self.verified_bytes += len(data)
        self.observed_hash.update(data)
        return True

    @property
    def complete(self):
        return self.cursor == len(self.expected) and not self.errors
