import unittest

from metrics import ByteOracle, expected_burst, summarize


class MeasurementContracts(unittest.TestCase):
    def test_failed_and_censored_attempts_cannot_disappear(self):
        attempts = [{"operation": "input", "outcome": "completed", "elapsed_ms": 2}] * 95
        attempts += [{"operation": "input", "outcome": "timed_out", "elapsed_ms": 10000}] * 5
        row = summarize(attempts)
        self.assertEqual(row["attempted"], 100)
        self.assertEqual(row["conditional_success_sample"]["n"], 95)
        self.assertEqual(len(row["right_censored"]), 5)
        self.assertNotIn("ms", row["right_censored"][0])

    def test_small_tail_is_labeled_sample_maximum(self):
        row = summarize([{"operation": "list", "outcome": "completed", "elapsed_ms": n} for n in range(30)])
        tail = row["conditional_success_sample"]
        self.assertEqual(tail["quantiles"]["p99"], {"rank": 30, "ms": 29})
        self.assertFalse(tail["population_tail_claim"])

    def test_loss_reorder_duplicate_and_wrong_payload_are_detected(self):
        for start, end, payload in [(1, 3, b"bc"), (0, 2, b"ba"), (0, 3, b"ab")]:
            oracle = ByteOracle(b"abc")
            self.assertFalse(oracle.observe(start, end, payload))
            self.assertFalse(oracle.complete)
        oracle = ByteOracle(b"abc")
        self.assertTrue(oracle.observe(0, 3, b"abc"))
        self.assertFalse(oracle.observe(0, 3, b"abc"))

    def test_gap_does_not_erase_verified_bytes_or_advance_cursor(self):
        oracle = ByteOracle(b"abc")
        oracle.observe(0, 1, b"a")
        oracle.gaps.append({"latest_output_bytes": 3})
        self.assertEqual((oracle.cursor, oracle.verified_bytes), (1, 1))
        self.assertTrue(oracle.observe(1, 3, b"bc"))
        self.assertTrue(oracle.complete)
        self.assertEqual(len(oracle.gaps), 1)

    def test_binary_payload_is_independent_and_wraps(self):
        self.assertEqual(expected_burst(260, 254), bytes([254, 255]) + bytes(range(256)) + bytes([0, 1]))


if __name__ == "__main__":
    unittest.main()
