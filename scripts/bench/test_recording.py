"""Adversarial measurement checks; these do not qualify a production Runtime."""

import asyncio
import gzip
import json
from pathlib import Path
import tempfile
import unittest

from run_benchmark import FrameSink, Ledger


class RecordingContracts(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_producers_survive_real_pipe_backpressure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.gz"
            sink = await FrameSink().open(path)
            expected = {(producer, sequence) for producer in range(8) for sequence in range(16)}
            async def producer(identity):
                for sequence in range(16):
                    await sink.write(json.dumps({"producer": identity, "sequence": sequence,
                                                 "payload": "x" * 16384}) + "\n")
            await asyncio.gather(*(producer(identity) for identity in range(8)))
            await sink.close()
            with gzip.open(path, "rt") as source:
                rows = [json.loads(line) for line in source]
            self.assertEqual(len(rows), len(expected))
            self.assertEqual({(row["producer"], row["sequence"]) for row in rows}, expected)
            self.assertTrue(all(row["payload"] == "x" * 16384 for row in rows))
            for identity in range(8):
                self.assertEqual([row["sequence"] for row in rows if row["producer"] == identity], list(range(16)))
            self.assertEqual(sink.facts()["records"], len(rows))

    async def test_real_timeout_and_cancellation_preserve_unknown_results(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory), 0.01)
            with self.assertRaises(asyncio.TimeoutError):
                await ledger.measure("delayed", asyncio.sleep(1))
            pending = asyncio.create_task(ledger.measure("input", asyncio.sleep(1)))
            await asyncio.sleep(0)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            rows = ledger.summary()
            self.assertEqual(rows["delayed"]["outcomes"], {"timed_out": 1})
            self.assertEqual(rows["input"]["outcomes"], {"cancelled_unknown": 1})
            self.assertEqual(len(rows["input"]["right_censored"]), 1)
            self.assertEqual(rows["input"]["conditional_success_sample"]["n"], 0)
            ledger.trace.close()
