import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ground_storage import census, readonly, summarize_heads


class StorageGroundingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.database = self.root / "state.sqlite3"
        self.writer = sqlite3.connect(self.database)
        self.addCleanup(self.writer.close)
        self.writer.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            PRAGMA user_version=6;
            CREATE TABLE runs(id TEXT PRIMARY KEY, state_kind TEXT,
                durable_first_available_byte INTEGER, durable_output_bytes INTEGER,
                replay_bytes INTEGER, replay_truncated INTEGER, metadata_bytes INTEGER,
                spec_json TEXT);
            CREATE TABLE replay_chunks(run_id TEXT, start_byte INTEGER,
                end_byte INTEGER, data_file TEXT, data_offset INTEGER, data_bytes INTEGER);
        """)
        self.writer.execute(
            "INSERT INTO runs VALUES ('secret-run','running',0,5,5,0,40,'private-command')"
        )
        self.writer.execute(
            "INSERT INTO runs VALUES ('quiet-run','exited',0,0,0,0,20,'private-path')"
        )
        self.writer.executemany(
            "INSERT INTO replay_chunks VALUES (?,?,?,?,?,?)",
            [
                ("secret-run", 0, 2, "replay-test.bin", 0, 2),
                ("secret-run", 2, 5, "replay-test.bin", 2, 3),
            ],
        )
        self.writer.commit()
        (self.root / "replay").mkdir()
        (self.root / "replay" / "replay-test.bin").write_bytes(b"abcde")

    def test_live_wal_is_visible_and_connection_cannot_mutate(self):
        connection = readonly(self.database)
        try:
            self.assertEqual(
                connection.execute("SELECT count(*) FROM runs").fetchone()[0], 2
            )
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("DELETE FROM runs")
        finally:
            connection.close()
        self.assertEqual(
            self.writer.execute("SELECT count(*) FROM runs").fetchone()[0], 2
        )

    def test_census_includes_quiet_runs_and_exports_no_identity_or_content(self):
        report = census(self.database)
        self.assertEqual(report["runs"], 2)
        self.assertEqual(report["retained_rows_per_run"]["min"], 0)
        self.assertEqual(report["recorded_extent_bytes"]["sum"], 5)
        self.assertTrue(report["indexed_bytes_match_retained"])
        self.assertEqual(report["physical"]["missing_referenced_files"], 0)
        encoded = json.dumps(report)
        for private in (
            "secret-run",
            "quiet-run",
            "private-command",
            "private-path",
            str(self.root),
            "abcde",
        ):
            self.assertNotIn(private, encoded)

    def test_missing_payload_and_accounting_mismatch_remain_visible(self):
        (self.root / "replay" / "replay-test.bin").unlink()
        self.writer.execute("UPDATE runs SET replay_bytes=9 WHERE id='secret-run'")
        self.writer.commit()
        report = census(self.database)
        self.assertFalse(report["indexed_bytes_match_retained"])
        self.assertEqual(report["physical"]["missing_referenced_files"], 1)

    def test_head_deltas_keep_zero_and_negative_samples_and_record_churn(self):
        sample = summarize_heads(
            {"a": 5, "b": 2, "c": 0, "gone": 8}, {"a": 9, "b": 2, "c": -1, "new": 5}, 2
        )
        self.assertEqual(sample["same_identity_runs"], 3)
        self.assertEqual(sample["unchanged_runs"], 1)
        self.assertEqual(sample["regressing_heads"], 1)
        self.assertEqual(sample["new_records"], 1)
        self.assertEqual(sample["removed_records"], 1)
        self.assertEqual(sample["committed_byte_deltas"]["sum"], 3)

    def test_empty_store_is_an_observation_and_unknown_schema_is_explicit(self):
        self.writer.execute("DELETE FROM replay_chunks")
        self.writer.execute("DELETE FROM runs")
        self.writer.commit()
        report = census(self.database)
        self.assertEqual(report["runs"], 0)
        self.assertIsNone(report["recorded_extent_bytes"]["p99"])
        self.writer.execute("PRAGMA user_version=99")
        with self.assertRaisesRegex(ValueError, "unsupported census schema"):
            census(self.database)


if __name__ == "__main__":
    unittest.main()
