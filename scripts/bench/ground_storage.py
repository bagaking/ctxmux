#!/usr/bin/env python3
"""Explicitly selected, read-only storage census; never a performance gate.

Only aggregate numeric facts leave the database. Commands, Run IDs, terminal
bytes, paths and wall-clock times are not included in the output. A live WAL
is read through SQLite, not ignored with immutable=1. See the storage benchmark
contract for the distinction between recorded extents and producer writes.
"""

import argparse
import json
import math
import sqlite3
import time
from collections import Counter
from pathlib import Path


def readonly(database):
    connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", timeout=1)
    connection.execute("PRAGMA query_only=ON")
    return connection


def distribution(histogram):
    """Nearest-rank sample quantiles, with frequencies and a visible denominator."""
    entries = sorted((value, count) for value, count in histogram.items() if count)
    total = sum(count for _, count in entries)

    def quantile(fraction):
        rank = math.ceil(total * fraction)
        seen = 0
        for value, count in entries:
            seen += count
            if seen >= rank:
                return value
        return None

    return {
        "n": total,
        "sum": sum(value * count for value, count in entries),
        "min": entries[0][0] if entries else None,
        "p50": quantile(0.5),
        "p90": quantile(0.9),
        "p99": quantile(0.99),
        "max": entries[-1][0] if entries else None,
    }


def heads(database):
    connection = readonly(database)
    try:
        return dict(connection.execute("SELECT id,durable_output_bytes FROM runs"))
    finally:
        connection.close()


def summarize_heads(previous, current, elapsed):
    shared = previous.keys() & current.keys()
    deltas = [current[key] - previous[key] for key in shared]
    return {
        "elapsed_seconds": elapsed,
        "same_identity_runs": len(shared),
        "new_records": len(current.keys() - previous.keys()),
        "removed_records": len(previous.keys() - current.keys()),
        "advancing_runs": sum(delta > 0 for delta in deltas),
        "unchanged_runs": sum(delta == 0 for delta in deltas),
        "regressing_heads": sum(delta < 0 for delta in deltas),
        "committed_byte_deltas": distribution(Counter(deltas)),
    }


def physical_files(database, referenced):
    categories = {}
    incomplete = False
    # Directory traversal is read-only. Names are classified, never exported.
    for path in database.parent.rglob("*"):
        try:
            if not path.is_file():
                continue
            stat = path.stat()
            relative = path.relative_to(database.parent)
            kind = (
                "database"
                if path == database
                else "wal"
                if path.name == database.name + "-wal"
                else "shm"
                if path.name == database.name + "-shm"
                else "replay"
                if relative.parts[0] == "replay"
                else "terminal_checkpoint"
                if relative.parts[0] == "terminal-checkpoints"
                else "other"
            )
            facts = categories.setdefault(
                kind, {"files": 0, "bytes": 0, "allocated_bytes": 0}
            )
            facts["files"] += 1
            facts["bytes"] += stat.st_size
            facts["allocated_bytes"] += stat.st_blocks * 512
        except OSError:
            incomplete = True
    missing = 0
    short = 0
    for name, required in referenced.items():
        # Database-controlled coordinates may never escape the replay directory.
        if Path(name).name != name or name in ("", ".", ".."):
            missing += 1
            continue
        try:
            if (database.parent / "replay" / name).stat().st_size < required:
                short += 1
        except OSError:
            missing += 1
    return {
        "categories": categories,
        "incomplete": incomplete,
        "missing_referenced_files": missing,
        "short_referenced_files": short,
        "atomic_with_sql_snapshot": False,
    }


def census(database):
    started = time.monotonic()
    connection = readonly(database)
    try:
        connection.execute("BEGIN")
        schema = connection.execute("PRAGMA user_version").fetchone()[0]
        if schema != 6:
            raise ValueError("unsupported census schema; not a corruption diagnosis")
        runs = list(
            connection.execute(
                "SELECT id,state_kind,durable_first_available_byte,durable_output_bytes,"
                "replay_bytes,replay_truncated,metadata_bytes FROM runs"
            )
        )
        histogram = dict(
            connection.execute(
                "SELECT data_bytes,count(*) FROM replay_chunks GROUP BY data_bytes"
            )
        )
        row_counts = dict(
            connection.execute(
                "SELECT run_id,count(*) FROM replay_chunks GROUP BY run_id"
            )
        )
        references = dict(
            connection.execute(
                "SELECT data_file,max(data_offset+data_bytes) FROM replay_chunks GROUP BY data_file"
            )
        )
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        page_count = connection.execute("PRAGMA page_count").fetchone()[0]
        free_pages = connection.execute("PRAGMA freelist_count").fetchone()[0]
        indexed_bytes = distribution(histogram)["sum"]
        connection.commit()
    finally:
        connection.close()
    retained = [run[4] for run in runs]
    total = sum(retained)
    return {
        "schema": "ctxmux.storage-grounding.v1",
        "scope": "one selected store; recorded state, not a fleet or read-frequency qualification",
        "storage_schema": schema,
        "sql_snapshot_seconds": time.monotonic() - started,
        "runs": len(runs),
        "states": dict(Counter(run[1] for run in runs)),
        "running_is_service_health": False,
        "truncated_runs": sum(bool(run[5]) for run in runs),
        "nonzero_floor_runs": sum(run[2] > 0 for run in runs),
        "lifetime_output_bytes": distribution(Counter(run[3] for run in runs)),
        "retained_output_bytes": distribution(Counter(retained)),
        "retained_rows_per_run": distribution(
            Counter(row_counts.get(run[0], 0) for run in runs)
        ),
        "recorded_extent_bytes": distribution(histogram),
        # Thresholds describe this report, not storage/frame capacity or latency rules.
        "extent_buckets": {
            str(bound): {
                "rows": sum(
                    count for size, count in histogram.items() if size <= bound
                ),
                "bytes": sum(
                    size * count for size, count in histogram.items() if size <= bound
                ),
            }
            for bound in (128, 512, 1024, 4096, 16384, 65536)
        },
        "metadata_charged_bytes": sum(run[6] for run in runs),
        "largest_five_retained_share": sum(sorted(retained, reverse=True)[:5]) / total
        if total
        else None,
        "indexed_bytes_match_retained": indexed_bytes == total,
        "database_pages": {
            "page_bytes": page_size,
            "pages": page_count,
            "free_pages": free_pages,
        },
        "physical": physical_files(database, references),
    }


def observe(database, seconds, interval):
    previous = heads(database)
    began = previous_time = time.monotonic()
    samples = []
    while time.monotonic() - began < seconds:
        time.sleep(min(interval, max(0, seconds - (time.monotonic() - began))))
        try:
            current = heads(database)
            now = time.monotonic()
            sample = summarize_heads(previous, current, now - previous_time)
            sample["offset_seconds"] = now - began
            sample["outcome"] = "observed"
            samples.append(sample)
            previous, previous_time = current, now
        except (OSError, sqlite3.Error):
            samples.append(
                {"offset_seconds": time.monotonic() - began, "outcome": "unavailable"}
            )
            # The next successful delta spans the entire unsampled interval.
    return {
        "requested_seconds": seconds,
        "requested_interval_seconds": interval,
        "actual_seconds": time.monotonic() - began,
        "samples": samples,
        "measures": "committed head movement; not producer rate, live delivery, fsyncs or read demand",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--observe-seconds",
        type=float,
        default=0,
        help="Optional passive head observation window; zero means census only.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5,
        help="Observation resolution, not a production flush policy.",
    )
    arguments = parser.parse_args()
    if (
        not math.isfinite(arguments.observe_seconds)
        or not math.isfinite(arguments.interval_seconds)
        or arguments.observe_seconds < 0
        or arguments.interval_seconds <= 0
    ):
        parser.error(
            "observation duration must be finite and nonnegative; interval finite and positive"
        )
    if arguments.output.exists():
        parser.error("output already exists; preserve the previous observation")
    if (
        arguments.output.resolve() == arguments.database.resolve()
        or arguments.output.resolve().is_relative_to(
            arguments.database.resolve().parent
        )
    ):
        parser.error("write evidence outside the observed Runtime directory")
    report = census(arguments.database)
    if arguments.observe_seconds:
        report["head_observation"] = observe(
            arguments.database, arguments.observe_seconds, arguments.interval_seconds
        )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    with arguments.output.open("x") as destination:
        json.dump(report, destination, indent=2)
        destination.write("\n")
    print(
        json.dumps(
            {
                "runs": report["runs"],
                "recorded_rows": report["recorded_extent_bytes"]["n"],
                "retained_bytes": report["retained_output_bytes"]["sum"],
            }
        )
    )


if __name__ == "__main__":
    main()
