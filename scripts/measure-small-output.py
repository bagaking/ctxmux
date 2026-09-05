#!/usr/bin/env python3
"""Isolated macOS daemon measurement through the public protocol (stdlib only).

WAL marker counts are sampled lower bounds, not a trace of durable commits.
Process rusage covers replay, database and WAL writes, including checkpoints.
Status polling and attachment decoding are identical in all comparison arms.
Each fixture owns its daemon, socket, state directory and gated Python Runs.
"""

import argparse
import base64
import bisect
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid


class Usage(ctypes.Structure):
    # rusage_info_v2, macOS SDK sys/resource.h; CPU time is in Mach ticks.
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in (
            "user", "system", "idle", "interrupt", "pageins", "wired",
            "resident", "footprint", "start", "exit", "child_user",
            "child_system", "child_idle", "child_interrupt", "child_pageins",
            "child_elapsed", "read_bytes", "write_bytes",
        )
    ]


LIBPROC = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
LIBPROC.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
LIBPROC.proc_pid_rusage.restype = ctypes.c_int
LIBSYSTEM = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
LIBSYSTEM.clock_gettime_nsec_np.argtypes = [ctypes.c_int]
LIBSYSTEM.clock_gettime_nsec_np.restype = ctypes.c_uint64


class Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


TIMEBASE = Timebase()
assert LIBSYSTEM.mach_timebase_info(ctypes.byref(TIMEBASE)) == 0


def now_ns():
    # CLOCK_MONOTONIC is shared across processes, unlike Python <3.10 on macOS.
    return LIBSYSTEM.clock_gettime_nsec_np(6)


def usage(pid):
    value = Usage()
    if LIBPROC.proc_pid_rusage(pid, 2, ctypes.byref(value)) != 0:
        raise OSError(ctypes.get_errno(), "proc_pid_rusage")
    return value


def connect(path, protocol):
    stream = socket.socket(socket.AF_UNIX)
    stream.settimeout(30)
    wire = None
    try:
        stream.connect(str(path))
        wire = stream.makefile("rwb")
        send(wire, {"type": "hello", "hello": {"protocol": protocol}})
        hello = receive(wire)
        if (hello["type"] != "hello"
                or hello["runtime"]["protocolGeneration"] != protocol):
            raise RuntimeError(f"invalid protocol handshake: {hello}")
        return stream, wire
    except BaseException:
        if wire is not None:
            wire.close()
        stream.close()
        raise


def send(wire, value):
    wire.write(json.dumps(value).encode() + b"\n")
    wire.flush()


def receive(wire):
    line = wire.readline()
    if not line:
        raise RuntimeError("unexpected daemon EOF")
    value = json.loads(line)
    if value["type"] == "error":
        raise RuntimeError(value)
    return value


def request(path, value, protocol):
    stream, wire = connect(path, protocol)
    try:
        send(wire, {"type": "request", "request": value})
        return receive(wire)["response"]
    finally:
        wire.close()
        stream.close()


def spawn(binary, path, state, stderr, protocol):
    process = subprocess.Popen(
        [str(binary), "--socket", str(path), "--state-dir", str(state)],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=stderr,
    )
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"daemon exited: {process.returncode}")
            try:
                stream, wire = connect(path, protocol)
                wire.close()
                stream.close()
                return process
            except (OSError, RuntimeError):
                time.sleep(0.01)
        raise RuntimeError("daemon readiness timeout")
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.wait()
        raise


def quantiles(values):
    ordered = sorted(values)
    return {name: ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]
            for name, fraction in (("p50", 0.5), ("p95", 0.95), ("max", 1))}


def measure(binary, name, runs, count, size, interval, protocol):
    child_code = (
        "import os,time,tty,ctypes; "
        "lib=ctypes.CDLL('/usr/lib/libSystem.B.dylib'); "
        "lib.clock_gettime_nsec_np.restype=ctypes.c_uint64; "
        "tty.setraw(0); open(os.environ['CTXMUX_MEASURE_READY'],'w').close(); os.read(0,1); "
        f"count={count}; size={size}; interval={interval}\n"
        "for i in range(count):\n"
        " data=f'{lib.clock_gettime_nsec_np(6):020d} {i:06d} '.encode()\n"
        " data+=b'x'*(size-len(data)-1)+b'\\n'\n"
        " while data:\n"
        "  n=os.write(1,data); data=data[n:]\n"
        " if i+1<count: time.sleep(interval)\n"
    )
    with tempfile.TemporaryDirectory(prefix="cx-batch-", dir="/tmp") as directory:
        root = Path(directory)
        path, state = root / "socket", root / "state"
        with (root / "stderr").open("wb") as stderr:
            process = spawn(binary, path, state, stderr, protocol)
            stop = threading.Event()
            markers = set()
            rows = []
            errors = []
            wal_thread = None

            def observe_wal():
                while not stop.is_set():
                    try:
                        data = (state / "state.sqlite3-wal").read_bytes()
                        if len(data) >= 32:
                            page_size = struct.unpack_from(">I", data, 8)[0]
                            salt = data[16:24]
                            if page_size:
                                for offset in range(32, len(data) - 23, page_size + 24):
                                    if data[offset + 8:offset + 16] != salt:
                                        break
                                    if struct.unpack_from(">I", data, offset + 4)[0]:
                                        markers.add((salt, offset))
                    except FileNotFoundError:
                        pass
                    stop.wait(0.001)

            def observe_run(row):
                try:
                    stream, wire = connect(path, protocol)
                    with stream, wire:
                        send(wire, {"type": "request", "request": {
                            "type": "attach", "id": row["id"], "after_byte": 0}})
                        assert receive(wire)["type"] == "attached"
                        row["ready"].set()
                        buffered = b""
                        cursor = 0
                        raw_head = 0
                        while True:
                            event = receive(wire)["event"]
                            now = now_ns()
                            if event["type"] == "output":
                                chunk = event["chunk"]
                                assert chunk["start_byte"] == raw_head, (chunk["start_byte"], raw_head)
                                data = base64.b64decode(chunk["data"])
                                raw_head += len(data)
                                buffered += data
                                while b"\n" in buffered:
                                    line, buffered = buffered.split(b"\n", 1)
                                    cursor += len(line) + 1
                                    emitted = int(line[:20])
                                    row["lines"].append((cursor, emitted, now))
                            elif event["type"] == "exited":
                                assert not buffered
                                assert cursor == count * size, cursor
                                row["terminal_ns"] = now
                                return
                            else:
                                raise RuntimeError(event)
                except Exception as error:
                    errors.append(repr(error))
                    row["ready"].set()

            try:
                for _ in range(runs):
                    ready_file = root / str(uuid.uuid4())
                    info = request(path, {"type": "start", "operation_key": str(uuid.uuid4()),
                        "spec": {"program": sys.executable, "args": ["-c", child_code],
                                 "cwd": None, "env": {"CTXMUX_MEASURE_READY": str(ready_file)}, "declared_inputs": [],
                                 "initial_size": {"cols": 80, "rows": 24}}}, protocol)["run"]
                    row = {"id": info["id"], "lines": [], "acks": [],
                           "ready": threading.Event()}
                    rows.append(row)
                    row["thread"] = threading.Thread(target=observe_run, args=(row,))
                    row["thread"].start()
                    assert row["ready"].wait(10), "attachment readiness timeout"
                    deadline = time.monotonic() + 10
                    while not ready_file.exists():
                        assert time.monotonic() < deadline, "child readiness timeout"
                        time.sleep(0.005)
                before = usage(process.pid)
                start = now_ns()
                wal_thread = threading.Thread(target=observe_wal)
                wal_thread.start()
                for row in rows:
                    request(path, {"type": "input", "id": row["id"], "data": [103]}, protocol)
                deadline = time.monotonic() + 30
                while any(row["thread"].is_alive() for row in rows):
                    assert not errors, errors
                    assert time.monotonic() < deadline, "fixture timeout"
                    for row in rows:
                        info = request(path, {"type": "status", "id": row["id"]}, protocol)["run"]
                        row["acks"].append((info["durable_output_bytes"], now_ns()))
                    time.sleep(0.01)
                assert not errors, errors
                for row in rows:
                    row["thread"].join()
                    info = request(path, {"type": "status", "id": row["id"]}, protocol)["run"]
                    assert info["state"]["type"] == "exited", info
                    assert info["durable_output_bytes"] == count * size
                    row["acks"].append((count * size, now_ns()))
                time.sleep(0.1)  # include the final idle checkpoint in both arms
                after = usage(process.pid)
                elapsed = (now_ns() - start) / 1e9
                stop.set()
                wal_thread.join()
                live, durable, terminal = [], [], []
                for row in rows:
                    heads = [ack[0] for ack in row["acks"]]
                    for cursor, emitted, observed in row["lines"]:
                        live.append((observed - emitted) / 1e6)
                        ack = row["acks"][bisect.bisect_left(heads, cursor)][1]
                        durable.append((ack - emitted) / 1e6)
                    terminal.append((row["terminal_ns"] - row["lines"][-1][1]) / 1e6)
                result = {"case": name, "runs": runs, "output_bytes": runs * count * size,
                          "elapsed_s": elapsed, "write_bytes": after.write_bytes - before.write_bytes,
                          "cpu_s": (after.user + after.system - before.user - before.system)
                                   * TIMEBASE.numer / TIMEBASE.denom / 1e9,
                          "wal_commit_markers_lower_bound": len(markers),
                          "live_ms": quantiles(live), "durable_observed_ms": quantiles(durable),
                          "terminal_ms": quantiles(terminal)}
            finally:
                stop.set()
                process.kill()
                process.wait()
                if wal_thread is not None:
                    wal_thread.join()
                for row in rows:
                    row["thread"].join(timeout=2)
            # Actually reopen after SIGKILL, rather than infer recovery from status.
            process = spawn(binary, path, state, stderr, protocol)
            try:
                for row in rows:
                    info = request(path, {"type": "status", "id": row["id"]}, protocol)["run"]
                    assert info["state"]["type"] == "exited"
                    assert info["latest_output_bytes"] == count * size
                    stream, wire = connect(path, protocol)
                    with stream, wire:
                        send(wire, {"type": "request", "request": {
                            "type": "attach", "id": row["id"], "after_byte": 0}})
                        header = receive(wire)["snapshot"]["replay"]
                        retained = bytearray()
                        cursor = header["first_available_byte"]
                        while True:
                            event = receive(wire)["event"]
                            if event["type"] == "exited":
                                break
                            assert event["type"] == "output", event
                            chunk = event["chunk"]
                            assert chunk["start_byte"] == cursor
                            data = base64.b64decode(chunk["data"])
                            retained.extend(data)
                            cursor += len(data)
                        expected = bytearray()
                        for index, (_, emitted, _) in enumerate(row["lines"]):
                            prefix = f"{emitted:020d} {index:06d} ".encode()
                            expected.extend(prefix + b"x" * (size - len(prefix) - 1) + b"\n")
                        assert retained == expected[header["first_available_byte"]:]
                        assert cursor == count * size
                        row["recovered_replay_bytes"] = len(retained)
                result["recovered_replay_bytes"] = [row["recovered_replay_bytes"] for row in rows]
                result["terminal_recovery"] = "passed"
            finally:
                process.kill()
                process.wait()
            return result


def main():
    protocol_source = Path(__file__).resolve().parents[1] / "crates/ctxmux-protocol/src/lib.rs"
    match = re.search(r"\bPROTOCOL_VERSION: u16 = ([0-9]+);", protocol_source.read_text())
    if match is None:
        raise RuntimeError("cannot read the authoritative protocol generation")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--case", choices=["all", "paced", "sparse", "multi", "sustained"], default="all")
    parser.add_argument("--protocol", type=int, default=int(match[1]),
                        help="exact public protocol generation; defaults to the workspace contract; set explicitly for a historical binary")
    args = parser.parse_args()
    if not 0 < args.protocol <= 65535:
        parser.error("--protocol must fit a positive u16 wire generation")
    cases = [("paced", 1, 600, 128, .003), ("sparse", 1, 12, 128, .2),
             ("multi", 4, 600, 128, .003), ("sustained", 1, 2048, 4096, .001)]
    report = {"platform": platform.platform(), "protocol": args.protocol,
              "daemon_sha256": hashlib.sha256(args.daemon.read_bytes()).hexdigest(),
              "wal_sampling_interval_ms": 1, "status_poll_interval_ms": 10, "cases": []}
    for case in cases:
        if args.case in ("all", case[0]):
            value = measure(args.daemon.resolve(), *case, args.protocol)
            report["cases"].append(value)
            args.out.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
