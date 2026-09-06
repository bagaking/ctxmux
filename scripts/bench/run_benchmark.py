#!/usr/bin/env python3
"""Public-protocol native Run measurements; see docs/benchmark-standard.md.

All paths and operational identities are private artifacts. No user Runtime is
contacted. Test windows/load shapes below are recorded experiment parameters,
never daemon capacity requirements or latency SLAs.
"""

import argparse
import asyncio
import base64
import gzip
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import termios
import struct
import sys
import resource
import time
import uuid

from metrics import ByteOracle, expected_burst, summarize

PROTOCOL = 22
FRAME_BYTES = 1024 * 1024  # Public protocol structural frame budget.
VT_SAVE = b"\x1b[2J\x1b[24;80H\x1b7\xe4\xb8\xade\xcc\x81"
VT_RESTORE = b"\x1b8\x1b[0mX\r\n"


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def burst_output(run, length, seed):
    # Identity in each burst distinguishes Runs even when binary cycles repeat.
    return f"B {run['spec']['args'][0]} {length} {seed}\n".encode() + expected_burst(length, seed)


def process_argv(pid):
    """Observation only; never signal a PID discovered through a census."""
    if Path("/proc").is_dir():
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
    return subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True).strip()


def process_sample(pid, detailed=False):
    root = Path("/proc") / str(pid)
    try:
        stat = (root / "stat").read_text().rsplit(")", 1)[1].split()
        status = dict(line.split(":", 1) for line in (root / "status").read_text().splitlines() if ":" in line)
        facts = {"pid": pid, "cpu_ticks": int(stat[11]) + int(stat[12]),
                "start_ticks": int(stat[19]), "rss_kib": int(status["VmRSS"].split()[0]),
                "threads": int(status["Threads"].strip()), "fds": len(list((root / "fd").iterdir())),
                "io": (root / "io").read_text()}
        if detailed:
            try:
                rollup = dict(line.split(":", 1) for line in (root / "smaps_rollup").read_text().splitlines() if ":" in line)
                facts["pss_kib"] = int(rollup["Pss"].split()[0])
            except (OSError, KeyError) as error:
                facts["pss_unavailable"] = repr(error)
        return facts
    except (FileNotFoundError, ProcessLookupError, KeyError, PermissionError):
        return {"pid": pid, "unavailable": True}


def storage_sample(directory):
    files = []
    errors = []
    for path in directory.rglob("*"):
        try:
            if path.is_file():
                stat = path.stat()
                files.append({"path": str(path.relative_to(directory)), "bytes": stat.st_size,
                              "allocated_bytes": stat.st_blocks * 512})
        except OSError as error:
            errors.append({"path": str(path), "error": repr(error)})
    return {"files": files, "logical_bytes": sum(f["bytes"] for f in files),
            "allocated_bytes": sum(f["allocated_bytes"] for f in files), "errors": errors}


def host_sample():
    facts = {}
    for name, path in {
        "cpu_stat": "/sys/fs/cgroup/cpu.stat", "cpu_pressure": "/sys/fs/cgroup/cpu.pressure",
        "memory_current": "/sys/fs/cgroup/memory.current", "memory_peak": "/sys/fs/cgroup/memory.peak",
        "memory_events": "/sys/fs/cgroup/memory.events", "pids_current": "/sys/fs/cgroup/pids.current",
        "pids_events": "/sys/fs/cgroup/pids.events", "io_stat": "/sys/fs/cgroup/io.stat",
        "host_cpu_stat": "/proc/stat", "host_load": "/proc/loadavg", "pty_nr": "/proc/sys/kernel/pty/nr",
    }.items():
        try:
            facts[name] = Path(path).read_text()
        except OSError as error:
            facts[name] = {"unavailable": str(error)}
    return facts


class Ledger:
    def __init__(self, directory, timeout):
        self.directory = directory
        self.timeout = timeout
        self.attempts = []
        self.trace = (directory / "attempts.jsonl").open("a")

    async def measure(self, operation, call, **context):
        row = {"operation": operation, "start": time.monotonic(), **context}
        try:
            result = await asyncio.wait_for(call, self.timeout)
            row["outcome"] = "completed"
            return result
        except (TimeoutError, asyncio.TimeoutError):
            row["outcome"] = "timed_out"
            raise
        except asyncio.CancelledError:
            row.update(outcome="cancelled_unknown", error="caller cancelled; operation result is unknown")
            raise
        except Exception as error:
            row.update(outcome="failed", error=repr(error))
            raise
        finally:
            row.update(end=time.monotonic())
            row["elapsed_ms"] = (row["end"] - row["start"]) * 1000
            self.attempts.append(row)
            self.trace.write(json.dumps(row) + "\n")
            self.trace.flush()

    def summary(self):
        return {name: summarize([r for r in self.attempts if r["operation"] == name])
                for name in sorted({r["operation"] for r in self.attempts})}


class Protocol:
    def __init__(self, socket, identity=None, trace=None):
        self.socket = socket
        self.identity = identity
        self.trace = trace

    async def record(self, direction, frame, **context):
        if self.trace:
            await self.trace.write(json.dumps({"monotonic": time.monotonic(), "direction": direction, **context, "frame": frame}) + "\n")

    async def connect(self):
        reader, writer = await asyncio.open_unix_connection(str(self.socket), limit=FRAME_BYTES + 1)
        try:
            sent = await self.send(writer, {"type": "hello", "hello": {"protocol": PROTOCOL}})
            await self.record("sent", {"type": "hello", "hello": {"protocol": PROTOCOL}}, submitted_monotonic=sent)
            hello = await self.read(reader)
            await self.record("received", hello)
            if hello["type"] != "hello" or hello["runtime"]["protocolGeneration"] != PROTOCOL:
                raise RuntimeError("invalid Hello: " + repr(hello))
            if self.identity is not None and hello["runtime"]["daemonInstanceId"] != self.identity["daemonInstanceId"]:
                raise RuntimeError("daemon incarnation changed")
            return reader, writer, hello["runtime"]
        except BaseException:
            writer.close()
            await writer.wait_closed()
            raise

    @staticmethod
    async def send(writer, frame):
        data = json.dumps(frame, separators=(",", ":")).encode() + b"\n"
        if len(data) - 1 > FRAME_BYTES:
            raise ValueError("generated oversized frame")
        writer.write(data)
        submitted = time.monotonic()
        await writer.drain()
        return submitted

    async def read(self, reader):
        line = await reader.readline()
        if not line:
            raise EOFError("protocol disconnected")
        if len(line) - 1 > FRAME_BYTES:
            raise ValueError("oversized server frame")
        frame = json.loads(line)
        if frame["type"] == "error":
            await self.record("received", frame, error_boundary="framed_refusal")
            raise RuntimeError(json.dumps(frame))
        return frame

    async def request(self, request):
        reader, writer, _ = await self.connect()
        try:
            await self.record("offered", {"type": "request", "request": request})
            sent = await self.send(writer, {"type": "request", "request": request})
            await self.record("sent", {"type": "request", "request": request}, submitted_monotonic=sent)
            frame = await self.read(reader)
            await self.record("received", frame)
            if frame["type"] != "response":
                raise RuntimeError("unexpected request frame: " + repr(frame))
            response = frame["response"]
            if response["type"] == "control_rejected":
                raise RuntimeError(json.dumps(response))
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    async def listing(self):
        runs, after = [], None
        while True:
            response = await self.request({"type": "list", "after": after})
            runs.extend(response["runs"])
            after = response["next_cursor"]
            if after is None:
                break
        ids = [r["id"] for r in runs]
        if ids != sorted(set(ids)):
            raise AssertionError("list duplicated or reordered stable Run ids")
        return runs


class View:
    def __init__(self, protocol, run, expected, timeout, cursor=0):
        self.protocol, self.run, self.timeout = protocol, run, timeout
        self.oracle = ByteOracle(expected, cursor)
        self.pending = {}
        self.command_id = 0
        self.changed = asyncio.Event()
        self.events = []
        self.error = None
        self.recoveries = []
        self.seed_bytes = 0
        self.seed_hash = hashlib.sha256()

    async def open(self, view="raw"):
        self.reader, self.writer, _ = await self.protocol.connect()
        request = {"type": "request", "request": {
            "type": "attach", "id": self.run["id"], "view": view, "after_byte": self.oracle.cursor}}
        await self.protocol.record("offered", request, run=self.run["id"])
        sent = await self.protocol.send(self.writer, request)
        await self.protocol.record("sent", request, run=self.run["id"], submitted_monotonic=sent)
        frame = await self.protocol.read(self.reader)
        if frame["type"] != "attached" or frame["snapshot"]["run"]["id"] != self.run["id"]:
            raise AssertionError("wrong attachment identity")
        self.header = frame["snapshot"]
        await self.protocol.record("received", frame, run=self.run["id"], view_cursor=self.oracle.cursor)
        if self.header["replay"]["first_available_byte"] > self.oracle.cursor:
            raise AssertionError("expected byte prefix was truncated: " + repr(self.header["replay"]))
        self.task = asyncio.create_task(self.consume())
        return self

    async def consume(self):
        try:
            while True:
                frame = await self.protocol.read(self.reader)
                await self.protocol.record("received", frame, run=self.run["id"], view_cursor=self.oracle.cursor)
                kind = frame["type"]
                if kind == "event":
                    event = frame["event"]
                    if event["type"] == "output":
                        chunk = event["chunk"]
                        data = base64.b64decode(chunk["data"], validate=True)
                        self.oracle.observe(chunk["start_byte"], chunk["end_byte"], data)
                    elif event["type"] == "gap":
                        self.oracle.gaps.append(event)
                        self.needs_replay = True
                        return
                    elif event["type"] in ["exited", "interrupted"]:
                        self.events.append(event)
                        return
                    else:
                        self.events.append(event)
                elif kind == "command_result":
                    future = self.pending.pop(frame["command_id"])
                    future.set_result(frame["outcome"])
                elif kind == "replay_window":
                    self.error = "retained replay changed: " + repr(frame)
                elif kind == "detached":
                    return
                elif kind == "terminal_checkpoint_chunk":
                    # Synthetic terminal seed has no original byte cursor.
                    seed = base64.b64decode(frame["data"], validate=True)
                    if frame["offset"] != self.seed_bytes:
                        raise AssertionError("synthetic checkpoint chunks reordered or duplicated")
                    self.seed_bytes += len(seed)
                    self.seed_hash.update(seed)
                else:
                    self.error = "unexpected attachment frame: " + repr(frame)
                self.changed.set()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.error = repr(error)
        finally:
            self.changed.set()
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(RuntimeError(self.error or "attachment ended"))

    async def command(self, kind, expected_output=None, **fields):
        # Trace backpressure may yield. Establish the expected bytes only after
        # that yield, immediately before the one atomic socket write.
        command = {"type": kind, **fields}
        await self.protocol.record("dispatch", command, run=self.run["id"])
        self.command_id += 1
        future = asyncio.get_running_loop().create_future()
        self.pending[self.command_id] = future
        command = {"type": kind, "command_id": self.command_id, **fields}
        if expected_output is not None:
            self.oracle.expect(expected_output)
        target = len(self.oracle.expected)
        sent = await self.protocol.send(self.writer, command)
        await self.protocol.record("sent", command, run=self.run["id"], submitted_monotonic=sent)
        outcome = await future
        if outcome["type"] != "accepted":
            raise RuntimeError(json.dumps(outcome))
        return outcome["receipt"], target

    async def wait_bytes(self, target=None):
        target = len(self.oracle.expected) if target is None else target
        async def wait():
            while self.oracle.cursor < target:
                if self.error or self.oracle.errors:
                    raise AssertionError(self.error or self.oracle.errors)
                if getattr(self, "needs_replay", False):
                    started = time.monotonic()
                    cursor = self.oracle.cursor
                    await self.close()
                    self.needs_replay = False
                    await self.open()
                    self.recoveries.append({"cursor": cursor, "reconnected_seconds": time.monotonic() - started})
                    continue
                if self.task.done():
                    raise EOFError("attachment ended before expected bytes")
                self.changed.clear()
                await self.changed.wait()
            if self.error or self.oracle.errors:
                raise AssertionError(self.error or self.oracle.errors)
        await asyncio.wait_for(wait(), self.timeout)

    async def close(self):
        if hasattr(self, "writer"):
            self.writer.close()
            await self.writer.wait_closed()
        if hasattr(self, "task"):
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    def facts(self):
        return {"id": self.run["id"], "pid": self.run["pid"], "cursor": self.oracle.cursor,
                "expected_bytes": len(self.oracle.expected), "verified_bytes": self.oracle.verified_bytes,
                "expected_sha256": hashlib.sha256(self.oracle.expected).hexdigest(),
                "observed_segment_sha256": self.oracle.observed_hash.hexdigest(),
                "observed_segment_start": self.oracle.cursor - self.oracle.verified_bytes,
                "errors": self.oracle.errors, "gaps": self.oracle.gaps, "recoveries": self.recoveries, "view_error": self.error}


class FrameSink:
    async def open(self, path):
        self.process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).with_name("frame_recorder.py")),
            str(path), stdin=asyncio.subprocess.PIPE)
        self.rows, self.bytes, self.drain_seconds, self.peak_buffer_bytes = 0, 0, 0, 0
        self.write_lock = asyncio.Lock()  # One pipe owner, including older Python drain().
        self.queue_wait_seconds = 0
        self.buffer_limits = self.process.stdin.transport.get_write_buffer_limits()
        return self

    async def write(self, text):
        payload = text.encode()
        began = time.monotonic()
        async with self.write_lock:
            self.queue_wait_seconds += time.monotonic()-began
            self.process.stdin.write(payload)
            self.rows += 1
            self.bytes += len(payload)
            self.peak_buffer_bytes = max(self.peak_buffer_bytes, self.process.stdin.transport.get_write_buffer_size())
            began = time.monotonic()
            await self.process.stdin.drain()
            self.drain_seconds += time.monotonic()-began

    async def close(self):
        self.process.stdin.close()
        await self.process.stdin.wait_closed()
        await self.process.wait()
        if self.process.returncode:
            raise RuntimeError("raw frame recorder failed")

    def facts(self):
        return {"pid": self.process.pid, "records": self.rows, "uncompressed_bytes": self.bytes,
                "drain_seconds": self.drain_seconds, "peak_pending_bytes": self.peak_buffer_bytes,
                "queue_wait_seconds": self.queue_wait_seconds,
                "transport_buffer_limits": self.buffer_limits, "lossless": True,
                "backpressure_policy": "finite OS/asyncio pipe; caller waits, never drops records"}


class Runtime:
    def __init__(self, args, directory, mode):
        self.args, self.directory, self.mode = args, directory, mode
        # Unix socket pathname limits are structural; artifact roots may be long.
        self.socket_directory = tempfile.TemporaryDirectory(prefix="ctxmux-bench-", dir="/tmp")
        self.socket = Path(self.socket_directory.name) / "daemon.sock"
        self.barrier = Path(self.socket_directory.name) / "barrier"
        os.mkfifo(self.barrier)
        self.barrier_fd = os.open(self.barrier, os.O_RDWR | os.O_NONBLOCK)
        self.ledger = Ledger(directory, args.timeout)
        self.runs, self.views, self.resources = [], [], []
        self.background = []
        self.sampling_errors = []
        self.started = time.monotonic()
        self.epoch = 0

    async def open(self):
        argv = [str(self.args.daemon), "--socket", str(self.socket)]
        if getattr(self.args, "resource_limits", None) is not None:
            argv += ["--resource-limits", json.dumps(self.args.resource_limits, separators=(",", ":"))]
        if self.mode == "persistent":
            argv += ["--state-dir", str(self.directory / "state")]
        self.stdout = (self.directory / f"daemon-{self.epoch}.stdout").open("wb")
        self.stderr = (self.directory / f"daemon-{self.epoch}.stderr").open("wb")
        self.process = await asyncio.create_subprocess_exec(*argv, stdout=self.stdout, stderr=self.stderr)
        # Lossless compression retains every frame. Compression cost belongs
        # to the generator and remains in the resource measurements.
        self.frames = await FrameSink().open(self.directory / f"frames-{self.epoch}.private.jsonl.gz")
        self.protocol = Protocol(self.socket, trace=self.frames)
        async def ready():
            while True:
                if self.process.returncode is not None:
                    raise RuntimeError("daemon exited before ready")
                try:
                    reader, writer, identity = await self.protocol.connect()
                    writer.close()
                    await writer.wait_closed()
                    self.protocol.identity = identity
                    return identity
                except (FileNotFoundError, ConnectionRefusedError):
                    await asyncio.sleep(0.01)
        self.identity = await self.ledger.measure("daemon_ready", ready())
        save(self.directory / f"identity-{self.epoch}.private.json", {"runtime": self.identity,
            "daemon": str(self.args.daemon), "sha256": sha(self.args.daemon), "argv": argv,
            "pid": self.process.pid, "initial_process": process_sample(self.process.pid)})
        self.sampling = asyncio.create_task(self.sample())

    async def sample(self):
        previous = time.monotonic()
        while True:
            now = time.monotonic()
            pids = [run["pid"] for run in self.runs if run.get("pid")]
            def collect():
                row = {"monotonic": now, "sample_interval_seconds": now - previous,
                       "daemon": process_sample(self.process.pid, detailed=True), "generator": process_sample(os.getpid(), detailed=True),
                       "recorder": process_sample(self.frames.process.pid, detailed=True), "host": host_sample()}
                children = [process_sample(pid) for pid in pids]
                live = [child for child in children if not child.get("unavailable")]
                row["fixture_children"] = {"sampled": len(children), "available": len(live),
                                           "rss_kib_sum": sum(c["rss_kib"] for c in live),
                                           "cpu_ticks_sum": sum(c["cpu_ticks"] for c in live)}
                row["fixture_children"]["rss_scope"] = "sum of process RSS; shared pages may be counted repeatedly"
                row["durable_storage"] = storage_sample(self.directory / "state")
                row["sampler_seconds"] = time.monotonic() - now
                with gzip.open(self.directory / "resources.jsonl.gz", "at") as output:
                    output.write(json.dumps(row) + "\n")
                return row
            work = asyncio.create_task(asyncio.to_thread(collect))
            try:
                await asyncio.shield(work)
            except asyncio.CancelledError:
                try:
                    await work  # Complete the selected epoch's census before recovery.
                except Exception as error:
                    self.sampling_errors.append({"monotonic": now, "epoch": self.epoch, "error": repr(error)})
                    save(self.directory / "sampling-errors.private.json", self.sampling_errors)
                    raise
                raise
            except Exception as error:
                self.sampling_errors.append({"monotonic": now, "epoch": self.epoch, "error": repr(error)})
                save(self.directory / "sampling-errors.private.json", self.sampling_errors)
                raise
            previous = now
            await asyncio.sleep(self.args.sample_interval)

    async def start(self, label, attach=False):
        spec = {"program": str(self.args.fixture), "args": [label, str(self.barrier)], "cwd": None,
                "env": {}, "initial_size": {"rows": 24, "cols": 80}, "declared_inputs": []}
        response = await self.ledger.measure("start", self.protocol.request({"type": "start", "spec": spec,
                                               "operation_key": str(uuid.uuid4())}), label=label)
        run = response["run"]
        if run["spec"] != spec or run["state"]["type"] != "running" or run["pid"] is None:
            raise AssertionError("accepted Run specification/lifecycle differs")
        self.runs.append(run)
        expected = f"READY {label} {run['pid']}\n".encode()
        if attach:
            view = await self.ledger.measure("attach", View(self.protocol, run, expected, self.args.timeout).open())
            self.views.append(view)
            await self.ledger.measure("first_output", view.wait_bytes(), run=run["id"])
            return run, view
        return run, expected

    async def input(self, run, data):
        receipt = await self.ledger.measure("input_receipt", self.protocol.request({"type": "input", "id": run["id"],
                                                                                  "data": list(data)}), run=run["id"])
        if receipt["receipt"] != {"type": "input", "written_bytes": len(data)}:
            raise AssertionError("wrong PTY input receipt")
        return receipt

    async def probe(self, run, view, sequence, via_attachment=False):
        command = f"P {sequence}\n".encode()
        expected = f"P {run['spec']['args'][0]} {sequence}\n".encode()
        async def action():
            if via_attachment:
                receipt, end = await view.command("input", expected_output=expected, data=list(command))
                if receipt != {"type": "input", "written_bytes": len(command)}:
                    raise AssertionError("wrong attached input receipt")
            else:
                view.oracle.expect(expected)
                end = len(view.oracle.expected)
                await self.input(run, command)
            await view.wait_bytes(end)
        await self.ledger.measure("input_output_attached" if via_attachment else "input_output_fresh", action(), run=run["id"])

    async def close(self):
        for task in self.background:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        for view in self.views:
            await view.close()
        cleanup = {"attempts": [], "forced": [], "remaining_children": [], "census_errors": []}
        if hasattr(self, "protocol") and self.protocol.identity:
            try:
                listing = await asyncio.wait_for(self.protocol.listing(), self.args.timeout)
                known = {r["id"] for r in self.runs}
                self.runs.extend(r for r in listing if r["id"] not in known)
            except Exception as error:
                listing = self.runs
                cleanup["list_error"] = repr(error)
            sem = asyncio.Semaphore(self.args.concurrency)
            async def stop(run):
                async with sem:
                    operation = {"daemon_instance": self.identity["daemonInstanceId"],
                                 "operation_key": str(uuid.uuid4()), "id": run["id"]}
                    try:
                        if run["state"]["type"] == "running":
                            await self.ledger.measure("stop", self.protocol.request({"type": "stop", "operation": operation}), run=run["id"])
                        await self.ledger.measure("remove", self.protocol.request({"type": "remove", "id": run["id"]}), run=run["id"])
                        cleanup["attempts"].append({"id": run["id"], "outcome": "removed"})
                    except Exception as error:
                        cleanup["attempts"].append({"id": run["id"], "outcome": "failed", "error": repr(error)})
            await asyncio.gather(*(stop(run) for run in listing))
        if hasattr(self, "sampling"):
            self.sampling.cancel()
            sampled = await asyncio.gather(self.sampling, return_exceptions=True)
            cleanup["sampling_errors"] = self.sampling_errors + [repr(x) for x in sampled
                if isinstance(x, BaseException) and not isinstance(x, asyncio.CancelledError)]
        if hasattr(self, "process"):
            if self.process.returncode is None:
                self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), self.args.timeout)
            except TimeoutError:
                cleanup["forced"].append({"owner": "benchmark daemon", "pid": self.process.pid})
                self.process.kill()
                await self.process.wait()
        # Only fixture children accepted by this private Runtime, with identity revalidation.
        for run in self.runs:
            pid = run.get("pid")
            if not pid:
                continue
            try:
                argv = process_argv(pid)
                if str(self.args.fixture) in argv and run["spec"]["args"][0] in argv:
                    cleanup["remaining_children"].append(pid)
            except (FileNotFoundError, ProcessLookupError):
                pass
            except subprocess.CalledProcessError as error:
                if error.returncode != 1:  # ps returns 1 when this PID is absent.
                    cleanup["census_errors"].append({"pid": pid, "error": repr(error)})
            except Exception as error:
                cleanup["census_errors"].append({"pid": pid, "error": repr(error)})
        save(self.directory / "cleanup.private.json", cleanup)
        save(self.directory / "views.private.json", [view.facts() for view in self.views])
        save(self.directory / "latencies.json", self.ledger.summary())
        self.ledger.trace.close()
        if hasattr(self, "stdout"):
            self.stdout.close()
            self.stderr.close()
        if hasattr(self, "frames"):
            await self.frames.close()
            save(self.directory / f"recorder-{self.epoch}.private.json", self.frames.facts())
        os.close(self.barrier_fd)
        self.socket_directory.cleanup()
        return cleanup

    def save_workload_snapshot(self, result):
        """Keep finished workload evidence while lifecycle cleanup is pending."""
        save(self.directory / "workload.private.json", {
            "phase": "before_cleanup", "cleanup_qualified": False, "result": result,
        })
        save(self.directory / "views-before-cleanup.private.json", [view.facts() for view in self.views])
        save(self.directory / "latencies-before-cleanup.json", self.ledger.summary())


async def bounded_map(concurrency, items, action):
    semaphore = asyncio.Semaphore(concurrency)
    async def one(item):
        async with semaphore:
            return await action(item)
    return await asyncio.gather(*(one(item) for item in items), return_exceptions=True)


async def cli_terminal(runtime):
    """Actual interactive CLI in an owned PTY; original Run survives detach."""
    run, view = await runtime.start("cli", True)
    transcript = bytearray()
    rounds = []
    for sequence in [81, 82]:
        master, slave = os.openpty()
        os.set_blocking(master, False)
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
        process = await asyncio.create_subprocess_exec(str(runtime.args.fixture), "--exec-tty", str(runtime.args.cli), "--socket", str(runtime.socket),
            "attach", run["id"], str(view.oracle.cursor), stdin=slave, stdout=slave, stderr=slave,
            start_new_session=True)
        began = time.monotonic()
        async def raw_ready():
            while termios.tcgetattr(slave)[3] & termios.ICANON:
                if process.returncode is not None:
                    raise AssertionError("interactive CLI exited before entering raw input")
                await asyncio.sleep(0.01)
        async def read_marker(marker, after=0):
            while marker not in transcript[after:]:
                if process.returncode is not None:
                    raise AssertionError("CLI exited before expected response")
                try:
                    data = os.read(master, 65536)
                    if not data:
                        raise EOFError("CLI terminal closed before expected response")
                    transcript.extend(data)
                except BlockingIOError:
                    await asyncio.sleep(0.005)
        try:
            await runtime.ledger.measure("cli_attach_ready", raw_ready(), run=run["id"])
            view.oracle.expect(f"P cli {sequence}\n".encode())
            os.write(master, f"P {sequence}\n".encode())
            await runtime.ledger.measure("cli_terminal_input_output", read_marker(f"P cli {sequence}".encode()), run=run["id"])
            await view.wait_bytes()
            view.oracle.expect(b"INT cli\n")
            previous = len(transcript)
            os.write(master, b"\x03")
            await runtime.ledger.measure("cli_ctrl_c", read_marker(b"INT cli", previous), run=run["id"])
            await view.wait_bytes()
            os.write(master, b"\x02d")
            await runtime.ledger.measure("cli_detach", process.wait(), run=run["id"])
            if process.returncode:
                raise AssertionError("interactive CLI detach failed")
            status = (await runtime.protocol.request({"type": "status", "id": run["id"]}))["run"]
            if status["pid"] != run["pid"] or status["native_service"]["owner"]["type"] != "serving":
                raise AssertionError("Run did not survive CLI detach")
            rounds.append({"sequence": sequence, "cli_pid": process.pid, "seconds": time.monotonic()-began,
                           "original_run_id": run["id"], "original_child_pid": run["pid"]})
        finally:
            if process.returncode is None:
                process.terminate()
                await process.wait()
            os.close(master)
            os.close(slave)
            (runtime.directory / "cli-terminal.private.bin").write_bytes(transcript)
    return {"outcome": "completed", "rounds": rounds, "raw_view": view.facts(),
            "transcript_sha256": hashlib.sha256(transcript).hexdigest()}


async def fleet(runtime, population, soak=0):
    errors = []
    accepted = await bounded_map(runtime.args.concurrency, range(population),
                                 lambda i: runtime.start(f"r{i}"))
    failed = [repr(x) for x in accepted if isinstance(x, BaseException)]
    if failed:
        survivors = [x for x in accepted if not isinstance(x, BaseException)]
        isolation = {"outcome": "not_executed", "reason": "no fully started Run available after admission failures"}
        if survivors:
            run, ready = survivors[0]
            try:
                view = await View(runtime.protocol, run, ready, runtime.args.timeout).open()
                runtime.views.append(view)
                await view.wait_bytes()
                await runtime.probe(run, view, 9001)
                status = (await runtime.protocol.request({"type": "status", "id": run["id"]}))["run"]
                if status["pid"] != run["pid"] or status["native_service"]["owner"]["type"] != "serving":
                    raise AssertionError("admission failure changed the surviving Run or its service owner")
                isolation = {"outcome": "completed", "run": run["id"], "pid": run["pid"],
                             "status": status, "bytes": view.facts()}
            except Exception as error:
                isolation = {"outcome": "failed", "run": run["id"], "error": repr(error)}
        return {"population": population, "accepted": len(runtime.runs), "outcome": "failed",
                "start_errors": failed, "admission_failure_isolation": isolation}
    listing = await runtime.ledger.measure("list_all", runtime.protocol.listing())
    if {r["id"] for r in listing} != {r["id"] for r in runtime.runs}:
        raise AssertionError("stable fleet enumeration omitted/added Runs")
    idle = process_sample(runtime.process.pid)
    await asyncio.sleep(runtime.args.idle_seconds)
    phases = []
    expected_by_run = {run["id"]: ready for run, ready in accepted}
    for shape in ["active", "mixed"]:
        phase_start = time.monotonic()
        phase_views = []
        async def arm(item):
            i, (run, ready) = item
            # All-active phase writes binary to every Run. Mixed phase varies
            # shape by stable identity, keeping 3/4 of the fleet quiet.
            if shape == "mixed" and i % 4:
                return
            prefix = expected_by_run[run["id"]]
            view = await View(runtime.protocol, run, prefix, runtime.args.timeout).open()
            runtime.views.append(view)
            phase_views.append(view)
            # Replaying the known prefix is checked before new expected bytes.
            await view.wait_bytes()
            if shape == "active":
                data = burst_output(run, runtime.args.active_bytes, i)
                command = f"B {runtime.args.active_bytes} {i}\n".encode()
            else:
                label = run["spec"]["args"][0]
                data = b"".join(f"T {label} {j} \x1b[32m\u4e2de\u0301\x1b[0m\n".encode() for j in range(runtime.args.token_count))
                command = f"T {runtime.args.token_count} {runtime.args.token_interval_us}\n".encode()
            # Active output is retained across phases: expected prefix is carried
            # by the independently generated prior workload, not read back.
            # FIFO gates the workload, not the Runtime: every selected child is
            # already attached and armed before release. Dispatch concurrency
            # must not turn a 4000-active experiment into eight active Runs.
            view.oracle.expect(f"ARM {run['spec']['args'][0]}\n".encode())
            arm_end = len(view.oracle.expected)
            view.payload_start = arm_end
            if shape == "active":
                view.payload_start += len(data) - runtime.args.active_bytes
            view.oracle.expect(data)
            expected_by_run[run["id"]] = bytes(view.oracle.expected)
            await runtime.input(run, b"A " + command)
            await view.wait_bytes(arm_end)
        async def observed_arm(item):
            i, (run, _) = item
            return await runtime.ledger.measure("arm_" + shape, arm(item), run=run["id"], selected_index=i)
        results = await bounded_map(runtime.args.concurrency, list(enumerate(accepted)), observed_arm)
        if any(isinstance(x, BaseException) for x in results):
            errors += [{"phase": shape, "run": accepted[i][0]["id"], "selected_index": i, "error": repr(x)}
                       for i, x in enumerate(results) if isinstance(x, BaseException)]
            save(runtime.directory / "phase-errors.private.json", {"population": population, "accepted": len(runtime.runs),
                 "phase": shape, "attempted": len(results), "completed": len(results) - len(errors), "errors": errors})
            raise AssertionError("fleet could not arm all selected producers; workload was not reduced")
        released = time.monotonic()
        tickets = b"x" * len(phase_views)
        while tickets:
            try:
                written = os.write(runtime.barrier_fd, tickets)
                tickets = tickets[written:]
            except BlockingIOError:
                await asyncio.sleep(0)
        results = await asyncio.gather(*(runtime.ledger.measure("payload_verified_" + shape, view.wait_bytes(),
                                        run=view.run["id"]) for view in phase_views), return_exceptions=True)
        errors += [{"phase": shape, "error": repr(x)} for x in results if isinstance(x, BaseException)]
        elapsed = time.monotonic() - phase_start
        facts = [v.facts() for v in phase_views]
        verified_payload = sum(max(0, view.oracle.cursor - view.payload_start) for view in phase_views)
        phases.append({"shape": shape, "seconds": elapsed, "participating_runs": len(facts),
                       "verified_bytes_including_replay": sum(v["verified_bytes"] for v in facts),
                       "verified_new_payload_bytes": verified_payload,
                       "verified_payload_bytes_per_second": verified_payload / elapsed,
                       "release_to_verified_seconds": time.monotonic() - released,
                       "views": facts, "resource": process_sample(runtime.process.pid)})
        for view in phase_views:
            await view.close()
    # Continuous sparse probes next to paced ANSI producers during the soak.
    if soak:
        run, ready = accepted[-1]
        i = population - 1
        expected = expected_by_run[run["id"]]
        view = await View(runtime.protocol, run, expected, runtime.args.timeout).open()
        runtime.views.append(view)
        await view.wait_bytes()
        producers = []
        count = int(soak * 1000000 / runtime.args.token_interval_us)
        selected = accepted[:-1:4]
        if runtime.args.soak_streams:
            selected = selected[:runtime.args.soak_streams]
        for producer, _ in selected:
            stream_view = await View(runtime.protocol, producer, expected_by_run[producer["id"]], runtime.args.timeout).open()
            runtime.views.append(stream_view)
            await stream_view.wait_bytes()
            label = producer["spec"]["args"][0]
            stream_view.oracle.expect(b"".join(f"T {label} {j} \x1b[32m\u4e2de\u0301\x1b[0m\n".encode() for j in range(count)))
            await runtime.input(producer, f"T {count} {runtime.args.token_interval_us}\n".encode())
            producers.append(stream_view)
        began, scheduled, probe_rows = time.monotonic(), [], []
        async def scheduled_soak_probe(sequence, intended):
            sent = time.monotonic()
            try:
                await runtime.probe(run, view, sequence, via_attachment=True)
                outcome = "completed"
            except Exception as error:
                errors.append({"phase": "soak", "sequence": sequence, "error": repr(error)})
                outcome = repr(error)
            probe_rows.append({"sequence": sequence, "intended": intended, "sent": sent,
                               "end": time.monotonic(), "schedule_late_ms": max(0, (sent-intended)*1000),
                               "outcome": outcome})
        sequence = 0
        while sequence * runtime.args.soak_probe_interval < soak:
            intended = began + sequence * runtime.args.soak_probe_interval
            await asyncio.sleep(max(0, intended - time.monotonic()))
            scheduled.append(asyncio.create_task(scheduled_soak_probe(sequence, intended)))
            sequence += 1
        await asyncio.gather(*scheduled)
        await asyncio.gather(*(v.wait_bytes() for v in producers))
        phases.append({"shape": "soak", "seconds_requested": soak, "probes": sequence,
                       "open_loop_schedule": probe_rows,
                       "view": view.facts(), "continuous_producer_count": len(producers),
                       "continuous_producers": [v.facts() for v in producers]})
    return {"population": population, "accepted": len(runtime.runs), "outcome": "failed" if errors else "completed",
            "idle": idle, "phases": phases, "errors": errors}


async def scene(runtime):
    (run, a), (other, b) = await asyncio.gather(runtime.start("interactive", True), runtime.start("neighbor", True))
    await runtime.probe(run, a, 0)
    await runtime.probe(other, b, 0, True)
    await a.close()
    a = await View(runtime.protocol, run, a.oracle.expected, runtime.args.timeout, a.oracle.cursor).open()
    runtime.views.append(a)
    await runtime.probe(run, a, 1, True)
    # Original cursor save before shrinking; raw bytes remain independently checked.
    a.oracle.expect(VT_SAVE)
    await runtime.input(run, b"V\n")
    await a.wait_bytes()
    receipt, _ = await a.command("resize", size={"rows": 1, "cols": 2})
    if receipt != {"type": "resize", "applied_size": {"rows": 1, "cols": 2}}:
        raise AssertionError("resize owner did not confirm narrow geometry")
    a.oracle.expect(VT_RESTORE)
    await runtime.input(run, b"R\n")
    await a.wait_bytes()
    await runtime.probe(other, b, 1)
    terminal = await View(runtime.protocol, run, a.oracle.expected, runtime.args.timeout, a.oracle.cursor).open("terminal")
    runtime.views.append(terminal)
    continuation = terminal.header["terminal"]
    if continuation["type"] != "basic_vt":
        raise AssertionError("narrow saved-cursor regression lost its supported terminal continuation")
    checkpoint = continuation["checkpoint"]
    if checkpoint["run_id"] != run["id"] or checkpoint["size"] != {"rows": 1, "cols": 2}:
        raise AssertionError("terminal seed has wrong Run or acknowledged geometry")
    async def seed_complete():
        while terminal.seed_bytes < checkpoint["restore_bytes"]:
            if terminal.error:
                raise AssertionError(terminal.error)
            terminal.changed.clear()
            await terminal.changed.wait()
    await runtime.ledger.measure("terminal_seed", seed_complete(), run=run["id"])
    terminal_facts = {"header": continuation, "seed_bytes": terminal.seed_bytes,
                      "seed_sha256": terminal.seed_hash.hexdigest(),
                      "claim_scope": "seed identity, geometry and streaming; grid semantics need codec/consumer tests"}
    await terminal.close()
    a.oracle.expect(b"INT interactive\n")
    await runtime.input(run, b"\x03")
    await a.wait_bytes()
    await runtime.probe(run, a, 2)
    child = (await runtime.ledger.measure("fork_level_a", runtime.protocol.request({"type": "fork", "parent": run["id"],
             "plan": {"type": "level_a"}, "operation_key": str(uuid.uuid4())})))["run"]
    runtime.runs.append(child)
    if child["spec"] != run["spec"] or child["id"] == run["id"] or child["pid"] == run["pid"]:
        raise AssertionError("Level A did not clone the specification into an independent real child")
    fork_view = await View(runtime.protocol, child, f"READY interactive {child['pid']}\n".encode(), runtime.args.timeout).open()
    runtime.views.append(fork_view)
    await fork_view.wait_bytes()
    await runtime.probe(child, fork_view, 7, True)
    await runtime.probe(other, b, 7)
    fork_facts = fork_view.facts()
    await fork_view.close()
    cli_samples = []
    for sequence in range(runtime.args.cli_samples):
        async def cli_status():
            process = await asyncio.create_subprocess_exec(str(runtime.args.cli), "--socket", str(runtime.socket), "status", run["id"],
                                                           stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            if process.returncode or stdout.decode().split("\t", 1)[0] != run["id"]:
                raise AssertionError("CLI status failed: " + repr((process.returncode, stdout, stderr)))
            return {"stdout": stdout.decode(), "stderr": stderr.decode()}
        cli_samples.append(await runtime.ledger.measure("cli_status", cli_status(), run=run["id"]))
    # Recoverable input response lost by the caller, then rejoin with same key.
    status = (await runtime.protocol.request({"type": "status", "id": run["id"]}))["run"]
    operation = {"daemon_instance": runtime.identity["daemonInstanceId"], "operation_key": str(uuid.uuid4()),
                 "id": run["id"], "expected_byte": status["applied_input_bytes"], "data": list(b"P 3\n")}
    a.oracle.expect(b"P interactive 3\n")
    reader, writer, _ = await runtime.protocol.connect()
    await runtime.protocol.send(writer, {"type": "request", "request": {"type": "recoverable_input", "operation": operation}})
    writer.close()
    await writer.wait_closed()
    recovered = await runtime.ledger.measure("recoverable_input", runtime.protocol.request({"type": "recoverable_input", "operation": operation}))
    await a.wait_bytes()
    repeated = await runtime.protocol.request({"type": "recoverable_input", "operation": operation})
    if recovered["range"] != repeated["range"] or recovered["range"]["end_byte"] - recovered["range"]["start_byte"] != 4:
        raise AssertionError("recovered input range changed")
    # A real blocked writer must be observed before testing its neighbor.
    blocked, held = await runtime.start("blocked", True)
    held.oracle.expect(b"H blocked\n")
    await runtime.input(blocked, f"H {int(runtime.args.timeout * 2)}\n".encode())
    await held.wait_bytes()
    writes = [asyncio.create_task(runtime.input(blocked, b"a" * 131072)) for _ in range(4)]
    runtime.background.extend(writes)
    async def observe_blocked():
        while True:
            status = (await runtime.protocol.request({"type": "status", "id": blocked["id"]}))["run"]
            if status["native_service"]["input"]["write_blocked"]:
                return status
            await asyncio.sleep(0.01)
    blocked_status = await runtime.ledger.measure("writer_blocked_observed", observe_blocked())
    await runtime.probe(other, b, 2, True)
    await runtime.probe(run, a, 4)
    held.oracle.expect(b"INT blocked\n")
    await runtime.ledger.measure("interrupt_blocked", runtime.protocol.request({"type": "signal", "id": blocked["id"], "signal": "interrupt"}))
    write_results = await asyncio.gather(*writes, return_exceptions=True)
    await held.wait_bytes()
    blocked_after = (await runtime.protocol.request({"type": "status", "id": blocked["id"]}))["run"]
    # Separate fixture interpretation from the exact PTY writer settlement.
    blocked_facts = {"before": blocked_status, "after": blocked_after,
                     "write_results": [repr(x) if isinstance(x, BaseException) else x for x in write_results]}
    # Frozen open-loop schedule: tasks are dispatched independent of completion.
    schedule, inflight = [], []
    begin = time.monotonic()
    # A hot Run generates work concurrently with probes of its quiet neighbor.
    flood_stop = asyncio.Event()
    async def flood():
        turn = 0
        while turn < runtime.args.flood_turns or not flood_stop.is_set():
            data = burst_output(other, runtime.args.fanout_bytes, turn)
            b.oracle.expect(data)
            await runtime.input(other, f"B {runtime.args.fanout_bytes} {turn}\n".encode())
            await b.wait_bytes()
            turn += 1
        return turn
    flood_task = asyncio.create_task(flood())
    runtime.background.append(flood_task)
    async def scheduled_probe(sequence, intended):
        sent = time.monotonic()
        outcome = "completed"
        try:
            await runtime.ledger.measure("open_loop_status", runtime.protocol.request({"type": "status", "id": run["id"]}),
                                         intended=intended, sent=sent)
            await runtime.probe(run, a, 1000 + sequence, via_attachment=True)
        except Exception as error:
            outcome = repr(error)
        schedule.append({"sequence": sequence, "intended": intended, "sent": sent,
                         "schedule_late_ms": max(0, (sent - intended) * 1000), "end": time.monotonic(), "outcome": outcome})
    for i in range(runtime.args.open_loop_samples):
        intended = begin + i / runtime.args.open_loop_rate
        await asyncio.sleep(max(0, intended - time.monotonic()))
        inflight.append(asyncio.create_task(scheduled_probe(i, intended)))
        runtime.background.append(inflight[-1])
    await asyncio.gather(*inflight)
    flood_stop.set()
    flood_turns = await flood_task
    save(runtime.directory / "open-loop.json", {"rate_per_second": runtime.args.open_loop_rate, "samples": schedule})
    if any(row["outcome"] != "completed" for row in schedule):
        raise AssertionError("open-loop attempts failed; successful latency samples do not qualify this scene")
    fanout = []
    for clients in [1, 8, 32]:
        views = [await View(runtime.protocol, other, b.oracle.expected, runtime.args.timeout, b.oracle.cursor).open() for _ in range(clients)]
        runtime.views.extend(views)
        data = burst_output(other, runtime.args.fanout_bytes, clients)
        b.oracle.expect(data)
        for view in views:
            view.oracle.expect(data)
        started = time.monotonic()
        await runtime.input(other, f"B {runtime.args.fanout_bytes} {clients}\n".encode())
        await asyncio.gather(b.wait_bytes(), *(v.wait_bytes() for v in views))
        fanout.append({"clients": clients, "elapsed_seconds": time.monotonic() - started,
                       "payload_bytes": runtime.args.fanout_bytes, "verified_wire_bytes": len(data),
                       "views": [v.facts() for v in views]})
        for view in views:
            await view.close()
    echo, echo_view = await runtime.start("echo", True)
    echo_view.oracle.expect(f"E echo {runtime.args.input_bytes}\n".encode())
    await runtime.input(echo, f"E {runtime.args.input_bytes}\n".encode())
    await echo_view.wait_bytes()
    binary_input = expected_burst(runtime.args.input_bytes, 73)
    echo_view.oracle.expect(binary_input + b"E_DONE echo\n")
    async def echo_binary_input():
        for offset in range(0, len(binary_input), runtime.args.input_chunk_bytes):
            await runtime.input(echo, binary_input[offset:offset+runtime.args.input_chunk_bytes])
        await echo_view.wait_bytes()
    began = time.monotonic()
    await runtime.ledger.measure("binary_input_echo_verified", echo_binary_input(), run=echo["id"])
    binary_echo = {"payload_bytes": len(binary_input), "seconds": time.monotonic()-began,
                   "payload_sha256": hashlib.sha256(binary_input).hexdigest(), "view": echo_view.facts(),
                   "claim_scope": "byte-exact child echo plus PTY-applied receipts; fixture and observer costs included"}
    echo_view.oracle.expect(b"INT echo\n")
    await runtime.input(echo, b"\x03")
    await echo_view.wait_bytes()
    sdk = {"outcome": "not_executed", "reason": "No selected built SDK module provided"}
    terminal_cli = await cli_terminal(runtime)
    if runtime.args.sdk_module:
        async def sdk_consumer():
            process = await asyncio.create_subprocess_exec("node", str(Path(__file__).with_name("sdk_consumer.mjs")),
                str(runtime.args.sdk_module), str(runtime.socket), str(runtime.args.fixture),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                stdout, stderr = await process.communicate()
            finally:
                if process.returncode is None:
                    process.terminate()
                    await process.wait()
            (runtime.directory / "sdk.stdout").write_bytes(stdout)
            (runtime.directory / "sdk.stderr").write_bytes(stderr)
            result = json.loads(stdout)
            if process.returncode or result["outcome"] != "completed":
                raise AssertionError("SDK consumer failed: " + repr(result))
            return result
        sdk = await runtime.ledger.measure("sdk_consumer", sdk_consumer())
    recovery = {"outcome": "not_applicable", "reason": "memory-only does not promise cold recovery"}
    if runtime.mode == "persistent":
        recovery = await persistent_recovery(runtime, [a, b])
    return {"outcome": "completed", "two_run_views": [a.facts(), b.facts()], "fanout": fanout,
            "terminal_seed": terminal_facts, "level_a_fork": fork_facts,
            "cli_terminal": terminal_cli,
            "binary_input_echo": binary_echo,
            "sdk": sdk, "persistent_recovery": recovery,
            "cli_status_samples": cli_samples, "flood_turns": flood_turns,
            "blocked_writer": blocked_facts,
            "recoverable_input": {"first_range": recovered["range"], "repeat_range": repeated["range"]},
            "limitations": ["raw byte and owner availability oracle; terminal grid rendering is separately qualified",
                            "caller closed before result; exact server response-loss timing is not controlled"]}


async def persistent_recovery(runtime, views):
    before = {"runtime": runtime.identity, "pid": runtime.process.pid,
              "listener_inode": runtime.socket.stat().st_ino,
              "argv": process_argv(runtime.process.pid),
              "runs": [{"id": v.run["id"], "pid": v.run["pid"], "cursor": v.oracle.cursor} for v in views]}
    if "--handoff-fd" in before["argv"]:
        raise AssertionError("upgrade observation must begin with the original image")
    for view in runtime.views:
        await view.close()
    os.kill(runtime.process.pid, signal.SIGHUP)
    async def rejoin_upgrade():
        while True:
            try:
                if "--handoff-fd" not in process_argv(runtime.process.pid):
                    await asyncio.sleep(0.01)
                    continue
                listing = await runtime.protocol.listing()
                if {v.run["id"] for v in views} <= {r["id"] for r in listing}:
                    return listing
            except (ConnectionError, EOFError, FileNotFoundError):
                pass
            await asyncio.sleep(0.01)
    await runtime.ledger.measure("planned_exec_rejoin", rejoin_upgrade())
    if runtime.socket.stat().st_ino != before["listener_inode"] or runtime.process.pid != before["pid"]:
        raise AssertionError("planned exec replaced listener/daemon identity")
    after_upgrade = []
    resumed_views = []
    for i, view in enumerate(views):
        status = (await runtime.protocol.request({"type": "status", "id": view.run["id"]}))["run"]
        if status["pid"] != view.run["pid"] or status["native_service"]["owner"]["type"] != "serving":
            raise AssertionError("upgrade did not preserve real child/service")
        resumed = await View(runtime.protocol, status, view.oracle.expected, runtime.args.timeout, view.oracle.cursor).open()
        runtime.views.append(resumed)
        await runtime.probe(status, resumed, 9000 + i, True)
        resumed_views.append(resumed)
        after_upgrade.append(status)
    views = resumed_views
    async def durable():
        while True:
            rows = [(await runtime.protocol.request({"type": "status", "id": v.run["id"]}))["run"] for v in views]
            if all(r["durable_output_bytes"] == len(v.oracle.expected) for r, v in zip(rows, views)):
                return rows
            await asyncio.sleep(0.01)
    durable_rows = await runtime.ledger.measure("durable_output_fence", durable())
    for view in runtime.views:
        await view.close()
    # Deliberate crash of this private daemon only; no live PTY adoption claim.
    runtime.sampling.cancel()
    await asyncio.gather(runtime.sampling, return_exceptions=True)
    runtime.process.kill()
    await runtime.process.wait()
    runtime.stdout.close()
    runtime.stderr.close()
    await runtime.frames.close()
    save(runtime.directory / f"recorder-{runtime.epoch}.private.json", runtime.frames.facts())
    runtime.epoch += 1
    await runtime.open()
    if runtime.identity["runtimeId"] != before["runtime"]["runtimeId"] or runtime.identity["daemonInstanceId"] == before["runtime"]["daemonInstanceId"]:
        raise AssertionError("cold replacement has incorrect runtime/incarnation identity")
    historical = []
    for view, row in zip(views, durable_rows):
        status = (await runtime.protocol.request({"type": "status", "id": view.run["id"]}))["run"]
        if status["pid"] is not None or status["state"]["type"] != "interrupted":
            raise AssertionError("cold restart invented live process ownership")
        if status["latest_output_bytes"] != row["durable_output_bytes"]:
            raise AssertionError("cold replay lost its committed output fence")
        floor = status["first_available_byte"]
        replay = await View(runtime.protocol, status, view.oracle.expected, runtime.args.timeout, floor).open()
        runtime.views.append(replay)
        await replay.wait_bytes()
        historical.append({"status": status, "replay": replay.facts(), "intentional_retention_floor": floor})
    receipt = {"outcome": "completed", "before": before, "after_upgrade": after_upgrade,
               "exec_observed_by": "same PID argv changed to inherited --handoff-fd before protocol rejoin",
               "durable_before_crash": durable_rows, "after_cold_runtime": runtime.identity,
               "historical": historical, "power_loss_qualified": False}
    save(runtime.directory / "recovery.private.json", receipt)
    return receipt


async def campaign(args):
    args.output.mkdir(parents=True, exist_ok=False)
    nofile_before = resource.getrlimit(resource.RLIMIT_NOFILE)
    nofile_raise_error = None
    if nofile_before[1] != resource.RLIM_INFINITY and nofile_before[0] < nofile_before[1]:
        try:
            # Fund this observer from its existing process-local host allowance.
            # Neither host-global limits nor daemon policy are changed.
            resource.setrlimit(resource.RLIMIT_NOFILE, (nofile_before[1], nofile_before[1]))
        except OSError as error:
            nofile_raise_error = repr(error)
    source_root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=args.source_dir, text=True).strip()).resolve()
    if source_root != args.source_dir:
        raise ValueError("source-dir must be its actual Git checkout root; parent Git traversal is not source provenance")
    dirty = subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=args.source_dir)
    (args.output / "selected-source.diff").write_bytes(dirty)
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=args.source_dir).decode().split("\0")
    plan = {"schema": "ctxmux.run-benchmark.v1", "parameters": vars(args).copy(),
            "host_constraints_visibility": "container snapshot; ancestors need separate attestation",
            "source": {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.source_dir, text=True).strip(),
                       "tree": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=args.source_dir, text=True).strip()},
            "cpu_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "generator_nofile": resource.getrlimit(resource.RLIMIT_NOFILE),
            "generator_nofile_before": nofile_before, "generator_nofile_raise_error": nofile_raise_error,
            "source_dirty_diff_sha256": hashlib.sha256(dirty).hexdigest(),
            "source_untracked_sha256": {p: sha(args.source_dir / p) for p in untracked if p},
            "files": {"daemon": sha(args.daemon), "cli": sha(args.cli), "fixture": sha(args.fixture),
                      "harness": sha(Path(__file__)), "metrics": sha(Path(__file__).with_name("metrics.py")),
                      "sdk_consumer": sha(Path(__file__).with_name("sdk_consumer.mjs"))},
            "cells": []}
    plan["parameters"] = {k: str(v) if isinstance(v, Path) else v for k, v in plan["parameters"].items()}
    plan["files"]["frame_recorder"] = sha(Path(__file__).with_name("frame_recorder.py"))
    if args.sdk_module:
        plan["sdk_emitted_files"] = {str(p.relative_to(args.sdk_module.parent)): sha(p)
                                     for p in args.sdk_module.parent.rglob("*") if p.is_file()}
    for mode in args.modes:
        plan["cells"].append({"id": mode + "-scene", "mode": mode, "kind": "scene"})
        for repeat in range(args.rounds):
            for tier in args.tiers:
                plan["cells"].append({"id": f"{mode}-{tier}-{repeat}", "mode": mode, "kind": "fleet", "tier": tier,
                    "repeat": repeat, "soak": args.soak_seconds if mode == "persistent" and repeat == args.rounds - 1 and tier == max(args.tiers) else 0})
    if args.frontier:
        plan["cells"].append({"id": "default-frontier", "mode": "memory", "kind": "fleet", "tier": args.frontier, "soak": 0})
    save(args.output / "plan.private.json", plan)
    results = []
    for index, cell in enumerate(plan["cells"]):
        directory = args.output / cell["id"]
        directory.mkdir()
        runtime = Runtime(args, directory, cell["mode"])
        result = dict(cell)
        result["host_before"] = host_sample()
        result["comparison_qualified"] = False
        result["noise_scope"] = "observational baseline; host noise retained, no frozen comparative noise gate"
        try:
            await runtime.open()
            result.update(await (scene(runtime) if cell["kind"] == "scene" else fleet(runtime, cell["tier"], cell["soak"])))
        except Exception as error:
            result.update(outcome="failed", error=repr(error))
        finally:
            # A slow or failed Stop must not keep completed byte oracles only
            # in generator memory until a Job deadline destroys the process.
            # This snapshot cannot qualify cleanup or the whole cell.
            try:
                runtime.save_workload_snapshot(result)
            except Exception as error:
                result.update(outcome="failed", snapshot_error=repr(error))
            try:
                result["cleanup"] = await runtime.close()
            except Exception as error:
                result["cleanup"] = {"error": repr(error)}
                result["outcome"] = "failed"
            result["host_after"] = host_sample()
            save(directory / "result.private.json", result)
            results.append(result)
            save(args.output / "results.private.json", results)
            print(json.dumps({k: result.get(k) for k in ["id", "outcome", "error", "accepted"]}), flush=True)
        cleanup = result["cleanup"]
        cleanup_bad = any(cleanup.get(key) for key in ["remaining_children", "error", "list_error", "forced", "census_errors", "sampling_errors"])
        cleanup_bad = cleanup_bad or any(row["outcome"] != "removed" for row in cleanup.get("attempts", []))
        if cleanup_bad:
            result["outcome"] = "failed"
            save(directory / "result.private.json", result)
            save(args.output / "results.private.json", results)
            for pending in plan["cells"][index + 1:]:
                results.append({**pending, "outcome": "blocked_precondition", "not_started": True,
                                "reason": "previous private cell cleanup is unqualified", "blocked_by": cell["id"]})
            save(args.output / "results.private.json", results)
            break
    save(args.output / "campaign.private.json", {"results": results, "selected_cells": len(plan["cells"]),
                                              "cells_with_dispositions": len(results),
                                              "executed_cells": sum(not r.get("not_started", False) for r in results),
                                              "selected_cells_pass": all(r["outcome"] == "completed" for r in results)})
    return 0 if all(r["outcome"] == "completed" for r in results) else 1


def parse():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["daemon", "cli", "fixture", "source-dir", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--tiers", type=int, nargs="+", default=[128, 512, 2048, 4000])
    parser.add_argument("--modes", nargs="+", choices=["memory", "persistent"], default=["memory", "persistent"])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--idle-seconds", type=float, default=2)
    parser.add_argument("--active-bytes", type=int, default=4096)
    parser.add_argument("--token-count", type=int, default=20)
    parser.add_argument("--token-interval-us", type=int, default=50000)
    parser.add_argument("--fanout-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--input-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--input-chunk-bytes", type=int, default=131072,
                        help="caller chunk size; NDJSON frame integrity is checked separately")
    parser.add_argument("--open-loop-samples", type=int, default=300)
    parser.add_argument("--open-loop-rate", type=float, default=50)
    parser.add_argument("--soak-seconds", type=float, default=1800)
    parser.add_argument("--soak-probe-interval", type=float, default=1)
    parser.add_argument("--soak-streams", type=int, default=0, help="zero selects one quarter of the existing fleet")
    parser.add_argument("--flood-turns", type=int, default=16)
    parser.add_argument("--cli-samples", type=int, default=30)
    parser.add_argument("--sdk-module", type=Path)
    parser.add_argument("--frontier", type=int, default=8192)
    parser.add_argument("--resource-limits", type=json.loads, help="explicit daemon operating policy as JSON; absent preserves defaults")
    args = parser.parse_args()
    if args.resource_limits is not None and not isinstance(args.resource_limits, dict):
        parser.error("resource-limits must be a JSON object")
    for name in ["daemon", "cli", "fixture", "source_dir", "output"]:
        setattr(args, name, getattr(args, name).resolve())
    if args.sdk_module:
        args.sdk_module = args.sdk_module.resolve()
    positive = ["concurrency", "rounds", "timeout", "open_loop_rate", "sample_interval", "active_bytes",
                "token_count", "token_interval_us", "fanout_bytes", "input_bytes", "input_chunk_bytes", "open_loop_samples", "soak_probe_interval", "flood_turns", "cli_samples"]
    if any(t <= 0 for t in args.tiers) or any(getattr(args, key) <= 0 for key in positive) or any(
            getattr(args, key) < 0 for key in ["soak_seconds", "soak_streams", "idle_seconds", "frontier"]):
        parser.error("positive workload values required")
    return args


if __name__ == "__main__":
    raise SystemExit(asyncio.run(campaign(parse())))
