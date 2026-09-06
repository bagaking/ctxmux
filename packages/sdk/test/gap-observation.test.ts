import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import {
  Attachment,
  CtxmuxAttachmentObservationUnavailableError,
} from "../src/attachment.ts";
import { emptyGapCauses } from "../src/gap-observation.ts";
import {
  validateServerFrame,
  CtxmuxInvalidFrameError,
} from "../src/validation.ts";
import { PROTOCOL_VERSION } from "../src/generated/constants.ts";
import type {
  AttachedSnapshot,
  RuntimeIdentity,
  ServerFrame,
} from "../src/index.ts";
import type { AttachmentGapEvent } from "../src/gap-observation.ts";

const runId = "00000000-0000-4000-8000-000000000001";
const runtime: RuntimeIdentity = {
  runtimeId: "00000000-0000-4000-8000-000000000002",
  daemonInstanceId: "00000000-0000-4000-8000-000000000003",
  runtimeIdPersistence: "daemon",
  buildId: "fixture",
  protocolGeneration: PROTOCOL_VERSION,
  platform: "fixture",
  arch: "fixture",
  capabilities: {},
};
function snapshot(): AttachedSnapshot {
  return {
    run: {
      id: runId,
      spec: {
        program: "fixture",
        args: [],
        cwd: null,
        env: {},
        initial_size: { rows: 24, cols: 80 },
        declared_inputs: [],
      },
      lineage: null,
      backend: { type: "native" },
      capabilities: {
        input: true,
        resize: true,
        signal: true,
        stop: true,
        fork_level_a: true,
        fork_level_b: true,
        replay: "raw_from_start",
      },
      pid: 42,
      state: { type: "running" },
      native_service: {
        revision: 0,
        owner: { type: "serving" },
        output: { type: "serving" },
        input: {
          phase: { type: "open" },
          unsettled_commands: 0,
          unsettled_request_bytes: 0,
          write_blocked: false,
          completed_input_bytes: 0,
          active_confirmed_bytes: 0,
          current_size: { rows: 24, cols: 80 },
        },
        terminal_fault: null,
      },
      latest_output_bytes: 0,
      durable_output_bytes: null,
      first_available_byte: 0,
      attachments: 1,
      applied_input_bytes: 0,
      current_size: { rows: 24, cols: 80 },
    },
    replay: {
      chunks: [],
      first_available_byte: 0,
      latest_output_bytes: 0,
      truncated: false,
    },
    terminal: { type: "not_requested" },
    terminal_restore: new Uint8Array(),
    resize_revision: 0,
  };
}

class ControlledWire {
  readonly frames: unknown[] = [];
  readonly sent: any[] = [];
  waiter: ((value: unknown) => void) | undefined;
  async send(value: unknown): Promise<void> {
    this.sent.push(value);
  }
  async sendEncoded(value: string): Promise<void> {
    this.sent.push(JSON.parse(value));
  }
  async receive(): Promise<unknown> {
    if (this.frames.length !== 0) return this.frames.shift();
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }
  close(): void {
    this.waiter?.(undefined);
    this.waiter = undefined;
  }
  push(frame: ServerFrame): void {
    if (this.waiter !== undefined) {
      const waiter = this.waiter;
      this.waiter = undefined;
      waiter(frame);
    } else this.frames.push(frame);
  }
  output(start: number, count: number): void {
    this.push({
      type: "event",
      event: {
        type: "output",
        chunk: {
          start_byte: start,
          end_byte: start + count,
          data: new Uint8Array(count),
        },
      },
    });
  }
  gap(head: number, cause: keyof ReturnType<typeof emptyGapCauses>): void {
    this.push({
      type: "event",
      event: {
        type: "gap",
        latest_output_bytes: head,
        causes: { ...emptyGapCauses(), [cause]: true },
      },
    });
  }
  ackInput(bytes: number): void {
    const command = this.sent.findLast((frame) => frame.type === "input");
    assert.ok(command !== undefined);
    this.push({
      type: "command_result",
      command_id: command.command_id,
      outcome: {
        type: "accepted",
        receipt: { type: "input", written_bytes: bytes },
      },
    });
  }
}

async function nextGap(attachment: Attachment): Promise<AttachmentGapEvent> {
  const event = await attachment.nextEvent();
  assert.equal(event?.type, "gap");
  assert.ok(event?.type === "gap");
  return event;
}

test("Gap causes are required, exact and explicitly unknown when unclassified", () => {
  const good = {
    type: "event",
    event: {
      type: "gap",
      latest_output_bytes: 7,
      causes: { ...emptyGapCauses(), unknown: true },
    },
  };
  assert.equal(validateServerFrame(good), good);
  for (const causes of [
    undefined,
    emptyGapCauses(),
    [],
    { ...good.event.causes, unknown: 1 },
    { ...good.event.causes, guessed_failure: true },
  ]) {
    assert.throws(
      () => validateServerFrame({ ...good, event: { ...good.event, causes } }),
      CtxmuxInvalidFrameError,
    );
  }
});

test("daemon Gap distinguishes reported head from received and delivered bytes", async () => {
  const wire = new ControlledWire();
  const attachment = new Attachment(wire, snapshot(), {}, runtime);
  wire.gap(7, "unknown");
  await setImmediate();
  const gap = await nextGap(attachment);
  assert.equal(gap.latest_output_bytes, 7);
  assert.deepEqual(gap.observation.origins, { daemon: true, client: false });
  assert.equal(gap.causes.unknown, true);
  assert.deepEqual(gap.observation.runtime, {
    runtimeId: runtime.runtimeId,
    daemonInstanceId: runtime.daemonInstanceId,
  });
  assert.equal(gap.observation.runId, runId);
  assert.equal(gap.observation.receivedOutputHeadByte, null);
  assert.equal(gap.observation.deliveredThroughByte, null);
  assert.equal(gap.observation.requestedAfterByte, 0);
  assert.equal(gap.observation.recoveryAfterByte, 0);
  assert.equal(gap.observation.missingOutputBytes, null);
  assert.equal(gap.observation.localPressure, null);
  assert.ok(gap.observation.deliveredAtUnixMs !== null);
  assert.equal(
    wire.sent.length,
    0,
    "observing Gap must not resend input or restart anything",
  );
  attachment.close();
});

for (const mode of [
  "fully_truncated",
  "at_head",
  "future_cursor",
  "checkpoint_only",
] as const) {
  test(`empty ${mode} replay never turns an advertised head into an Output receipt`, async () => {
    const wire = new ControlledWire();
    const initial = snapshot();
    const afterByte =
      mode === "at_head" ? 7 : mode === "future_cursor" ? 11 : 0;
    initial.replay = {
      chunks: [],
      first_available_byte: 7,
      latest_output_bytes: 7,
      truncated: mode === "fully_truncated",
    };
    initial.run.latest_output_bytes = 7;
    initial.run.first_available_byte = 7;
    if (mode === "checkpoint_only") {
      initial.terminal_restore = new Uint8Array([27, 91, 72]);
      initial.terminal = {
        type: "basic_vt",
        checkpoint: {
          run_id: runId,
          through_byte: 7,
          resize_revision: 0,
          size: { rows: 24, cols: 80 },
          restore_size: { rows: 24, cols: 80 },
          restore_bytes: 3,
          restore_scrollback_rows: null,
          resize_after_restore_bytes: 0,
        },
        resizes: [],
      };
    }
    const attachment = new Attachment(wire, initial, {}, runtime, afterByte);
    wire.gap(7, "unknown");
    await setImmediate();
    const gap = await nextGap(attachment);
    assert.equal(gap.observation.receivedOutputHeadByte, null);
    assert.equal(gap.observation.deliveredThroughByte, null);
    assert.equal(gap.observation.requestedAfterByte, afterByte);
    assert.equal(gap.observation.recoveryAfterByte, afterByte);
    assert.equal(gap.observation.missingOutputBytes, null);
    attachment.close();
  });
}

test("original replay proves its available suffix independently of requested and owner heads", async () => {
  const wire = new ControlledWire();
  const initial = snapshot();
  initial.replay = {
    chunks: [{ start_byte: 3, end_byte: 7, data: new Uint8Array(4) }],
    first_available_byte: 3,
    latest_output_bytes: 7,
    truncated: true,
  };
  const attachment = new Attachment(wire, initial, {}, runtime, 1);
  wire.gap(9, "subscriber_lag");
  await setImmediate();
  const gap = await nextGap(attachment);
  assert.equal(gap.observation.receivedOutputHeadByte, 7);
  assert.equal(gap.observation.deliveredThroughByte, 7);
  assert.equal(gap.observation.requestedAfterByte, 1);
  assert.equal(gap.observation.recoveryAfterByte, 7);
  assert.equal(attachment.snapshot.replay.truncated, true);
  attachment.close();
});

for (const order of ["daemon_then_client", "client_then_daemon"] as const) {
  test(`mixed Gap keeps both origins in ${order}`, async () => {
    const wire = new ControlledWire();
    const attachment = new Attachment(
      wire,
      snapshot(),
      { payloadBytes: 1 },
      runtime,
    );
    if (order === "daemon_then_client") wire.gap(2, "source_discontinuity");
    wire.output(0, 4);
    if (order === "client_then_daemon") wire.gap(2, "source_discontinuity");
    await setImmediate();
    const gap = await nextGap(attachment);
    assert.equal(gap.latest_output_bytes, 4);
    assert.deepEqual(gap.observation.origins, { daemon: true, client: true });
    assert.equal(gap.causes.source_discontinuity, true);
    assert.equal(gap.causes.client_view_pressure, true);
    assert.equal(gap.observation.missingOutputBytes, null);
    assert.equal(gap.observation.localPressure?.droppedOutputBytes, 4);
    assert.equal(gap.observation.localPressure?.payloadLimitHit, true);
    assert.equal(gap.observation.receivedOutputHeadByte, 4);
    assert.equal(gap.observation.recoveryAfterByte, 0);
    assert.equal(gap.observation.queue.payloadBudgetBytes, 1);
    attachment.close();
  });
}

test("local Gap measures discarded bytes and keeps the consumed prefix as recovery cursor", async () => {
  const wire = new ControlledWire();
  const attachment = new Attachment(
    wire,
    snapshot(),
    { payloadBytes: 4 },
    runtime,
  );
  wire.output(0, 4);
  wire.output(4, 5);
  await setImmediate();
  const prefix = await attachment.nextEvent();
  assert.equal(prefix?.type, "output");
  const gap = await nextGap(attachment);
  assert.deepEqual(gap.observation.origins, { daemon: false, client: true });
  assert.equal(gap.observation.missingOutputBytes, 5);
  assert.equal(gap.observation.queue.retainedPayloadBytes, 4);
  assert.equal(gap.observation.queue.payloadHighWaterBytes, 4);
  assert.equal(gap.observation.deliveredThroughByte, 4);
  assert.equal(gap.observation.recoveryAfterByte, 4);
  assert.equal(gap.latest_output_bytes, 9);
  wire.output(9, 1);
  await setImmediate();
  assert.equal((await attachment.nextEvent())?.type, "output");
  wire.gap(10, "subscriber_lag");
  await setImmediate();
  assert.equal(
    (await nextGap(attachment)).observation.recoveryAfterByte,
    4,
    "later output cannot erase an unresolved discontinuity",
  );
  attachment.close();
});

test("source, lag and geometry causes survive multi-Gap aggregation without fabricated loss length", async () => {
  const wire = new ControlledWire();
  const attachment = new Attachment(wire, snapshot(), {}, runtime);
  for (const cause of [
    "source_discontinuity",
    "subscriber_lag",
    "geometry_lag",
    "unknown",
  ] as const)
    wire.gap(0, cause);
  await setImmediate();
  const gap = await nextGap(attachment);
  for (const cause of [
    "source_discontinuity",
    "subscriber_lag",
    "geometry_lag",
    "unknown",
  ] as const)
    assert.equal(gap.causes[cause], true);
  assert.equal(gap.latest_output_bytes, 0);
  assert.equal(
    gap.observation.missingOutputBytes,
    null,
    "an unchanged head does not establish a zero-byte source loss",
  );
  attachment.close();
});

test("Gap diagnostics consume envelope funding and preserve issued input ACK under refusal", async () => {
  const wire = new ControlledWire();
  const attachment = new Attachment(
    wire,
    snapshot(),
    { payloadBytes: 1, envelopeBytes: 0 },
    runtime,
  );
  const pending = attachment.input("x");
  await setImmediate();
  wire.output(0, 2);
  wire.ackInput(1);
  const accepted = await pending;
  assert.equal(accepted.receipt.written_bytes, 1);
  await assert.rejects(
    attachment.nextEvent(),
    (error) =>
      error instanceof CtxmuxAttachmentObservationUnavailableError &&
      error.resource === "envelope_bytes" &&
      error.lostOutputBytes === 2,
  );
  attachment.close();
});

test("different attachments to the same Run retain distinct observation identities", async () => {
  const wires = [new ControlledWire(), new ControlledWire()];
  const attachments = wires.map(
    (wire) => new Attachment(wire, snapshot(), {}, runtime),
  );
  for (const wire of wires) wire.gap(1, "subscriber_lag");
  await setImmediate();
  const gaps = await Promise.all(attachments.map(nextGap));
  assert.notEqual(
    gaps[0]!.observation.attachmentId,
    gaps[1]!.observation.attachmentId,
  );
  assert.equal(gaps[0]!.observation.runId, gaps[1]!.observation.runId);
  for (const attachment of attachments) attachment.close();
});

test("live suffix after an empty truncated snapshot is received without inventing continuous delivery", async () => {
  const wire = new ControlledWire();
  const initial = snapshot();
  initial.replay = {
    chunks: [],
    first_available_byte: 7,
    latest_output_bytes: 7,
    truncated: true,
  };
  const attachment = new Attachment(wire, initial, {}, runtime, 0);
  wire.output(7, 2);
  await setImmediate();
  assert.equal((await attachment.nextEvent())?.type, "output");
  wire.gap(9, "unknown");
  await setImmediate();
  const gap = await nextGap(attachment);
  assert.equal(gap.observation.receivedOutputHeadByte, 9);
  assert.equal(gap.observation.deliveredThroughByte, null);
  assert.equal(gap.observation.recoveryAfterByte, 0);
  attachment.close();
});
