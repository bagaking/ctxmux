import { setTimeout as delay } from "node:timers/promises";

export interface GcRunIdentity {
  readonly mode: string;
  readonly index: number;
  readonly operation_key: string;
}

interface GcRunClient<Run> {
  readonly start: () => Promise<Run>;
  readonly status: (id: string) => Promise<GcRunStatus>;
}

interface GcRunStatus {
  readonly state: { readonly type: string } & Readonly<Record<string, unknown>>;
}

export async function startAndWaitForGcRunExit<Run extends { id: string }>(
  client: GcRunClient<Run>,
  identity: GcRunIdentity,
  phaseDeadline: number,
  now: () => number = Date.now,
): Promise<Run> {
  const run = await beforeGcPhaseDeadline(
    () => client.start(),
    { ...identity, run_id: "not_observed" },
    "start",
    "not_observed",
    phaseDeadline,
    now,
  );
  await waitForGcRunExit(
    client,
    { ...identity, run_id: run.id },
    phaseDeadline,
    now,
  );
  return run;
}

export async function waitForGcRunExit(
  client: Pick<GcRunClient<never>, "status">,
  identity: GcRunIdentity & { readonly run_id: string },
  phaseDeadline: number,
  now: () => number = Date.now,
): Promise<void> {
  let lastState: Readonly<Record<string, unknown>> | "not_observed" =
    "not_observed";
  for (;;) {
    const remainingMs = phaseDeadline - now();
    if (remainingMs <= 0) break;
    const run: GcRunStatus = await beforeGcPhaseDeadline(
      () => client.status(identity.run_id),
      identity,
      "status",
      lastState,
      phaseDeadline,
      now,
    );
    lastState = run.state;
    if (lastState.type !== "running") return;
    const pollDelayMs = Math.min(10, Math.max(0, phaseDeadline - now()));
    if (pollDelayMs === 0) break;
    await delay(pollDelayMs);
  }
  throw gcExitDeadlineError(
    identity,
    "status",
    lastState,
    phaseDeadline,
    now(),
  );
}

function gcExitDeadlineError(
  identity: GcRunIdentity & { readonly run_id: string },
  operation: "start" | "status",
  lastState: Readonly<Record<string, unknown>> | "not_observed",
  phaseDeadline: number,
  observedAt: number,
): Error {
  return new GcPhaseDeadlineError(
    `GC Run did not complete ${operation} inside its phase deadline: mode=${identity.mode} index=${String(identity.index)} operation_key=${identity.operation_key} run_id=${identity.run_id} last_state=${JSON.stringify(lastState)} phase_deadline_ms=${String(phaseDeadline)} observed_at_ms=${String(observedAt)} remaining_phase_ms=${String(Math.max(0, phaseDeadline - observedAt))}`,
  );
}

async function beforeGcPhaseDeadline<T>(
  operationPromise: () => Promise<T>,
  identity: GcRunIdentity & { readonly run_id: string },
  operation: "start" | "status",
  lastState: Readonly<Record<string, unknown>> | "not_observed",
  phaseDeadline: number,
  now: () => number,
): Promise<T> {
  const remainingMs = phaseDeadline - now();
  if (remainingMs <= 0) {
    throw gcExitDeadlineError(
      identity,
      operation,
      lastState,
      phaseDeadline,
      now(),
    );
  }
  let timer: NodeJS.Timeout | undefined;
  try {
    let value: T;
    try {
      value = await Promise.race([
        operationPromise(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(
                gcExitDeadlineError(
                  identity,
                  operation,
                  lastState,
                  phaseDeadline,
                  now(),
                ),
              ),
            remainingMs,
          );
        }),
      ]);
    } catch (error) {
      if (error instanceof GcPhaseDeadlineError) throw error;
      throw new Error(
        `GC Run ${operation} failed: mode=${identity.mode} index=${String(identity.index)} operation_key=${identity.operation_key} run_id=${identity.run_id}`,
        { cause: error },
      );
    }
    if (now() > phaseDeadline) {
      throw gcExitDeadlineError(
        identity,
        operation,
        lastState,
        phaseDeadline,
        now(),
      );
    }
    return value;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

class GcPhaseDeadlineError extends Error {}
