import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  closeSync,
  constants,
  fstatSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename } from "node:path";

export interface InodeIdentity {
  readonly dev: string;
  readonly ino: string;
}

export interface QualificationPreflight {
  readonly schema: "ctxmux.reliability-preflight.v3";
  readonly profile: string;
  readonly not_before: string;
  readonly invocation_nonce: string;
  readonly artifact_owner_identity: InodeIdentity;
  readonly preexisting_receipt_identity: InodeIdentity | null;
  readonly workload_contract: {
    readonly path: string;
    readonly sha256: string;
  };
  readonly workload_helper: {
    readonly path: string;
    readonly sha256: string;
  };
}

const PROFILES = new Set(["smoke", "nightly", "release", "observe"]);
const INVOCATION_NONCE_PATTERN = /^[0-9a-f]{64}$/u;
const OWNER_COMPONENTS = (profile: string): readonly string[] => [
  "target",
  "reliability",
  profile,
];

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function sameMembers(
  left: readonly string[],
  right: readonly string[],
): boolean {
  return (
    left.length === right.length &&
    [...left].sort().every((value, index) => value === [...right].sort()[index])
  );
}

function identityOf(metadata: {
  readonly dev: number | bigint;
  readonly ino: number | bigint;
}): InodeIdentity {
  return { dev: String(metadata.dev), ino: String(metadata.ino) };
}

function sameIdentity(left: InodeIdentity, right: InodeIdentity): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}

function validIdentity(value: unknown): value is InodeIdentity {
  return (
    isObject(value) &&
    sameMembers(Object.keys(value), ["dev", "ino"]) &&
    [value.dev, value.ino].every(
      (item) => typeof item === "string" && /^\d+$/u.test(item),
    )
  );
}

function assertProfile(profile: string): void {
  if (!PROFILES.has(profile)) {
    throw new Error(`invalid reliability profile: ${profile}`);
  }
}

function assertBasename(name: string): void {
  if (
    name.length === 0 ||
    name === "." ||
    name === ".." ||
    basename(name) !== name
  ) {
    throw new Error("qualification artifact name must be one basename");
  }
}

function assertCurrentDirectory(expectedIdentity: InodeIdentity): void {
  const current = statSync(".", { bigint: true });
  if (
    !current.isDirectory() ||
    !sameIdentity(identityOf(current), expectedIdentity)
  ) {
    throw new Error("qualification artifact owner identity changed");
  }
}

function enterComponent(component: string, create: boolean): void {
  if (create) {
    try {
      mkdirSync(component, { mode: 0o700 });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
  }
  const descriptor = openSync(
    component,
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_DIRECTORY,
  );
  try {
    const opened = fstatSync(descriptor, { bigint: true });
    const named = lstatSync(component, { bigint: true });
    if (
      !opened.isDirectory() ||
      named.isSymbolicLink() ||
      !named.isDirectory() ||
      !sameIdentity(identityOf(opened), identityOf(named))
    ) {
      throw new Error(
        "qualification artifact owner component is not a direct directory",
      );
    }
    process.chdir(component);
    assertCurrentDirectory(identityOf(opened));
  } finally {
    closeSync(descriptor);
  }
}

export function enterCanonicalArtifactOwner({
  root,
  profile,
  expectedIdentity,
  create,
}: {
  readonly root: string;
  readonly profile: string;
  readonly expectedIdentity?: InodeIdentity;
  readonly create: boolean;
}): InodeIdentity {
  assertProfile(profile);
  const startingDirectory = statSync(".", { bigint: true });
  const rootDirectory = statSync(root, { bigint: true });
  if (
    !startingDirectory.isDirectory() ||
    !rootDirectory.isDirectory() ||
    !sameIdentity(identityOf(startingDirectory), identityOf(rootDirectory))
  ) {
    throw new Error(
      "qualification artifact traversal must start at the source root",
    );
  }
  for (const component of OWNER_COMPONENTS(profile)) {
    enterComponent(component, create);
  }
  const ownerIdentity = identityOf(statSync(".", { bigint: true }));
  if (
    expectedIdentity !== undefined &&
    !sameIdentity(ownerIdentity, expectedIdentity)
  ) {
    throw new Error("qualification artifact owner no longer matches preflight");
  }
  return ownerIdentity;
}

export function assertInheritedArtifactOwner(
  expectedIdentity: InodeIdentity,
): void {
  assertCurrentDirectory(expectedIdentity);
}

function openOwnedRegularFile(
  name: string,
  flags: number,
): {
  readonly fd: number;
  readonly identity: InodeIdentity;
} {
  assertBasename(name);
  const fd = openSync(name, flags | constants.O_NOFOLLOW);
  try {
    const opened = fstatSync(fd, { bigint: true });
    const named = lstatSync(name, { bigint: true });
    if (
      !opened.isFile() ||
      named.isSymbolicLink() ||
      !named.isFile() ||
      !sameIdentity(identityOf(opened), identityOf(named))
    ) {
      throw new Error("qualification artifact must be a direct regular file");
    }
    return { fd, identity: identityOf(opened) };
  } catch (error) {
    closeSync(fd);
    throw error;
  }
}

export function existingOwnedFileIdentity(name: string): InodeIdentity | null {
  try {
    const file = openOwnedRegularFile(name, constants.O_RDONLY);
    closeSync(file.fd);
    return file.identity;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export function readOwnedFile(name: string): {
  readonly bytes: Buffer;
  readonly identity: InodeIdentity;
  readonly sha256: string;
} {
  const file = openOwnedRegularFile(name, constants.O_RDONLY);
  try {
    const bytes = readFileSync(file.fd);
    return {
      bytes,
      identity: file.identity,
      sha256: createHash("sha256").update(bytes).digest("hex"),
    };
  } finally {
    closeSync(file.fd);
  }
}

export function readOwnedJson<T>(name: string): {
  readonly value: T;
  readonly identity: InodeIdentity;
  readonly sha256: string;
} {
  const file = readOwnedFile(name);
  return {
    value: JSON.parse(file.bytes.toString("utf8")) as T,
    identity: file.identity,
    sha256: file.sha256,
  };
}

export function writeOwnedJsonAtomically(name: string, value: unknown): void {
  assertBasename(name);
  const temporaryName = `${name}.tmp-${process.pid}-${randomUUID()}`;
  try {
    writeFileSync(temporaryName, `${JSON.stringify(value, null, 2)}\n`, {
      encoding: "utf8",
      flag: "wx",
      mode: 0o600,
    });
    renameSync(temporaryName, name);
  } finally {
    rmSync(temporaryName, { force: true });
  }
}

export function openFreshOwnedFile(name: string): number {
  assertBasename(name);
  return openSync(
    name,
    constants.O_WRONLY |
      constants.O_CREAT |
      constants.O_EXCL |
      constants.O_NOFOLLOW,
    0o600,
  );
}

export function createQualificationPreflight(
  profile: string,
  artifactOwnerIdentity: InodeIdentity,
  preexistingReceiptIdentity: InodeIdentity | null,
  workloadContract: { readonly path: string; readonly sha256: string },
  workloadHelper: { readonly path: string; readonly sha256: string },
): QualificationPreflight {
  assertProfile(profile);
  return {
    schema: "ctxmux.reliability-preflight.v3",
    profile,
    not_before: new Date().toISOString(),
    invocation_nonce: randomBytes(32).toString("hex"),
    artifact_owner_identity: artifactOwnerIdentity,
    preexisting_receipt_identity: preexistingReceiptIdentity,
    workload_contract: workloadContract,
    workload_helper: workloadHelper,
  };
}

export function parseQualificationPreflight(
  value: string | undefined,
  expectedProfile: string,
): QualificationPreflight {
  let preflight: unknown;
  try {
    preflight = JSON.parse(value ?? "");
  } catch {
    throw new Error("qualification preflight token must be valid JSON");
  }
  const priorIdentity = isObject(preflight)
    ? preflight.preexisting_receipt_identity
    : undefined;
  if (
    !isObject(preflight) ||
    !sameMembers(Object.keys(preflight), [
      "schema",
      "profile",
      "not_before",
      "invocation_nonce",
      "artifact_owner_identity",
      "preexisting_receipt_identity",
      "workload_contract",
      "workload_helper",
    ]) ||
    preflight.schema !== "ctxmux.reliability-preflight.v3" ||
    preflight.profile !== expectedProfile ||
    typeof preflight.not_before !== "string" ||
    !Number.isFinite(Date.parse(preflight.not_before)) ||
    typeof preflight.invocation_nonce !== "string" ||
    !INVOCATION_NONCE_PATTERN.test(preflight.invocation_nonce) ||
    !validIdentity(preflight.artifact_owner_identity) ||
    !(priorIdentity === null || validIdentity(priorIdentity)) ||
    !validFileIdentity(preflight.workload_contract) ||
    !validFileIdentity(preflight.workload_helper)
  ) {
    throw new Error(
      "qualification preflight token must bind the expected profile, invocation, owner, and prior receipt",
    );
  }
  return preflight as unknown as QualificationPreflight;
}

function validFileIdentity(value: unknown): boolean {
  return (
    isObject(value) &&
    sameMembers(Object.keys(value), ["path", "sha256"]) &&
    typeof value.path === "string" &&
    value.path.length > 0 &&
    !value.path.includes("\\") &&
    !value.path.split("/").includes("..") &&
    typeof value.sha256 === "string" &&
    /^[0-9a-f]{64}$/u.test(value.sha256)
  );
}
