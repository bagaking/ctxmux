# 006 — Rust schema and TypeScript code generation

- Status: accepted
- Scope: cross-language wire authority and drift detection

## Context

The Rust daemon and TypeScript SDK must not maintain parallel handwritten protocol schemas. Compile-time TypeScript declarations should follow the same serde-tagged types the daemon encodes.

## Decision

Rust types in `ctxmux-protocol` are authoritative. `ts-rs` exports the recursive `ClientFrame` and `ServerFrame` graph plus generated protocol constants. The generator formats output with Prettier. The repository gate regenerates into a temporary directory and diffs it against checked-in declarations.

Generated declarations provide static parity. The SDK separately validates full generation-1 frames at runtime and rejects unsafe integer cursors. Protocol compatibility policy remains a separate responsibility.

## Quality attributes and invariants

- Generated TypeScript wire files are never hand-edited.
- The protocol generation and frame limit constants come from Rust.
- CI fails when Rust types and committed declarations drift.
- Serialization tags remain visible in both languages.

## Alternatives

- Handwritten TypeScript types create silent schema forks.
- An independent IDL could become authoritative later, but would add a third representation now.
- Runtime JSON Schema generation could validate untrusted frames but does not replace static client types or compatibility decisions.

## Known constraints

`ts-rs` is configured to emit Rust large integers as TypeScript `number`. The SDK now fails closed above `2^53 - 1`, but the wire cannot represent a larger exact cursor for TypeScript clients. Type generation does not produce Rust-authored golden frames or require a protocol-version change when a wire shape changes.

Serde attributes unsupported or interpreted differently by the generator remain a cross-language risk.

## Wrong-case corpus

Evidence pack: [schema-codegen track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/schema-codegen.md), claim `C006`.

- `SC-01` (`f01`-`f03`): current `.with_large_int("number")` silently rounds a `u64` cursor above `2^53 - 1`. The SDK must preserve it exactly or reject it before replay.
- `SC-02` (`f02`, `f04`): TypeScript declarations are erased. A known top-level tag with malformed nested fields currently crosses `serverFrame` through a cast.
- `SC-03` (`f02`): unsupported or suppressed serde attributes can make serialization and declarations diverge even when generated text looks plausible. Golden Rust frames need TypeScript runtime validation.

The generated-directory diff remains valuable. It solves checked-in declaration drift, not runtime JSON validation, large-integer representation, or protocol-version policy.

## Fixture mapping

- Covered now: generated-directory drift in `scripts/check-protocol-types.sh`.
- Active: cursor values at and above `Number.MAX_SAFE_INTEGER` fail exact-or-rejected.
- Active: all server variants and nested mutation frames pass through the runtime validator.
- Active on each side: duplicate names and malformed frames fail before typed exposure.
- Future: Rust-authored golden frames decoded by TypeScript and a schema-change generation-bump gate.

## Open questions

- Should `u64` values become strings, `bigint` adapters, or bounded protocol integers?
- Which changes are wire-breaking and how is the generation-bump gate enforced?
- Does generation 2 need a runtime schema or only handwritten boundary validation?
- How are golden wire fixtures versioned without preserving obsolete pre-stable contracts?

## Repository evidence

- `crates/ctxmux-protocol/src/lib.rs`
- `crates/ctxmux-protocol/src/bin/export-types.rs`
- `packages/sdk/src/generated/`
- `scripts/generate-protocol-types.sh`
- `scripts/check-protocol-types.sh`
