# Feature plan revision 2 review

Status: approved by direct user instruction.

## T-002 confirmed minimum contract

```ts
type RuntimeIdentity = {
  daemonInstanceId: string;
  runtimeId: string;
  runtimeIdPersistence: "daemon" | "state_dir";
  buildId: string;
  protocolGeneration: number;
  platform: string;
  arch: string;
  capabilities: Record<string, number>;
};
```

- `runtimeIdPersistence` is `daemon` when the Runtime ID lasts for one
  memory-only daemon lifetime and `state_dir` when the state-directory owner
  preserves it across cold replacement.
- A capability value is a positive integer naming the highest fully implemented
  public contract version for that exact flat capability key. An absent key is
  unsupported. Zero, negative, fractional, boolean, string, nested-object, and
  inferred values are invalid. A request for an absent key or a higher version
  fails explicitly without fallback.
- `platform` and `arch` are daemon-authored build-target facts with documented
  canonical values. They are not host identity, credentials, or discovery.
- The nested boolean capability draft checkpointed in commit `af333a5` is
  reusable implementation work but is not accepted T-002 behavior.
- Changing the fields, discriminators, capability value semantics, or flat-key
  contract requires new explicit user confirmation and a later reviewed plan
  revision before implementation.

The rest of plan revision 1 remains approved and in the same delivery order.
