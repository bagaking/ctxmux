# Revision 16 persistence-oracle correction

## Verdict

T-032 was started from a reasonable but incorrect initial diagnosis. Focused
inspection proved that the two default-concurrency failures are stale test
oracles rather than a shipped lifecycle regression:

- a Start response is a dynamic `RunInfo` snapshot, so output and its durable
  cursor may legally advance beyond zero before the response is observed;
- the retained-replay fixture writes and synchronously persists more than 4 MiB
  and takes about five seconds alone, so the generic ten-second terminal wait is
  not a valid parallel-suite performance contract.

Revision 16 keeps T-032 but narrows it to the actual owner: make the public E2E
oracle concurrency-stable without changing runtime behavior, reducing payload
coverage, adding retries or sleeps, or serializing the test suite. The task was
returned to todo before revision because it had no Gate evidence or code change.

## Required proof

The start snapshot must assert only protocol-authoritative cursor relations.
The heavy replay fixture must retain the full over-4-MiB pruning and restart
proof while using an explicit workload budget distinct from the ordinary small-
Run hang budget. The full default-concurrency binary and repository Gate must
then pass once; repeated retries are not completion evidence.
