# Verification Evidence

## Automated Checks
- Command: `cargo fmt --all -- --check`; `npm run format:check`; `cargo check --workspace --all-targets`; `cargo test -p ctxmux-protocol`; `cargo test -p ctxmux-daemon`; `cargo test -p ctxmux-client -p ctxmux --all-targets --no-fail-fast`; `npm test`; `npm run typecheck`; `npm run build`; `scripts/check-protocol-types.sh`; `scripts/smoke-cli.sh`
- Result: All commands passed. The protocol suite executed 19 unit tests plus one seeded fuzz test; daemon, client, SDK unit, and end-to-end suites all reported non-zero passing counts. The SDK suite reported 59 passing tests, including malformed base64/range checks and raw-byte queue accounting.
- Command: `scripts/check-reliability.sh --profile smoke`
- Result: Passed from a clean committed source with source-bound v3 evidence. Idle and active resource cells stayed within the frozen budgets; the policy and receipt both passed.

## Manual Checks
- Step: Temporarily serialize `OutputChunk` with JSON integer-array bytes and run the exact generation-14 Rust wire-shape test.
- Outcome: One assertion in `tests::output_chunks_use_strict_padded_base64_on_the_generation_14_wire` turned red (wire data was an array instead of `AP8=`); the implementation was restored and the test passed with `1 passed`.
- Step: Temporarily remove the decoded-length comparison from SDK `outputChunk()` and run the canonical-base64 test.
- Outcome: One `assert.throws` assertion at the range-mismatch case turned red (`Missing expected exception`); the comparison was restored and the test passed.
- Step: Temporarily bypass the canonical base64 alphabet check and run the canonical-base64 test.
- Outcome: One assertion for `!!!!` turned red because the error path moved from `$frame.event.chunk.data` to the later range check; the decoder guard was restored and the test passed.
- Step: Temporarily count encoded base64 characters in `attachment.ts` and run the byte-accounting test.
- Outcome: One assertion turned red: the second event became `gap` instead of `output`; raw-byte accounting was restored and the test passed.
- Step: Search new helper symbols for production callers.
- Outcome: `output_chunk_bytes` and `decodeOutputBytes` are private owner helpers; no uncalled public export was added. `attachment.ts` continues to use decoded `chunk.data.length`.

## Residual Risks
- The measured qualification is the smoke profile; nightly/release duration and other platforms remain separate evidence. No reliability budget values were changed.
