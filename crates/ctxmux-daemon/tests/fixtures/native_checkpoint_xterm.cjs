// Test-only independent target. Uses the public xterm API; no daemon or PTY.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
assert.equal(
  process.argv.length,
  4,
  "supply native fixtures and exact xterm module",
);
const inputPath = path.resolve(process.argv[2]);
const modulePath = fs.realpathSync(process.argv[3]);
const { Terminal } = require(modulePath);
const write = (terminal, data) =>
  new Promise((resolve) => terminal.write(Uint8Array.from(data), resolve));
function bufferSnapshot(buffer, cols) {
  return {
    type: buffer.type,
    baseY: buffer.baseY,
    length: buffer.length,
    cursorX: buffer.cursorX,
    cursorY: buffer.cursorY,
    rows: Array.from({ length: buffer.length }, (_, y) => {
      const line = buffer.getLine(y);
      assert.ok(line, "every retained row exists");
      return {
        wrapped: line.isWrapped,
        cells: Array.from({ length: cols }, (_, x) => {
          const c = line.getCell(x);
          assert.ok(c, "every source cell exists");
          return {
            chars: c.getChars(),
            width: c.getWidth(),
            fg: c.getFgColor(),
            bg: c.getBgColor(),
            fgMode: c.getFgColorMode(),
            bgMode: c.getBgColorMode(),
            bold: c.isBold(),
            italic: c.isItalic(),
            underline: c.isUnderline(),
            inverse: c.isInverse(),
          };
        }),
      };
    }),
  };
}
function snapshot(t) {
  assert.ok(t.buffer.normal.length >= t.rows, "normal screen is nonempty");
  if (t.buffer.active.type === "alternate")
    assert.equal(t.buffer.alternate.length, t.rows);
  return {
    active: t.buffer.active.type,
    modes: t.modes,
    normal: bufferSnapshot(t.buffer.normal, t.cols),
    alternate: bufferSnapshot(t.buffer.alternate, t.cols),
  };
}
function leafDifferences(actual, expected, prefix = "") {
  if (Object.is(actual, expected)) return [];
  if (
    actual &&
    expected &&
    typeof actual === "object" &&
    typeof expected === "object"
  ) {
    return [
      ...new Set([...Object.keys(actual), ...Object.keys(expected)]),
    ].flatMap((key) =>
      leafDifferences(
        actual[key],
        expected[key],
        prefix ? prefix + "." + key : key,
      ),
    );
  }
  return [{ path: prefix, actual, expected }];
}
async function inputModes(t) {
  const replies = [];
  const subscription = t.onData((data) => replies.push(data));
  await write(t, Buffer.from("\x1b[?1006$p"));
  subscription.dispose();
  assert.equal(replies.length, 1, "real public SGR mode query must reply");
  return replies;
}
(async () => {
  const cases = JSON.parse(fs.readFileSync(inputPath, "utf8"));
  assert.equal(cases.length, 7, "exact nonempty native fixtures");
  const results = [];
  for (const c of cases) {
    const options = {
      rows: c.rows,
      cols: c.cols,
      scrollback: 100,
      allowProposedApi: true,
    };
    const reference = new Terminal(options),
      target = new Terminal(options);
    try {
      await write(reference, c.prefix);
      await write(target, c.seed);
      const beforeReference = snapshot(reference),
        beforeTarget = snapshot(target);
      const precisionLimit =
        c.name === "normal origin pending wrap through alternate";
      const differences = leafDifferences(beforeTarget, beforeReference);
      if (precisionLimit) {
        // Root-reviewed P2/T004 boundary: one empty alternate-row wrap flag.
        // Every other cell, cursor, history and input-mode leaf must agree.
        assert.deepEqual(
          differences,
          [{ path: "alternate.rows.4.wrapped", actual: false, expected: true }],
          "exact known precision boundary, no other mismatch permitted",
        );
      } else {
        assert.deepEqual(
          beforeTarget,
          beforeReference,
          c.name + ": checkpoint state",
        );
      }
      const beforeModeReference = await inputModes(reference),
        beforeModeTarget = await inputModes(target);
      assert.deepEqual(
        beforeModeTarget,
        beforeModeReference,
        c.name + ": SGR encoding",
      );
      if (c.name === "normal history")
        assert.equal(target.buffer.normal.baseY, 2);
      if (c.name === "CUP only no manufactured history")
        assert.equal(target.buffer.normal.baseY, 0);
      if (c.name === "alternate sgr and real exit") {
        assert.equal(target.buffer.active.type, "alternate");
        assert.equal(target.modes.mouseTrackingMode, "any");
        assert.deepEqual(beforeModeTarget, ["\x1b[?1006;1$y"]);
        assert.equal(target.buffer.normal.baseY, 2);
      }
      await write(reference, c.tail);
      await write(target, c.tail);
      const afterReference = snapshot(reference),
        afterTarget = snapshot(target);
      assert.deepEqual(
        afterTarget,
        afterReference,
        c.name + ": original continuation",
      );
      assert.deepEqual(
        await inputModes(target),
        await inputModes(reference),
        c.name + ": continued SGR encoding",
      );
      results.push({
        name: c.name,
        sourceGrid: { cols: c.cols, rows: c.rows },
        seedBytes: c.seed.length,
        before: {
          active: beforeTarget.active,
          baseY: beforeTarget.normal.baseY,
          length: beforeTarget.normal.length,
          mouse: beforeTarget.modes.mouseTrackingMode,
        },
        after: {
          active: afterTarget.active,
          baseY: afterTarget.normal.baseY,
          length: afterTarget.normal.length,
          mouse: afterTarget.modes.mouseTrackingMode,
        },
        passed: !precisionLimit,
        precisionLimit: precisionLimit
          ? { priority: "P2", task: "T-004", differences, tailExact: true }
          : undefined,
      });
    } finally {
      reference.dispose();
      target.dispose();
    }
  }
  const sha = (p) =>
    crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
  assert.equal(results.filter((result) => result.passed).length, 6);
  assert.equal(results.filter((result) => result.precisionLimit).length, 1);
  const receipt = {
    basicPassed: true,
    fullCodecPassed: false,
    nodeVersion: process.version,
    privateFieldsUsed: false,
    userRunTouched: false,
    inputPath,
    inputSha: sha(inputPath),
    modulePath,
    moduleSha: sha(modulePath),
    conditions:
      "Public native-generated seed vs uninterrupted public xterm, no browser/physical wheel/lifecycle claim.",
    results,
  };
  fs.writeFileSync(
    path.join(path.dirname(inputPath), "xterm-receipt.json"),
    JSON.stringify(receipt, null, 2) + "\n",
  );
  console.log(JSON.stringify(receipt, null, 2));
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
