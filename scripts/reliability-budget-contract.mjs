export const MODES = ["idle", "active"];
export const COUNTS = ["1", "32", "128"];
export const OBSERVED_FIELDS =
  "cpu_core_percent peak_rss_kib steady_rss_kib retained_output_bytes_per_run rss_kib_per_run threads_per_run fds_per_run cleanup_threads_delta cleanup_live_children cleanup_attachments".split(
    " ",
  );
export const BUDGET_FIELDS = OBSERVED_FIELDS.map((field) => `max_${field}`);

// Exact fractions keep the pre-observation contract independent of IEEE-754
// ceiling edges. Changing this module invalidates every v2 baseline bound to it.
const RULES = Object.freeze({
  cpu_core_percent: rule(3, 2, 0, 1, 5, 1, 5, 1),
  peak_rss_kib: rule(3, 2, 0, 1, 8192, 1, 4096, 1),
  steady_rss_kib: rule(3, 2, 0, 1, 8192, 1, 4096, 1),
  retained_output_bytes_per_run: rule(5, 4, 0, 1, 0, 1, 4096, 1),
  rss_kib_per_run: rule(3, 2, 0, 1, 256, 1, 256, 1),
  threads_per_run: rule(1, 1, 1, 4, 0, 1, 1, 4),
  fds_per_run: rule(1, 1, 1, 4, 0, 1, 1, 4),
  cleanup_threads_delta: rule(1, 1, 1, 1, 1, 1, 1, 1),
  cleanup_live_children: rule(1, 1, 0, 1, 0, 1, 1, 1),
  cleanup_attachments: rule(1, 1, 0, 1, 0, 1, 1, 1),
});

function rule(
  multiplierNumerator,
  multiplierDenominator,
  additiveNumerator,
  additiveDenominator,
  minimumNumerator,
  minimumDenominator,
  quantumNumerator,
  quantumDenominator,
) {
  return {
    multiplier: fraction(multiplierNumerator, multiplierDenominator),
    additive: fraction(additiveNumerator, additiveDenominator),
    minimum: fraction(minimumNumerator, minimumDenominator),
    quantum: fraction(quantumNumerator, quantumDenominator),
  };
}

function fraction(numerator, denominator) {
  return { numerator: BigInt(numerator), denominator: BigInt(denominator) };
}

export function deriveBudgetCeiling(field, observed) {
  const selected = RULES[field];
  if (selected === undefined || !Number.isFinite(observed) || observed < 0) {
    throw new TypeError(`cannot derive ${field} from ${observed}`);
  }
  const multiplied = multiply(decimalFraction(observed), selected.multiplier);
  const candidate = add(multiplied, selected.additive);
  const bounded =
    compare(candidate, selected.minimum) < 0 ? selected.minimum : candidate;
  const steps = ceilDivide(
    bounded.numerator * selected.quantum.denominator,
    bounded.denominator * selected.quantum.numerator,
  );
  return (
    Number(steps * selected.quantum.numerator) /
    Number(selected.quantum.denominator)
  );
}

// This is the only mapping from raw census cells to governed observations.
// `cells` contains the same mode/count cell from all three baseline rounds.
export function deriveObservedMaxima(cells) {
  if (!Array.isArray(cells) || cells.length !== 3) {
    throw new TypeError("observed maxima require exactly three round cells");
  }
  const valueFor = (cell, field) => {
    if (field === "steady_rss_kib") return cell.steady?.rss_kib;
    if (field === "cleanup_threads_delta") {
      return Math.max(0, cell.cleanup?.threads - cell.baseline?.threads);
    }
    return cell?.[field];
  };
  return Object.fromEntries(
    OBSERVED_FIELDS.map((field) => [
      field,
      Math.max(...cells.map((cell) => valueFor(cell, field))),
    ]),
  );
}

function decimalFraction(value) {
  const match = /^(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/iu.exec(value.toString());
  if (match === null)
    throw new TypeError(`invalid non-negative number ${value}`);
  const decimal = match[2] ?? "";
  const exponent = Number(match[3] ?? 0);
  let numerator = BigInt(`${match[1]}${decimal}`);
  let denominator = 10n ** BigInt(decimal.length);
  if (exponent >= 0) numerator *= 10n ** BigInt(exponent);
  else denominator *= 10n ** BigInt(-exponent);
  return { numerator, denominator };
}

function multiply(left, right) {
  return {
    numerator: left.numerator * right.numerator,
    denominator: left.denominator * right.denominator,
  };
}

function add(left, right) {
  return {
    numerator:
      left.numerator * right.denominator + right.numerator * left.denominator,
    denominator: left.denominator * right.denominator,
  };
}

function compare(left, right) {
  const difference =
    left.numerator * right.denominator - right.numerator * left.denominator;
  return difference < 0n ? -1 : difference > 0n ? 1 : 0;
}

function ceilDivide(numerator, denominator) {
  return (numerator + denominator - 1n) / denominator;
}
