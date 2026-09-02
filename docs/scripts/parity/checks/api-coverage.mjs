import { computeCoverage } from "../../api/coverage.mjs";
import { collectInventories, SDKS } from "../../api/inventory.mjs";
import { runApiSyncPlan } from "../../api/plan.mjs";

// (e) API reference coverage.
//
// Two failures, both blocking, for the two ways the reference rots:
//
//   1. The generated half disagrees with the declarations. A method added to
//      the core reaches three shells and, before this check, zero reference
//      pages. Now it fails the build the way a slug collision does, and
//      `pnpm sync:api` is the fix.
//   2. The hand-written half loses ground. Prose coverage is a committed
//      number that may only be corrected upward, never quietly dropped —
//      turning the gate on at today's figure rather than blocking on the
//      backlog, which is the only version of this gate that can merge.
//
// The measure itself lives in scripts/api/coverage.mjs, shared with
// `pnpm sync:api --backlog`, so the gate and the worklist can't disagree.

export function checkApiCoverage(files) {
  const errors = [];
  const report = [];

  const plan = runApiSyncPlan(collectInventories(), SDKS);
  report.push(...plan.notes);
  errors.push(...plan.errors);
  const rerun = "run `pnpm sync:api`";
  for (const sdk of plan.snapshotDrift) {
    errors.push(
      `content/api/${sdk}.json no longer matches the SDK's declarations — ${rerun}`,
    );
  }
  for (const [path, file] of plan.files) {
    if (file.changed) {
      errors.push(`${path.split("/content/").pop()} is stale — ${rerun}`);
    }
  }
  for (const path of plan.stale) {
    errors.push(
      `${path.split("/content/").pop()} is no longer generated — ${rerun}`,
    );
  }

  for (const [sdk, coverage] of computeCoverage(files, plan.inventories)) {
    const { documented, expected, undocumented, baseline } = coverage;
    const percent = Math.round(
      (documented / Math.max(1, expected.length)) * 100,
    );
    report.push(
      `  ${sdk.padEnd(7)} ${documented}/${expected.length} documented (${percent}%), ${undocumented.length} to go, ${coverage.exempt.length} allowlisted`,
    );
    for (const entry of coverage.unusedAllowlist) {
      errors.push(
        `${sdk}: allowlist entry ${entry} matches no symbol — drop it from scripts/parity/api-coverage.json`,
      );
    }
    if (documented < baseline) {
      errors.push(
        `${sdk}: reference coverage fell from ${baseline} to ${documented}; still undocumented: ${undocumented
          .map((symbol) => symbol.name)
          .slice(0, 8)
          .join(", ")}`,
      );
    } else if (documented > baseline) {
      errors.push(
        `${sdk}: reference coverage rose to ${documented} — set documented.${sdk} to ${documented} in scripts/parity/api-coverage.json so it cannot fall back`,
      );
    }
  }
  report.push("  worklist: `pnpm sync:api --backlog`");
  return { name: "API reference coverage", errors, report };
}
