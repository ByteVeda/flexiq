import { describe, expect, it } from "vitest";
import type { Worker } from "@/lib/api-types";
import { divergentFingerprints } from "./utils";

function worker(worker_id: string, registry_fingerprint: string | null): Worker {
  return {
    worker_id,
    queues: "default",
    status: "active",
    last_heartbeat: 1_700_000_000_000,
    registered_at: 1_700_000_000_000,
    hostname: null,
    pid: null,
    pool_type: null,
    threads: 1,
    tags: null,
    sdk: "python",
    sdk_version: "1.0.0",
    registry_fingerprint,
  };
}

describe("divergentFingerprints", () => {
  it("flags nothing when the fleet agrees", () => {
    const flagged = divergentFingerprints([worker("a", "aaaa"), worker("b", "aaaa")]);
    expect([...flagged]).toEqual([]);
  });

  it("flags the odd worker out, not the group it differs from", () => {
    const flagged = divergentFingerprints([
      worker("a", "aaaa"),
      worker("b", "aaaa"),
      worker("c", "bbbb"),
    ]);
    expect([...flagged]).toEqual(["bbbb"]);
  });

  it("flags every group when no registry has a majority", () => {
    const flagged = divergentFingerprints([
      worker("a", "aaaa"),
      worker("b", "bbbb"),
      worker("c", "cccc"),
    ]);
    // A split fleet has no intended registry, so clearing any of them would be
    // telling the operator that side is fine.
    expect([...flagged].sort()).toEqual(["aaaa", "bbbb", "cccc"]);
  });

  it("ignores workers that report no registry", () => {
    // An SDK that predates the field must not turn a fleet that agrees into
    // one that looks split.
    const flagged = divergentFingerprints([
      worker("a", "aaaa"),
      worker("b", null),
      worker("c", "aaaa"),
    ]);
    expect([...flagged]).toEqual([]);
  });

  it("flags nothing when only one worker reports", () => {
    expect([...divergentFingerprints([worker("a", "aaaa")])]).toEqual([]);
  });

  it("flags nothing for an empty fleet", () => {
    expect([...divergentFingerprints([])]).toEqual([]);
  });
});
