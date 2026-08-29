import { describe, expect, it } from "vitest";
import type { Worker } from "@/lib/api-types";
import { divergentWorkers, parseQueues } from "./utils";

function worker(
  worker_id: string,
  registry_fingerprint: string | null,
  queues = "default",
): Worker {
  return {
    worker_id,
    queues,
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

describe("parseQueues", () => {
  it("splits, trims, and drops empties", () => {
    expect(parseQueues(" default , email ,,")).toEqual(["default", "email"]);
  });

  it("returns nothing for a worker that lists no queue", () => {
    expect(parseQueues("")).toEqual([]);
  });
});

describe("divergentWorkers", () => {
  it("flags nothing when the fleet agrees", () => {
    const flagged = divergentWorkers([worker("a", "aaaa"), worker("b", "aaaa")]);
    expect([...flagged]).toEqual([]);
  });

  it("flags the odd worker out, not the group it differs from", () => {
    const flagged = divergentWorkers([
      worker("a", "aaaa"),
      worker("b", "aaaa"),
      worker("c", "bbbb"),
    ]);
    expect([...flagged]).toEqual(["c"]);
  });

  it("flags every worker when no registry has a majority", () => {
    const flagged = divergentWorkers([
      worker("a", "aaaa"),
      worker("b", "bbbb"),
      worker("c", "cccc"),
    ]);
    // A split fleet has no intended registry, so clearing any of them would be
    // telling the operator that side is fine.
    expect([...flagged].sort()).toEqual(["a", "b", "c"]);
  });

  it("ignores workers that report no registry", () => {
    // An SDK that predates the field must not turn a fleet that agrees into
    // one that looks split.
    const flagged = divergentWorkers([worker("a", "aaaa"), worker("b", null), worker("c", "aaaa")]);
    expect([...flagged]).toEqual([]);
  });

  it("flags nothing when only one worker reports", () => {
    expect([...divergentWorkers([worker("a", "aaaa")])]).toEqual([]);
  });

  it("flags nothing for an empty fleet", () => {
    expect([...divergentWorkers([])]).toEqual([]);
  });

  it("does not compare fleets that share no queue", () => {
    // Three email workers and three video workers is the normal shape of a
    // heterogeneous fleet, not a six-way tie.
    const flagged = divergentWorkers([
      worker("e1", "aaaa", "email"),
      worker("e2", "aaaa", "email"),
      worker("e3", "aaaa", "email"),
      worker("v1", "bbbb", "video"),
      worker("v2", "bbbb", "video"),
      worker("v3", "bbbb", "video"),
    ]);
    expect([...flagged]).toEqual([]);
  });

  it("compares workers whose queue sets only partly overlap", () => {
    // A `default` job lands on either, so a registry only one of them has is
    // exactly what the column exists to catch. Grouping by the exact queue key
    // would have put them in separate groups of one and said nothing.
    const flagged = divergentWorkers([
      worker("a", "aaaa", "default,email"),
      worker("b", "aaaa", "default"),
      worker("c", "bbbb", "default"),
    ]);
    expect([...flagged]).toEqual(["c"]);
  });

  it("joins workers linked only through a third", () => {
    // `email` and `video` never meet directly, but a worker serving both means
    // a job on either can land beside the other's registry.
    const flagged = divergentWorkers([
      worker("a", "aaaa", "email"),
      worker("bridge", "aaaa", "email,video"),
      worker("c", "bbbb", "video"),
    ]);
    expect([...flagged]).toEqual(["c"]);
  });

  it("keeps a tie inside one group from touching a group that agrees", () => {
    // Page-wide, `aaaa` has the majority and only `e2` looks odd. Inside
    // `email` there is no majority at all, and a split group has no intended
    // registry to clear either half against.
    const flagged = divergentWorkers([
      worker("e1", "aaaa", "email"),
      worker("e2", "bbbb", "email"),
      worker("v1", "aaaa", "video"),
      worker("v2", "aaaa", "video"),
    ]);
    expect([...flagged].sort()).toEqual(["e1", "e2"]);
  });

  it("judges the same registry per group, not once for the page", () => {
    // `bbbb` is the odd one out on `email` and the agreed-on one on `video`.
    // Reporting divergence by fingerprint would badge the video workers too.
    const flagged = divergentWorkers([
      worker("e1", "aaaa", "email"),
      worker("e2", "aaaa", "email"),
      worker("e3", "bbbb", "email"),
      worker("v1", "bbbb", "video"),
      worker("v2", "bbbb", "video"),
    ]);
    expect([...flagged]).toEqual(["e3"]);
  });

  it("leaves a worker that lists no queue out of every group", () => {
    // It shares a queue with nobody, so there is no registry to compare it to.
    const flagged = divergentWorkers([
      worker("a", "aaaa", "default"),
      worker("b", "aaaa", "default"),
      worker("orphan", "bbbb", ""),
    ]);
    expect([...flagged]).toEqual([]);
  });
});
