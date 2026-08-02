/**
 * Sink routing belongs to a detached queue, not to the process.
 *
 * Two executors can attach from one Node process. While the sink was a single
 * module-level slot, the second attach took the first one's writes, and either
 * one stopping silenced whichever was still running.
 */

import { expect, it } from "vitest";
import {
  clearSink,
  createDetachedNative,
  type ExecutorSink,
  installSink,
} from "../../src/detached";

interface Recorder extends ExecutorSink {
  readonly progress: Array<[string, number]>;
  readonly logs: string[];
}

function recorder(): Recorder {
  const progress: Array<[string, number]> = [];
  const logs: string[] = [];
  return {
    progress,
    logs,
    updateProgress: (jobId, value) => {
      progress.push([jobId, value]);
    },
    writeTaskLog: (jobId, _taskName, _level, message) => {
      logs.push(`${jobId}:${message}`);
    },
  };
}

it("routes each detached queue's writes to its own sink", () => {
  const first = createDetachedNative();
  const second = createDetachedNative();
  const one = recorder();
  const two = recorder();

  installSink(first, one);
  installSink(second, two);

  first.updateProgress("job-1", 10);
  second.updateProgress("job-2", 20);
  first.writeTaskLog("job-1", "task", "info", "from one");

  expect(one.progress).toEqual([["job-1", 10]]);
  expect(two.progress).toEqual([["job-2", 20]]);
  expect(one.logs).toEqual(["job-1:from one"]);
  expect(two.logs).toEqual([]);
});

it("keeps one queue routing when another executor stops", () => {
  const first = createDetachedNative();
  const second = createDetachedNative();
  const one = recorder();
  const two = recorder();
  installSink(first, one);
  installSink(second, two);

  clearSink(first, one);

  first.updateProgress("job-1", 10);
  second.updateProgress("job-2", 20);

  expect(one.progress).toEqual([]);
  expect(two.progress).toEqual([["job-2", 20]]);
});

it("ignores a clear from an executor that has already been replaced", () => {
  const queue = createDetachedNative();
  const gone = recorder();
  const live = recorder();
  installSink(queue, gone);
  installSink(queue, live);

  clearSink(queue, gone);
  queue.updateProgress("job-1", 30);

  expect(live.progress).toEqual([["job-1", 30]]);
  expect(gone.progress).toEqual([]);
});

it("degrades rather than throwing when no executor has attached", () => {
  const queue = createDetachedNative();

  expect(() => queue.updateProgress("job-1", 10)).not.toThrow();
  expect(() => queue.writeTaskLog("job-1", "task", "info", "dropped")).not.toThrow();
});
