// Operational endpoints: the KEDA scaler payload. Liveness, readiness and
// worker-resource health live in the standalone `health` module so they can be
// used without a dashboard; they are re-exported here for the route table.

import type { Queue } from "../../index";

export {
  checkHealth,
  checkReadiness,
  type HealthReport,
  type ReadinessReport,
  type ResourceStatusEntry,
  resourceStatus,
} from "../../health";

/** KEDA-compatible scaler payload for the whole queue or one named queue. */
export async function scaler(queue: Queue, url: URL) {
  const targetQueueDepth = positiveIntOr(url.searchParams.get("target"), 10);
  const queueName = url.searchParams.get("queue");

  const stats = await queue.stats();
  const workers = await queue.listWorkers();
  const totalCapacity = workers.reduce((sum, w) => sum + (w.threads ?? 0), 0);

  const response: Record<string, unknown> = {
    metricName: "taskito_queue_depth",
    metricValue: stats.pending,
    isActive: stats.pending > 0,
    liveWorkers: workers.length,
    totalCapacity,
    targetQueueDepth,
  };
  if (totalCapacity > 0) {
    response.workerUtilization = Math.round((stats.running / totalCapacity) * 1000) / 1000;
  }

  if (queueName) {
    const queueStats = await queue.statsByQueue(queueName);
    response.metricValue = queueStats.pending;
    response.isActive = queueStats.pending > 0;
    response.metricName = `taskito_queue_depth_${queueName}`;
  }

  const perQueue: Record<string, { pending: number; running: number }> = {};
  for (const [name, s] of Object.entries(await queue.statsAllQueues())) {
    perQueue[name] = { pending: s.pending, running: s.running };
  }
  response.perQueue = perQueue;

  return response;
}

function positiveIntOr(value: string | null, fallback: number): number {
  const parsed = value === null ? Number.NaN : Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}
