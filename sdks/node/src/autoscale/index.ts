/**
 * Bare-metal autoscaler for flexiq worker processes.
 *
 * A control loop that spawns and drains worker subprocesses to track queue
 * depth and utilisation — for bare metal, Docker, or systemd, i.e. anyone who
 * can't reach for KEDA and Kubernetes. On Kubernetes use `serveScaler` instead
 * and let KEDA size the fleet.
 *
 * The formula mirrors the Kubernetes HPA:
 *
 * ```text
 * depthDesired = ceil(pending / targetQueueDepthPerWorker)
 * utilDesired  = ceil(workers × (utilisation / targetUtilisation))
 * desired      = clamp(max(depthDesired, utilDesired), minWorkers, maxWorkers)
 * ```
 *
 * Stabilisation windows keep it from flapping: every tick's recommendation is
 * buffered and the least aggressive one in the window wins. Scale-up is
 * immediate by default, scale-down waits 5 minutes (the HPA default), and a
 * 10% tolerance band absorbs single-tick noise.
 */

export type { AutoscaleConfig, AutoscaleOptions } from "./config";
export { resolveAutoscaleConfig } from "./config";
export type { AutoscaleMetricsSource, ScaleDecision, ScaleInputs } from "./controller";
export { Autoscaler, computeDesiredWorkers, serveAutoscaler } from "./controller";
export { WorkerProcessManager } from "./processManager";
