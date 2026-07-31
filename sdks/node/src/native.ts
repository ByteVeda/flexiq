// Typed loader for the napi-rs binding. The generated `native/index.js` is
// CommonJS (see native/package.json). The path is computed at runtime so the
// bundler leaves the addon external (it is shipped alongside dist/, not inlined),
// and `import.meta.url` is shimmed in the CJS build by tsup.
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const bindingPath = fileURLToPath(new URL("../native/index.js", import.meta.url));
const binding = require(bindingPath) as typeof import("../native/index");

export const { JsQueue, JsWorker, startExecutor, reservedSettingPrefixes } = binding;

/** Instance types of the native classes, for typing fields and parameters. */
export type NativeQueue = InstanceType<typeof JsQueue>;
export type NativeWorker = InstanceType<typeof JsWorker>;
export type NativeExecutor = Awaited<ReturnType<typeof startExecutor>>;

export type {
  CircuitBreakerInput,
  DetailedJobFilter,
  EnqueueOptions,
  ExecutorOptions as NativeExecutorOptions,
  JobFilter,
  JsCircuitBreaker,
  JsDagEdge,
  JsDeadJob,
  JsJob,
  JsJobDag,
  JsJobError,
  JsLockInfo,
  JsMetric,
  JsOutcome,
  JsReplayEntry,
  JsStats,
  JsSubscription,
  JsTaskInvocation,
  JsTaskLog,
  JsTaskOutcome,
  JsTopic,
  JsTopicLogStat,
  JsTopicMessage,
  JsTopicStat,
  JsWorkerRow,
  JsWorkflowAdvance,
  JsWorkflowNode,
  JsWorkflowRun,
  MeshWorkerConfig,
  OpenOptions,
  PublishOptions,
  QueueConfigInput,
  TaskConfigInput,
  WorkerOptions,
} from "../native/index";
