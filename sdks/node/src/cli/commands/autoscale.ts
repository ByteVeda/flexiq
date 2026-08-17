import type { Command } from "commander";
import { type AutoscaleOptions, serveAutoscaler } from "../../autoscale";
import { connect, type GlobalOptions } from "../connect";
import { nonNegativeIntFlag, numberFlag, positiveIntFlag } from "../parse";

interface AutoscaleFlags {
  minWorkers?: string;
  maxWorkers?: string;
  targetQueueDepth?: string;
  targetUtilisation?: string;
  scaleUpWindow?: string;
  scaleDownWindow?: string;
  tolerance?: string;
  pollInterval?: string;
  drainTimeout?: string;
  concurrency?: string;
  queues?: string;
  batchSize?: string;
  nodeArg?: string[];
}

export function registerAutoscale(program: Command): void {
  program
    .command("autoscale <app>")
    .description(
      "Scale worker processes to match queue depth. <app> is a module exporting a configured Queue.",
    )
    .option("--min-workers <n>", "never scale below this count", "1")
    .option("--max-workers <n>", "never scale above this count", "10")
    .option("--target-queue-depth <n>", "pending jobs per worker", "15")
    .option("--target-utilisation <ratio>", "target running/capacity ratio", "0.75")
    .option("--scale-up-window <ms>", "scale-up stabilisation window", "0")
    .option("--scale-down-window <ms>", "scale-down stabilisation window", "300000")
    .option("--tolerance <ratio>", "skip scaling within this fraction of current", "0.1")
    .option("--poll-interval <ms>", "milliseconds between decision ticks", "5000")
    .option("--drain-timeout <ms>", "SIGTERM grace period per worker", "30000")
    .option("--concurrency <n>", "jobs each worker runs at once", "4")
    .option("--queues <list>", "comma-separated queue names for the workers")
    .option("--batch-size <n>", "jobs claimed per poll")
    .option(
      "--node-arg <flag>",
      "extra flag for the worker's node binary (repeatable)",
      (value: string, previous: string[] = []) => [...previous, value],
    )
    .action(async (appPath: string, flags: AutoscaleFlags, command: Command) => {
      const queue = connect(command.optsWithGlobals() as GlobalOptions);
      const options: AutoscaleOptions = {
        app: appPath,
        minWorkers: nonNegativeIntFlag(flags.minWorkers, "min-workers"),
        maxWorkers: positiveIntFlag(flags.maxWorkers, "max-workers"),
        targetQueueDepthPerWorker: positiveIntFlag(flags.targetQueueDepth, "target-queue-depth"),
        targetUtilisation: numberFlag(flags.targetUtilisation, "target-utilisation"),
        scaleUpWindowMs: nonNegativeIntFlag(flags.scaleUpWindow, "scale-up-window"),
        scaleDownWindowMs: nonNegativeIntFlag(flags.scaleDownWindow, "scale-down-window"),
        tolerance: numberFlag(flags.tolerance, "tolerance"),
        pollIntervalMs: positiveIntFlag(flags.pollInterval, "poll-interval"),
        drainTimeoutMs: positiveIntFlag(flags.drainTimeout, "drain-timeout"),
        concurrencyPerWorker: positiveIntFlag(flags.concurrency, "concurrency"),
        queues: flags.queues ? flags.queues.split(",") : undefined,
        batchSize: positiveIntFlag(flags.batchSize, "batch-size"),
        nodeArgs: flags.nodeArg,
      };
      process.stdout.write(
        `flexiq autoscaler running (${options.minWorkers}-${options.maxWorkers} workers) — Ctrl-C to stop\n`,
      );
      // Resolves once a signal arrives and every worker has drained.
      await serveAutoscaler(queue, options);
    });
}
