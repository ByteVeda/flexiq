import type { Command } from "commander";
import { DETACHED_ENV } from "../../detached";
import { loadApp } from "../load-app";
import { positiveIntFlag } from "../parse";

interface ExecutorOptions {
  attach?: string;
  slots?: string;
  executorId?: string;
  connectTimeout?: string;
  drainTimeout?: string;
}

export function registerExecutor(program: Command): void {
  program
    .command("executor <app>")
    .description(
      "Run tasks for a detached scheduler. <app> is a module exporting a configured Queue " +
        "(default export or `queue`).",
    )
    .option(
      "--attach <address>",
      "scheduler address: host:port, :port, or unix:/path (env: FLEXIQ_ATTACH)",
    )
    .option("--slots <n>", "jobs to run concurrently (env: FLEXIQ_SLOTS)")
    .option("--executor-id <id>", "identity announced to the scheduler")
    .option("--connect-timeout <ms>", "how long to wait for the connection")
    .option("--drain-timeout <ms>", "how long a drain waits for in-flight jobs")
    .action(async (appPath: string, options: ExecutorOptions) => {
      // Set before the app is imported: building a Queue is what opens storage,
      // and an executor must not.
      process.env[DETACHED_ENV] = "1";

      const app = await loadApp(appPath);
      // The token is read from the environment inside `runExecutor`, never as a
      // flag: in argv it would show up in `ps` output and shell history.
      const executor = await app.runExecutor({
        attach: options.attach,
        slots: positiveIntFlag(options.slots, "slots"),
        executorId: options.executorId,
        connectTimeoutMs: positiveIntFlag(options.connectTimeout, "connect-timeout"),
        shutdownDrainMs: positiveIntFlag(options.drainTimeout, "drain-timeout"),
      });

      process.stdout.write(
        `taskito executor ${executor.executorId} attached to ${executor.schedulerId} ` +
          `at ${executor.peer} — Ctrl-C to stop\n`,
      );

      // `stop()` drains in-flight work and disconnects; it is memoized, so the
      // signal path and a scheduler-initiated shutdown cannot tear down twice.
      // The handler cannot be `async`: nothing awaits what a signal listener
      // returns, so a rejected `stop()` would surface as an unhandled rejection
      // and abort the process mid-drain instead of finishing it.
      const stop = (): void => {
        executor.stop().then(
          () => process.exit(0),
          (error: unknown) => {
            process.stderr.write(`taskito executor failed to stop cleanly: ${String(error)}\n`);
            process.exit(1);
          },
        );
      };
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);

      // Resolves when the scheduler ends the session, so a `taskito-server`
      // shutting down takes its executors with it rather than stranding them.
      await executor.wait();
      await executor.stop();
    });
}
