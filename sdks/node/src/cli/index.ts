#!/usr/bin/env node
import { Command } from "commander";
import {
  registerAutoscale,
  registerCancel,
  registerDashboard,
  registerDlq,
  registerEnqueue,
  registerJobs,
  registerQueues,
  registerRun,
  registerScaler,
  registerStats,
} from "./commands";

const program = new Command();

program
  .name("taskito")
  .description("Taskito task queue — Node CLI")
  .version("0.16.3")
  .option("--db <path>", "SQLite database path (shorthand for --backend sqlite --dsn <path>)")
  .option("--backend <name>", "backend: sqlite | postgres | redis")
  .option("--dsn <url>", "backend connection string")
  .option("--pool-size <n>", "connection pool size")
  .option("--schema <name>", "Postgres schema")
  .option("--prefix <name>", "Redis key prefix")
  .option("--namespace <ns>", "job namespace")
  .option("--json", "output raw JSON");

registerEnqueue(program);
registerStats(program);
registerJobs(program);
registerCancel(program);
registerQueues(program);
registerDlq(program);
registerRun(program);
registerDashboard(program);
registerScaler(program);
registerAutoscale(program);

program.parseAsync(process.argv).catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
