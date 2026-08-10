import type { Command } from "commander";
import { connect, type GlobalOptions } from "../connect";
import { printJson } from "../output";

export function registerMigrate(program: Command): void {
  program
    .command("migrate")
    .description("Apply pending schema changes (for a deployment that gates DDL)")
    .action(async (_options: unknown, command: Command) => {
      // Opened unmigrated on purpose: this command is the one path allowed to
      // apply DDL, so opening must not do it first.
      const queue = connect({
        ...(command.optsWithGlobals() as GlobalOptions),
        autoMigrate: false,
      });
      printJson(await queue.migrate());
    });
}
