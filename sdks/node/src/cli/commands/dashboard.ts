import { once } from "node:events";
import type { Command } from "commander";
import { serveDashboard } from "../../dashboard";
import { connect, type GlobalOptions } from "../connect";
import { positiveIntFlag } from "../parse";

interface DashboardFlags {
  port?: string;
  host?: string;
  auth?: boolean;
  token?: string;
  insecureCookies?: boolean;
}

export function registerDashboard(program: Command): void {
  program
    .command("dashboard")
    .description("Serve the web dashboard")
    .option("-p, --port <n>", "port to listen on", "8787")
    .option("--host <host>", "host to bind", "127.0.0.1")
    .option("--auth", "enable session authentication (login/setup, CSRF, roles); off by default")
    .option("--token <token>", "legacy shared-token gate (disables the login flow)")
    .option("--insecure-cookies", "drop the Secure cookie attribute for plain-HTTP dev")
    .action(async (options: DashboardFlags, command: Command) => {
      const queue = connect(command.optsWithGlobals() as GlobalOptions);
      const host = options.host ?? "127.0.0.1";
      const port = positiveIntFlag(options.port, "port") ?? 8787;
      if (port > 65535) {
        throw new Error(`--port must be <= 65535, got ${port}`);
      }
      const server = serveDashboard(queue, {
        port,
        host,
        auth: options.token ? { token: options.token } : undefined,
        authEnabled: options.auth,
        secureCookies: options.insecureCookies ? false : undefined,
      });
      // Confirm the bind succeeded before reporting success (e.g. port in use).
      await Promise.race([
        once(server, "listening"),
        once(server, "error").then(([error]) => {
          throw error;
        }),
      ]);
      process.stdout.write(`flexiq dashboard on http://${host}:${port}\n`);
    });
}
