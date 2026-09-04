import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // Both are failure deadlines, not delays: a passing test returns as soon as
    // its work is done, so a generous number costs a green run nothing and only
    // changes how long a genuinely stuck one takes to report.
    //
    // Sized for windows-latest, which is where every instance of this has come
    // from — the Linux and macOS runners finish these in a fraction of it. The
    // dashboard hooks are the expensive ones: each opens a fresh SQLite queue
    // (migrating on open) and binds a server, and the authed suites also seed a
    // PBKDF2 session. 15s covered that until a windows runner missed it too.
    testTimeout: 30_000,
    hookTimeout: 45_000,
  },
});
