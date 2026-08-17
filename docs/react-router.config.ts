import { existsSync, readdirSync, renameSync, rmdirSync } from "node:fs";
import { join } from "node:path";
import type { Config } from "@react-router/dev/config";
import { allDocPaths } from "./app/lib/doc-paths";
import { redirectPaths } from "./app/lib/redirects";

// Static docs site for GitHub Pages: no server runtime, every route prerendered
// to HTML. `DOCS_BASE_PATH=/flexiq` in CI deploys under docs.byteveda.org/flexiq;
// unset locally so `serve build/client` works from the root.
const basename = process.env.DOCS_BASE_PATH || "/";

export default {
  ssr: false,
  basename,
  // Opt into the React Router v8 behaviors now (silences the dev future-flag
  // warnings and future-proofs the migration). All are compatible with this
  // static, prerendered, loader-light site.
  future: {
    v8_middleware: true,
    v8_splitRouteModules: true,
    v8_viteEnvironmentApi: true,
    v8_passThroughRequests: true,
    v8_trailingSlashAwareDataRequests: true,
  },
  async prerender() {
    return [
      "/",
      "/llms.txt",
      "/llms-full.txt",
      ...allDocPaths(),
      ...redirectPaths(),
    ];
  },
  // Prerender nests every route under the basename dir, but Pages already serves
  // this project under /flexiq — hoist the tree so the artifact root is the site
  // root and the prefix isn't doubled. No-op locally where basename is "/".
  buildEnd({ reactRouterConfig }) {
    if (basename === "/") return;
    const clientDir = join(reactRouterConfig.buildDirectory, "client");
    const nested = join(clientDir, basename);
    if (!existsSync(nested)) return;
    for (const entry of readdirSync(nested)) {
      renameSync(join(nested, entry), join(clientDir, entry));
    }
    rmdirSync(nested);
  },
} satisfies Config;
