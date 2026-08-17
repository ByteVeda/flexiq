import path from "node:path";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import type { ProxyOptions } from "vite";

const backend = "http://127.0.0.1:8080";

/**
 * Vite's default proxy error handler logs a full stack trace per failed
 * request. The dashboard polls ~6 endpoints every 5s, so an offline
 * backend would fill the terminal with noise within seconds. We replace
 * the default handler with a throttled one (a single warning every 30s)
 * that still returns a 503 to the browser so `BackendOffline` triggers.
 *
 * `lastLogAt` is shared across every proxy entry (one warning total, not
 * one-per-path).
 */
let lastProxyErrorAt = 0;
const PROXY_LOG_THROTTLE_MS = 30_000;

const quietProxy = (target: string): ProxyOptions => ({
  target,
  changeOrigin: false,
  configure: (proxy) => {
    proxy.removeAllListeners("error");
    proxy.on("error", (err, _req, res) => {
      const now = Date.now();
      if (now - lastProxyErrorAt > PROXY_LOG_THROTTLE_MS) {
        lastProxyErrorAt = now;
        const code = (err as NodeJS.ErrnoException).code ?? err.message;
        console.warn(
          `[vite] flexiq backend offline (${code}). Start it with: ` +
            `'flexiq dashboard --app myapp:queue'. Further proxy errors suppressed for 30s.`,
        );
      }
      // Mimic the original handler: respond with a 5xx so the browser
      // sees a real error, then the dashboard's BackendOffline page kicks
      // in. Guard against the response being already-written.
      if (res && "writeHead" in res && !res.headersSent) {
        res.writeHead(503, { "content-type": "application/json" });
        res.end('{"error":"backend offline"}');
      }
    });
  },
});

export default defineConfig({
  plugins: [
    tanstackRouter({
      target: "react",
      routesDirectory: "./src/routes",
      generatedRouteTree: "./src/routeTree.gen.ts",
      quoteStyle: "double",
      semicolons: true,
    }),
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "../sdks/python/flexiq/static/dashboard",
    emptyOutDir: true,
    sourcemap: false,
    target: "es2022",
  },
  server: {
    proxy: {
      "/api": quietProxy(backend),
      "/health": quietProxy(backend),
      "/readiness": quietProxy(backend),
      "/metrics": quietProxy(backend),
    },
  },
});
