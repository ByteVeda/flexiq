// Old → new doc URLs. GitHub Pages has no server-side redirect rules, so each
// old path is prerendered as a stub that meta-refreshes (direct hits) and
// client-navigates (SPA) to its new home. Dependency-free: imported by both the
// client route and the build-time prerender config.
//
// Two rules for anything added here:
//   - point at the FINAL destination, never at another redirect source — a stub
//     resolves one hop only;
//   - never name a path that is still a real page. `pnpm check:parity` fails on
//     that (redirects win, so the page would be unreachable).

const ARCH_PAGES = [
  "",
  "overview",
  "job-lifecycle",
  "worker-pool",
  "scheduler",
  "mesh",
  "storage",
  "resources",
  "failure-model",
  "serialization",
];

const SDKS = ["node", "python", "java"];

// Sections that have child pages but no landing/index page. A bare hit on the
// section URL (e.g. from a breadcrumb crumb or an external link) would otherwise
// 404, so send it to the section's first page. This covers both the bare SDK
// root (`/python`) and its index-less sections.
const SECTION_LANDINGS = SDKS.flatMap((sdk): [string, string][] => {
  const first = `/${sdk}/getting-started/installation`;
  return [
    [`/${sdk}`, first],
    [`/${sdk}/getting-started`, first],
    [`/${sdk}/modules`, `/${sdk}/modules/workflows`],
    [`/${sdk}/operate`, `/${sdk}/operate/backends`],
  ];
});

// #780 — the modules came out of `guides/`. Every entry is a page that moved in
// all (or most) of the three trees; fanning a path an SDK never had costs one
// unreachable stub and keeps the table readable.
const MOVED_IN_EVERY_SDK: [string, string][] = [
  ["guides/core/steps", "modules/steps"],
  ["guides/workflows", "modules/workflows"],
  ["guides/workflows/building", "modules/workflows/building"],
  ["guides/workflows/fan-out", "modules/workflows/fan-out"],
  ["guides/workflows/conditions", "modules/workflows/conditions"],
  ["guides/workflows/gates", "modules/workflows/gates"],
  ["guides/workflows/sub-workflows", "modules/workflows/sub-workflows"],
  ["guides/workflows/saga", "modules/workflows/saga"],
  ["guides/workflows/canvas", "modules/workflows/canvas"],
  ["guides/workflows/caching", "modules/workflows/caching"],
  ["guides/workflows/analysis", "modules/workflows/analysis"],
  ["guides/resources", "modules/injection"],
  [
    "guides/resources/dependency-injection",
    "modules/injection/dependency-injection",
  ],
  ["guides/resources/proxies", "modules/injection/proxies"],
  ["guides/resources/interception", "modules/injection/interception"],
  ["guides/resources/configuration", "modules/injection/configuration"],
  ["guides/resources/observability", "modules/injection/observability"],
  ["guides/resources/testing", "modules/injection/testing"],
  ["guides/operations", "operate"],
  ["guides/operations/mesh", "modules/mesh"],
  ["guides/operations/executor", "modules/executor"],
  ["guides/operations/keda", "modules/autoscaling/keda"],
  ["guides/operations/autoscaling", "modules/autoscaling"],
  ["guides/operations/dashboard", "modules/dashboard"],
  ["guides/operations/dashboard-api", "modules/dashboard/rest-api"],
  ["guides/operations/sso", "modules/dashboard/sso"],
  ["guides/operations/testing", "guides/extend/testing"],
  ["guides/operations/backends", "operate/backends"],
  ["guides/operations/inspection", "operate/inspection"],
  ["guides/operations/cli", "operate/cli"],
  ["guides/operations/deployment", "operate/deployment"],
  ["guides/operations/security", "operate/security"],
  ["guides/operations/troubleshooting", "operate/troubleshooting"],
  ["guides/operations/migration", "operate/migration"],
  ["guides/observability", "guides/extend"],
  ["guides/observability/monitoring", "guides/extend/monitoring"],
  ["guides/observability/logging", "guides/extend/logging"],
  ["guides/observability/notes", "guides/extend/notes"],
  ["guides/extensibility", "guides/extend"],
  ["guides/extensibility/events", "guides/extend/events"],
  ["guides/extensibility/middleware", "guides/extend/middleware"],
  ["guides/extensibility/webhooks", "guides/extend/webhooks"],
  ["guides/extensibility/serializers", "guides/extend/serializers"],
  ["guides/integrations", "guides/extend"],
  ["guides/integrations/otel", "guides/extend/otel"],
  ["guides/integrations/prometheus", "guides/extend/prometheus"],
  ["guides/integrations/sentry", "guides/extend/sentry"],
];

// Pages only one tree ever had: python's `advanced-execution` and standalone
// dashboard group, and each SDK's own framework integrations.
const MOVED_IN_ONE_SDK: Record<string, [string, string][]> = {
  python: [
    ["guides/core/workflows", "modules/workflows"],
    ["guides/advanced-execution", "guides/core"],
    ["guides/advanced-execution/prefork", "guides/core/prefork"],
    ["guides/advanced-execution/async-tasks", "guides/core/async-tasks"],
    ["guides/advanced-execution/streaming", "guides/core/streaming"],
    ["guides/advanced-execution/dependencies", "guides/core/dependencies"],
    ["guides/advanced-execution/batch-enqueue", "guides/core/batch-enqueue"],
    ["guides/advanced-execution/unique-tasks", "guides/core/unique-tasks"],
    ["guides/dashboard", "modules/dashboard"],
    ["guides/dashboard/authentication", "modules/dashboard/authentication"],
    ["guides/dashboard/sso", "modules/dashboard/sso"],
    ["guides/dashboard/task-overrides", "modules/dashboard/task-overrides"],
    ["guides/dashboard/rest-api", "modules/dashboard/rest-api"],
    ["guides/operations/postgres", "operate/backends"],
    ["guides/operations/job-management", "operate/inspection"],
    ["guides/operations/upgrading-0.15", "operate/upgrading-0.15"],
    ["guides/integrations/flask", "guides/extend/flask"],
    ["guides/integrations/fastapi", "guides/extend/fastapi"],
    ["guides/integrations/django", "guides/extend/django"],
  ],
  node: [
    ["guides/integrations/express", "guides/extend/express"],
    ["guides/integrations/fastify", "guides/extend/fastify"],
    ["guides/integrations/nest", "guides/extend/nest"],
  ],
  java: [
    ["guides/operations/graalvm", "operate/graalvm"],
    ["guides/integrations/spring", "guides/extend/spring"],
    ["guides/integrations/micrometer", "guides/extend/micrometer"],
  ],
};

function sdkMoves(): [string, string][] {
  const all = SDKS.flatMap((sdk) =>
    MOVED_IN_EVERY_SDK.map(([from, to]): [string, string] => [
      `/${sdk}/${from}`,
      `/${sdk}/${to}`,
    ]),
  );
  for (const [sdk, moves] of Object.entries(MOVED_IN_ONE_SDK)) {
    for (const [from, to] of moves) {
      all.push([`/${sdk}/${from}`, `/${sdk}/${to}`]);
    }
  }
  return all;
}

// The fourth "Resources" — the SDK-neutral FAQ/comparison/changelog set — is
// now `About`, the only one of the four that names what it holds.
const ABOUT_PAGES = ["comparison", "faq", "migrating-to-flexiq", "changelog"];

export const REDIRECTS: Record<string, string> = {
  ...Object.fromEntries(
    ARCH_PAGES.map((page) => {
      const suffix = page ? `/${page}` : "";
      return [`/python/architecture${suffix}`, `/architecture${suffix}`];
    }),
  ),
  ...Object.fromEntries(SECTION_LANDINGS),
  ...Object.fromEntries(sdkMoves()),
  ...Object.fromEntries(
    ABOUT_PAGES.map((page) => [`/resources/${page}`, `/about/${page}`]),
  ),
  "/about": "/about/comparison",
  "/resources": "/about/comparison",
  "/python/more/comparison": "/about/comparison",
  "/python/more/faq": "/about/faq",
  "/python/more/changelog": "/about/changelog",
  // Shared-content migration: python's old "locking" and "sagas" names
  // normalized to the canonical "locks" / "saga" slugs shared with node/java.
  "/python/guides/reliability/locking": "/python/guides/reliability/locks",
  "/python/guides/workflows/sagas": "/python/modules/workflows/saga",
  // Slug alignment: python's per-topic names normalized to the node/java ones,
  // and its combined events+webhooks page split to match their two pages.
  "/python/guides/workflows/composition":
    "/python/modules/workflows/sub-workflows",
  "/python/guides/operations/autoscaler": "/python/modules/autoscaling",
  "/python/more/examples/batch-emails": "/python/more/examples/bulk-emails",
  "/python/guides/extensibility/events-webhooks":
    "/python/guides/extend/events",
};

/** The destination for a moved path, or undefined if it isn't a redirect. */
export function redirectFor(path: string): string | undefined {
  return REDIRECTS[path.replace(/\/$/, "") || "/"];
}

/** Old paths to prerender as redirect stubs (so direct hits don't 404). */
export function redirectPaths(): string[] {
  return Object.keys(REDIRECTS);
}
