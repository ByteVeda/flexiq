// Old → new doc URLs after the shared-content move (architecture + resources
// dropped their `/python` prefix). GitHub Pages has no server-side redirect
// rules, so each old path is prerendered as a stub that meta-refreshes (direct
// hits) and client-navigates (SPA) to its new home. Dependency-free: imported by
// both the client route and the build-time prerender config.

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
// root (`/python`) and its getting-started section (`/python/getting-started`).
const SECTION_LANDINGS = SDKS.flatMap((sdk): [string, string][] => {
  const first = `/${sdk}/getting-started/installation`;
  return [
    [`/${sdk}`, first],
    [`/${sdk}/getting-started`, first],
  ];
});

export const REDIRECTS: Record<string, string> = {
  ...Object.fromEntries(
    ARCH_PAGES.map((page) => {
      const suffix = page ? `/${page}` : "";
      return [`/python/architecture${suffix}`, `/architecture${suffix}`];
    }),
  ),
  ...Object.fromEntries(SECTION_LANDINGS),
  "/resources": "/resources/comparison",
  "/python/more/comparison": "/resources/comparison",
  "/python/more/faq": "/resources/faq",
  "/python/more/changelog": "/resources/changelog",
  // Shared-content migration: python's old "locking" and "sagas" names
  // normalized to the canonical "locks" / "saga" slugs shared with node/java.
  "/python/guides/reliability/locking": "/python/guides/reliability/locks",
  "/python/guides/workflows/sagas": "/python/guides/workflows/saga",
  // Slug alignment: python's per-topic names normalized to the node/java ones,
  // and its combined events+webhooks page split to match their two pages.
  "/python/guides/workflows/composition":
    "/python/guides/workflows/sub-workflows",
  "/python/guides/operations/autoscaler":
    "/python/guides/operations/autoscaling",
  "/python/more/examples/batch-emails": "/python/more/examples/bulk-emails",
  "/python/guides/extensibility/events-webhooks":
    "/python/guides/extensibility/events",
};

/** The destination for a moved path, or undefined if it isn't a redirect. */
export function redirectFor(path: string): string | undefined {
  return REDIRECTS[path.replace(/\/$/, "") || "/"];
}

/** Old paths to prerender as redirect stubs (so direct hits don't 404). */
export function redirectPaths(): string[] {
  return Object.keys(REDIRECTS);
}
