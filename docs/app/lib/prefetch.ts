import { allDocSlugs, getDocLoader } from "./content";
import type { Tier } from "./tier-registry";

/** Tiers already warmed this session — a toggle back to one is a no-op. */
const prefetched = new Set<Tier>();

/** Max chunks fetched at once, so a 80-page SDK doesn't saturate the network. */
const CONCURRENCY = 3;

/** Run `cb` when the browser is idle; fall back to a microtask-ish delay. */
function onIdle(cb: () => void): void {
  if (typeof window !== "undefined" && "requestIdleCallback" in window) {
    window.requestIdleCallback(cb, { timeout: 2000 });
  } else {
    setTimeout(cb, 1);
  }
}

interface NetworkInformation {
  saveData?: boolean;
  effectiveType?: string;
}

/** Honor the user's data-saver preference and skip slow (2G) connections. */
function prefetchAllowed(): boolean {
  const conn = (navigator as unknown as { connection?: NetworkInformation })
    .connection;
  if (!conn) {
    return true;
  }
  if (conn.saveData) {
    return false;
  }
  return conn.effectiveType !== "slow-2g" && conn.effectiveType !== "2g";
}

/**
 * Warm every doc chunk for `tier` in the background. Idempotent per tier;
 * chunked across idle callbacks with a small concurrency cap. A no-op during
 * SSR/prerender and under Save-Data / 2G. Failed prefetches are ignored — the
 * page still loads on demand when actually visited.
 */
export function prefetchTierDocs(tier: Tier): void {
  if (typeof window === "undefined" || prefetched.has(tier)) {
    return;
  }
  if (!prefetchAllowed()) {
    return; // not marked prefetched, so a later call can retry if conditions improve
  }
  prefetched.add(tier);

  const prefix = `/${tier}`;
  const loaders = allDocSlugs()
    .filter((slug) => slug === prefix || slug.startsWith(`${prefix}/`))
    .map(getDocLoader)
    .filter((loader): loader is NonNullable<typeof loader> => Boolean(loader));

  let next = 0;
  const pump = (): void => {
    if (next >= loaders.length) {
      return;
    }
    const loader = loaders[next++];
    loader()
      .catch(() => {})
      .finally(() => onIdle(pump));
  };
  for (let lane = 0; lane < CONCURRENCY; lane++) {
    onIdle(pump);
  }
}
