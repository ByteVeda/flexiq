import { useSyncExternalStore } from "react";
import { useLocation } from "react-router";
import { type Tier, tierForPath, tierStore } from "@/lib";
import { useSdk } from "./use-sdk";

export type { Tier };

/** The tier whose sidebar, nav bar, prev/next, prefetch and search scope the
 *  current page belongs to: forced by the URL prefix on a tier page, then the
 *  tier the reader last chose, and only then the stored SDK.
 *
 *  Deliberately not `useActiveSdk`: `/server/*` is a tier and not an SDK, so
 *  reading it through the SDK hook would fall back to the stored language and
 *  render a Python sidebar over a server page — and prev/next, which looks the
 *  current URL up in the flattened nav, would find nothing and render nothing.
 *
 *  The middle term is what makes a tier-neutral page (`/architecture/*`,
 *  `/about/*`) keep the tier rather than snap back to a language: those URLs
 *  name no tier, so without it every one of them was an exit from `/server`. */
export function useActiveTier(): Tier {
  const { sdk } = useSdk();
  const pinned = useSyncExternalStore(
    tierStore.subscribe,
    tierStore.getSnapshot,
    tierStore.getServerSnapshot,
  );
  const { pathname } = useLocation();
  return tierForPath(pathname.replace(/\/$/, "") || "/") ?? pinned ?? sdk;
}
