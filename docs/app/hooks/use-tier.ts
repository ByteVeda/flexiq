import { useLocation } from "react-router";
import { type Tier, tierForPath } from "@/lib";
import { useSdk } from "./use-sdk";

export type { Tier };

/** The tier whose sidebar, prev/next and prefetch the current page belongs to:
 *  forced by the URL prefix on a tier page, otherwise the stored SDK.
 *
 *  Deliberately not `useActiveSdk`: `/server/*` is a tier and not an SDK, so
 *  reading it through the SDK hook would fall back to the stored language and
 *  render a Python sidebar over a server page — and prev/next, which looks the
 *  current URL up in the flattened nav, would find nothing and render nothing. */
export function useActiveTier(): Tier {
  const { sdk } = useSdk();
  const { pathname } = useLocation();
  return tierForPath(pathname.replace(/\/$/, "") || "/") ?? sdk;
}
