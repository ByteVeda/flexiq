// Explicit .ts extension: this module is also imported by the parity scripts
// under plain Node (type stripping), where extensionless specifiers don't resolve.
import { isSdk, SDK_IDS, SDK_PROFILES, type Sdk } from "./sdk-registry.ts";

// A *tier* is a top-level way of reading these docs: its own sidebar, its own
// URL prefix, its own landing. Three of them are SDKs. The fourth is not (#825)
// — `flexiq-server` is a binary and a wire contract, and holding one copy of
// that per language is what let it scatter across three `modules/` and three
// `operate/` sections in the first place.
//
// `SDK_IDS` stays three on purpose. The active SDK is persisted and written to
// `<html data-sdk>`, where CSS uses it to pick which `<SdkOnly>` / `<CodeTabs>`
// variant to show; a `server` value there matches no variant and would blank
// every shared page. So the server tier is never stored — it is read off the
// URL, and selecting it navigates rather than setting the SDK.

/** The one tier that is not a language. Also its URL prefix and content dir. */
export const SERVER_TIER = "server";

export type Tier = Sdk | typeof SERVER_TIER;

/** Every tier in display order — the SDKs, then the door in front of them. */
export const TIER_IDS: readonly Tier[] = [...SDK_IDS, SERVER_TIER];

export interface TierProfile {
  /** Stable id; URL prefix and content-tree root. */
  id: Tier;
  /** Switcher label. */
  label: string;
  /** Section dirs under `content/docs`, in nav order. */
  navSections: string[];
}

// `architecture` and `about` close the list here exactly as they do for an SDK:
// the engine and the project are the same ones whichever door you came through.
const SERVER_PROFILE: TierProfile = {
  id: SERVER_TIER,
  label: "flexiq-server",
  navSections: ["server", "server/operate", "architecture", "about"],
};

export function isTier(value: string | null | undefined): value is Tier {
  return TIER_IDS.includes(value as Tier);
}

/** An SDK tier's profile is its `SDK_PROFILES` row — one nav, one definition. */
export function tierProfile(tier: Tier): TierProfile {
  return isSdk(tier) ? SDK_PROFILES[tier] : SERVER_PROFILE;
}

/** Ordered `{ id, label }` pairs for switcher UIs. */
export function tierLabels(): { id: Tier; label: string }[] {
  return TIER_IDS.map((id) => ({ id, label: tierProfile(id).label }));
}

/** The tier forced by an explicit `/<tier>` URL prefix, or null on a page that
 *  belongs to no tier (`/architecture/*`, `/about/*`, `/`), where the active
 *  tier comes from the stored SDK instead. */
export function tierForPath(path: string): Tier | null {
  for (const id of TIER_IDS) {
    if (path === `/${id}` || path.startsWith(`/${id}/`)) {
      return id;
    }
  }
  return null;
}
