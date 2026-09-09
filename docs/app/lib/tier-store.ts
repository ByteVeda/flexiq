// The tier a reader is in when the URL does not name one — `/architecture/*`,
// `/about/*`, `/`.
//
// Three of the four tiers are languages and `sdkStore` already holds those, so
// only a tier that is *not* a language needs recording here: `<html data-sdk>`
// cannot carry it (see tier-registry.ts), and without somewhere to put it,
// walking from `/server` into a shared page silently dropped the reader back
// into whatever language they had last picked — sidebar, nav bar, switcher and
// search scope all flipping at once.
//
// Deliberately in memory rather than persisted. It exists to carry a choice
// across a navigation, and a fresh load of a shared page has no such choice to
// honour: the stored language is the only signal there. That also keeps it out
// of the no-flash boot script in root.tsx, which stays about `data-sdk` alone.

import { isSdk } from "./sdk-registry";
import { sdkStore } from "./sdk-store";
import type { Tier } from "./tier-registry";

/** Null means "follow the SDK store", which is the answer for all three SDK tiers. */
let pinned: Tier | null = null;

const listeners = new Set<() => void>();

function notify(): void {
  for (const listener of listeners) {
    listener();
  }
}

export const tierStore = {
  subscribe(callback: () => void): () => void {
    listeners.add(callback);
    return () => {
      listeners.delete(callback);
    };
  },
  getSnapshot: (): Tier | null => pinned,
  getServerSnapshot: (): Tier | null => null,
  /** Record the tier the reader is in. A language *is* the SDK, so it goes to
   *  the SDK store and clears the pin; a tier that is no language has nowhere
   *  else to live. Routed on `isSdk`, so a second one needs no new branch. */
  set(tier: Tier): void {
    if (isSdk(tier)) {
      sdkStore.set(tier);
      pinned = null;
    } else {
      pinned = tier;
    }
    notify();
  },
};
