// Global active-SDK store. Single source of truth shared by the no-flash boot
// script (root.tsx), the sidebar switcher, inline `<CodeTabs>`, and `<SdkOnly>`.
// Kept as a tiny external store so `useSyncExternalStore` can hand React an
// explicit server snapshot — the SSG-safe way to read a browser-only value
// without a hydration mismatch. The live value lives on `<html data-sdk>`; CSS
// shows/hides each SDK's variants off that attribute.

import { DEFAULT_SDK, isSdk, type Sdk } from "./sdk-registry";

export type { Sdk };

const KEY = "flexiq-sdk";
const DEFAULT = DEFAULT_SDK;

const listeners = new Set<() => void>();

/** Resolve the active SDK: `?sdk=` query > localStorage > default. Used by the
 *  no-flash bootstrap in root.tsx and as the first client read. */
export function readSdk(): Sdk {
  if (typeof document === "undefined") {
    return DEFAULT;
  }
  try {
    const param = new URLSearchParams(window.location.search).get("sdk");
    if (isSdk(param)) {
      return param;
    }
    const stored = localStorage.getItem(KEY);
    if (isSdk(stored)) {
      return stored;
    }
  } catch {
    // ignore storage/URL access failures (private mode etc.)
  }
  return DEFAULT;
}

function currentSdk(): Sdk {
  const attr = document.documentElement.dataset.sdk;
  return isSdk(attr) ? attr : DEFAULT;
}

function notify(): void {
  for (const listener of listeners) {
    listener();
  }
}

// Another tab changed the choice — mirror it onto this document, then notify.
function onStorage(event: StorageEvent): void {
  if (event.key !== KEY || !isSdk(event.newValue)) {
    return;
  }
  document.documentElement.dataset.sdk = event.newValue;
  notify();
}

export const sdkStore = {
  subscribe(callback: () => void): () => void {
    if (listeners.size === 0) {
      window.addEventListener("storage", onStorage);
    }
    listeners.add(callback);
    return () => {
      listeners.delete(callback);
      if (listeners.size === 0) {
        window.removeEventListener("storage", onStorage);
      }
    };
  },
  getSnapshot: currentSdk,
  getServerSnapshot: (): Sdk => DEFAULT,
  /** Set + persist the active SDK; drives the CSS show/hide via `<html data-sdk>`. */
  set(sdk: Sdk): void {
    document.documentElement.dataset.sdk = sdk;
    try {
      localStorage.setItem(KEY, sdk);
    } catch {
      // ignore storage failures (private mode etc.)
    }
    notify();
  },
};
