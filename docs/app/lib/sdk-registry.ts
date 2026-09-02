// Single source of truth for every SDK. Add a language = append to `SDK_IDS` +
// add a `SDK_PROFILES` row; the `Sdk` type, nav, switcher, boot script and
// SDK-aware docs all derive from here. Don't hardcode "python"/"node" elsewhere.

/** Supported SDK ids in display order; also the URL prefix + `data-sdk` value. */
export const SDK_IDS = ["python", "node", "java"] as const;

export type Sdk = (typeof SDK_IDS)[number];

/** The SDK assumed before any user choice — used by the SSG server snapshot and
 *  the boot script, so prerendered HTML and first paint agree. */
export const DEFAULT_SDK: Sdk = "python";

export interface SdkProfile {
  /** Stable id; URL prefix and `data-sdk` value. */
  id: Sdk;
  /** Switcher / breadcrumb label, e.g. "Node.js". */
  label: string;
  /** Language name for prose ("a hybrid <language>/Rust system"). */
  language: string;
  /** FFI boundary into the Rust core, e.g. "PyO3", "N-API". */
  binding: string;
  /** Section dirs under `content/docs`, in nav order (architecture/about shared). */
  navSections: string[];
}

export const SDK_PROFILES: Record<Sdk, SdkProfile> = {
  python: {
    id: "python",
    label: "Python",
    language: "Python",
    binding: "PyO3",
    navSections: [
      "python/getting-started",
      "python/guides",
      "python/modules",
      "python/operate",
      "python/api-reference",
      "architecture",
      "python/more/examples",
      "about",
    ],
  },
  node: {
    id: "node",
    label: "Node.js",
    language: "Node.js",
    binding: "N-API",
    navSections: [
      "node/getting-started",
      "node/guides",
      "node/modules",
      "node/operate",
      "node/api-reference",
      "architecture",
      "node/more/examples",
      "about",
    ],
  },
  java: {
    id: "java",
    label: "Java",
    language: "Java",
    binding: "JNI",
    navSections: [
      "java/getting-started",
      "java/guides",
      "java/modules",
      "java/operate",
      "java/api-reference",
      "architecture",
      "java/more/examples",
      "about",
    ],
  },
};

export function isSdk(value: string | null | undefined): value is Sdk {
  return SDK_IDS.includes(value as Sdk);
}

export function sdkProfile(sdk: Sdk): SdkProfile {
  return SDK_PROFILES[sdk];
}

/** Ordered `{ id, label }` pairs for switcher UIs. */
export function sdkLabels(): { id: Sdk; label: string }[] {
  return SDK_IDS.map((id) => ({ id, label: SDK_PROFILES[id].label }));
}
