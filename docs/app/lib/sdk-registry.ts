// Single source of truth for every SDK. Add a language = append to `SDK_IDS` +
// add a `SDK_PROFILES` row; the `Sdk` type, nav, switcher, boot script and
// SDK-aware docs all derive from here. Don't hardcode "python"/"node" elsewhere.
//
// An SDK is one *tier* of these docs and not the only kind — `flexiq-server` is
// a fifth without being a language. See `tier-registry.ts`; only a *language*
// belongs in this list, because its value is also the persisted
// `<html data-sdk>` one and every `<CodeTabs>` panel is keyed off it.
//
// Appending an id is not additive: `checks/code-tabs.mjs` then requires a
// `<Tab sdk="...">` on every block in the shared tree, `checks/section-shape.mjs`
// a full tree under `content/docs/<id>/`, and `scripts/api/inventory.mjs` a
// `SOURCES` entry. `pnpm check:parity` stays red until all three are in.

/** Supported SDK ids in display order; also the URL prefix + `data-sdk` value. */
export const SDK_IDS = ["python", "node", "java", "rust"] as const;

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
  rust: {
    id: "rust",
    label: "Rust",
    // The only shell with nothing between it and the engine: `crates/flexiq`
    // links `flexiq-core` as a dependency, so `binding` names the absence.
    language: "Rust",
    binding: "no FFI",
    navSections: [
      "rust/getting-started",
      "rust/guides",
      "rust/modules",
      "rust/operate",
      "rust/api-reference",
      "architecture",
      "rust/more/examples",
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
