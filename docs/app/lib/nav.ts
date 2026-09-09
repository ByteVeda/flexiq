import { docTitle, hasDoc } from "./manifest";
import { isSdk, type Sdk } from "./sdk-registry";
import {
  SERVER_TIER,
  TIER_IDS,
  type Tier,
  tierForPath,
  tierProfile,
} from "./tier-registry";

interface Meta {
  title?: string;
  pages?: string[];
  root?: boolean;
}

// meta.json files describe nav order + group titles; reused unchanged as the nav
// source (same files Fumadocs used), keyed by their content-relative directory.
const META = import.meta.glob<{ default: Meta }>(
  "../../content/docs/**/meta.json",
  {
    eager: true,
  },
);

function metaFor(dir: string): Meta {
  const suffix = dir ? `${dir}/meta.json` : "meta.json";
  for (const [key, mod] of Object.entries(META)) {
    if (key.endsWith(`/content/docs/${suffix}`)) {
      return mod.default;
    }
  }
  return {};
}

function humanize(name: string): string {
  return name.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function titleFor(slug: string, fallback: string): string {
  return (
    docTitle(slug) ??
    metaFor(slug.replace(/^\//, "")).title ??
    humanize(fallback)
  );
}

/** A sidebar entry: either a link (`href`) or a labelled group (`children`). */
export interface NavNode {
  title: string;
  href?: string;
  children?: NavNode[];
}

/** Build the nav nodes for a directory's `pages`, recursing into subsections. */
function nodesForDir(dir: string): NavNode[] {
  const meta = metaFor(dir);
  const nodes: NavNode[] = [];
  for (const name of meta.pages ?? []) {
    if (name === "index") {
      continue; // the section's own index is its group header, not a child item
    }
    const childDir = `${dir}/${name}`;
    const childMeta = metaFor(childDir);
    if (childMeta.pages) {
      // Subsection → nested group. Link the group title to its index page if any.
      const indexSlug = `/${childDir}`;
      nodes.push({
        title: childMeta.title ?? humanize(name),
        href: hasDoc(indexSlug) ? indexSlug : undefined,
        children: nodesForDir(childDir),
      });
    } else {
      const slug = `/${childDir}`;
      nodes.push({ title: titleFor(slug, name), href: slug });
    }
  }
  return nodes;
}

/** Top-level sidebar groups for a tier, one per section directory. */
function buildTree(sections: string[]): NavNode[] {
  return sections.map((dir) => {
    const indexSlug = `/${dir}`;
    return {
      title: metaFor(dir).title ?? humanize(dir),
      href: hasDoc(indexSlug) ? indexSlug : undefined,
      children: nodesForDir(dir),
    };
  });
}

// Each tier's nav is its registry `navSections` built into a tree.
// `architecture` and `about` are tier-neutral (the engine and the project are
// the same ones); both appear in every tier's section list at shared top-level
// URLs.
const NAV_BY_TIER = Object.fromEntries(
  TIER_IDS.map((id) => [id, buildTree(tierProfile(id).navSections)]),
) as Record<Tier, NavNode[]>;

export type { Sdk };

/** The SDK forced by an explicit `/<sdk>` URL prefix, or null on a shared page
 *  or a `/server/*` one (where the active SDK comes from the global store). */
export function forcedSdkForPath(path: string): Sdk | null {
  const tier = tierForPath(path);
  return tier !== null && isSdk(tier) ? tier : null;
}

/** Where the switcher should go: the same page under the target tier's prefix
 *  if it exists, else that tier's landing. The server tier mounts one copy of
 *  each page rather than one per SDK, so it has no counterpart to swap to and
 *  always lands on its own index. Pages in no tier stay put (the caller skips
 *  navigation when the current path has no tier prefix). */
export function tierSwitchTarget(path: string, target: Tier): string {
  if (target === SERVER_TIER) {
    return `/${SERVER_TIER}`;
  }
  const current = forcedSdkForPath(path);
  if (current && current !== target) {
    const swapped = `/${target}${path.slice(`/${current}`.length)}`;
    if (hasDoc(swapped)) {
      return swapped;
    }
  }
  return `/${target}/getting-started/installation`;
}

export function navForTier(tier: Tier): NavNode[] {
  return NAV_BY_TIER[tier];
}

/** Depth-first flattened links for the active tier — drives prev/next. */
export function flatNav(tier: Tier): { title: string; href: string }[] {
  const out: { title: string; href: string }[] = [];
  const walk = (nodes: NavNode[]) => {
    for (const n of nodes) {
      if (n.href) {
        out.push({ title: n.title, href: n.href });
      }
      if (n.children) {
        walk(n.children);
      }
    }
  };
  walk(navForTier(tier));
  return out;
}
