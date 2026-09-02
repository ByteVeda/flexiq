import { readdirSync, readFileSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { mountsForRelPath } from "../../../app/lib/doc-slugs.ts";
import { REDIRECTS } from "../../../app/lib/redirects.ts";
import { SDK_IDS } from "../../../app/lib/sdk-registry.ts";

// (g) Internal links resolve, and none of them point at a redirect source.
//
// A restructure is not done when the pages are in place; it is done when
// nothing links to where they used to be. A stale link still *works* — the
// redirect stub catches it — which is exactly why nobody notices the second
// hop, and why this has to be blocking rather than a report.
//
// Two failures:
//   - the target is not a page at all (a typo, or a page deleted in a move);
//   - the target is a REDIRECTS key. Legal to type, silently slow forever.
//
// Anchors are stripped, not verified: heading ids come from rehype-slug at
// build time and are not knowable here.

const APP_DIR = fileURLToPath(new URL("../../../app", import.meta.url));

// Modules that hold the old→new table itself, or derive URLs rather than
// naming them. Scanning these would report every retired path as broken.
const APP_EXEMPT = ["lib/redirects.ts", "lib/doc-slugs.ts"];

// A doc URL always starts with one of these. Anything else in an app file is an
// asset, an API route or an external link, and is none of this check's business.
const DOC_ROOTS = [...SDK_IDS, "architecture", "about", "resources"];

// App-side literals are matched conservatively: a leading slash and at least two
// segments. `"python/guides"` (a content directory in the SDK registry) and
// `/python` (a URL prefix, not a link) both name real things that are not pages,
// and flagging them would only teach the next reader to add exemptions.
const MIN_APP_SEGMENTS = 2;

const MD_LINK = /\]\((\/[^)\s]*)\)/g;
const HREF = /href="(\/[^"]*)"/g;
// <SdkLink to="..."> prefixes the active SDK whether or not the value is
// absolute, so both forms resolve once per SDK.
const SDK_LINK = /<SdkLink\b[^>]*\sto="([^"]*)"/g;

// …unless the link sits inside a block that only one SDK ever sees. A shared
// page routinely says "under <SdkOnly sdk='java'>GraalVM native image</SdkOnly>"
// and links a java-only page; resolving that for python is a false positive, and
// a check that cries wolf gets weakened rather than fixed.
const SCOPES = [
  [/<SdkOnly\b[^>]*\ssdk="([a-z]+)"[^>]*>/g, "</SdkOnly>"],
  [/<Tab\b[^>]*\ssdk="([a-z]+)"[^>]*>/g, "</Tab>"],
];

/** `[start, end, sdk]` for every SDK-scoped region, outermost match first. */
function sdkScopes(raw) {
  const ranges = [];
  for (const [opener, closer] of SCOPES) {
    opener.lastIndex = 0;
    for (const match of raw.matchAll(opener)) {
      const from = match.index + match[0].length;
      const to = raw.indexOf(closer, from);
      ranges.push([from, to === -1 ? raw.length : to, match[1]]);
    }
  }
  return ranges;
}

/** The SDKs a link at `at` can render for: one if scoped, all otherwise. */
function sdksAt(ranges, at) {
  const scope = ranges.find(([from, to]) => at >= from && at < to);
  return scope ? [scope[2]] : SDK_IDS;
}
const APP_LITERAL = /["'`](\/[a-z0-9][a-z0-9/_.#-]*)["'`]/gi;

function isAsset(path) {
  return /\.[a-z0-9]+$/i.test(path);
}

function normalize(path) {
  const clean = path.split("#")[0].split("?")[0];
  return clean.replace(/\/$/, "") || "/";
}

function docRootOf(path) {
  return path.split("/")[1];
}

function appFiles() {
  const out = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (/\.tsx?$/.test(entry.name)) {
        out.push(full);
      }
    }
  };
  walk(APP_DIR);
  return out
    .map((full) => ({
      rel: `app/${relative(APP_DIR, full).split(sep).join("/")}`,
      raw: readFileSync(full, "utf8"),
    }))
    .filter((file) => !APP_EXEMPT.some((skip) => file.rel === `app/${skip}`));
}

/** Every (source, target) an internal link produces, already SDK-resolved. */
function* linksIn(files) {
  for (const file of files) {
    if (!file.rel.endsWith(".mdx")) {
      continue;
    }
    for (const pattern of [MD_LINK, HREF]) {
      for (const [, raw] of file.raw.matchAll(pattern)) {
        yield [file.rel, raw];
      }
    }
    const scopes = sdkScopes(file.raw);
    for (const match of file.raw.matchAll(SDK_LINK)) {
      const suffix = match[1].startsWith("/") ? match[1] : `/${match[1]}`;
      for (const sdk of sdksAt(scopes, match.index)) {
        yield [file.rel, `/${sdk}${suffix}`];
      }
    }
  }
  for (const file of appFiles()) {
    for (const [, path] of file.raw.matchAll(APP_LITERAL)) {
      const segments = normalize(path).split("/").length - 1;
      if (DOC_ROOTS.includes(docRootOf(path)) && segments >= MIN_APP_SEGMENTS) {
        yield [file.rel, path];
      }
    }
  }
}

export function checkLinks(files) {
  const errors = [];
  const slugs = new Set();
  for (const file of files) {
    if (file.rel.endsWith(".mdx")) {
      for (const { slug } of mountsForRelPath(file.rel)) {
        slugs.add(slug);
      }
    }
  }

  let checked = 0;
  const seen = new Set();
  for (const [source, raw] of linksIn(files)) {
    if (isAsset(raw)) {
      continue;
    }
    const target = normalize(raw);
    checked += 1;
    const key = `${source} ${target}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    if (REDIRECTS[target]) {
      errors.push(
        `${source}: links to ${target}, which moved to ${REDIRECTS[target]} — link the destination, not the stub`,
      );
    } else if (!slugs.has(target)) {
      errors.push(`${source}: links to ${target}, which is not a page`);
    }
  }

  return {
    name: "Internal links",
    errors,
    report: [`  ${checked} internal links across content and app`],
  };
}
