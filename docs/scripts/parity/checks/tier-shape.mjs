import { mountsForRelPath } from "../../../app/lib/doc-slugs.ts";
import { SERVER_TIER, tierProfile } from "../../../app/lib/tier-registry.ts";

// (h) The server tier's nav lists exactly what the server tier holds (#825).
//
// `section-shape.mjs` cannot cover this: it exists to hold three trees in
// agreement with one skeleton, and the server tier has one tree. What it still
// needs is the other half of that gate — the sidebar is built from meta.json
// alone, so a page nobody listed renders nowhere and a listed page that resolves
// to no MDX silently drops out. Both are the same silent failure the SDK trees
// are already protected from.
//
// Three ways the tier can be wrong, all of them errors:
//   - a section directory in the registry has no meta.json;
//   - `pages` names something that resolves to no page;
//   - an MDX file sits in the section unlisted.

const SUFFIX = "/meta.json";

/** The tier's own section dirs — `architecture` and `about` are shared with the
 *  SDK trees and are not this check's business. */
function tierSections() {
  return tierProfile(SERVER_TIER).navSections.filter(
    (dir) => dir === SERVER_TIER || dir.startsWith(`${SERVER_TIER}/`),
  );
}

export function checkTierShape(files) {
  const errors = [];
  const slugs = new Set();
  const metas = new Map();
  for (const file of files) {
    if (file.rel.endsWith(".mdx")) {
      for (const { slug } of mountsForRelPath(file.rel)) {
        slugs.add(slug);
      }
    } else if (file.rel.endsWith(SUFFIX)) {
      metas.set(file.rel.slice(0, -SUFFIX.length), JSON.parse(file.raw));
    }
  }

  const sections = tierSections();
  for (const dir of sections) {
    const meta = metas.get(dir);
    if (!meta) {
      errors.push(
        `${dir}/meta.json is missing — the ${SERVER_TIER} tier's sidebar is built from it`,
      );
      continue;
    }
    const pages = meta.pages ?? [];
    const hasIndex = slugs.has(`/${dir}`);
    if (hasIndex !== pages.includes("index")) {
      errors.push(
        hasIndex
          ? `${dir}/meta.json omits its own "index" page`
          : `${dir}/meta.json lists "index" but ${dir}/index.mdx does not exist`,
      );
    }
    for (const page of pages) {
      if (page !== "index" && !slugs.has(`/${dir}/${page}`)) {
        errors.push(
          `${dir}/meta.json lists "${page}", which resolves to no page`,
        );
      }
    }
    const prefix = `/${dir}/`;
    for (const slug of slugs) {
      if (!slug.startsWith(prefix)) {
        continue;
      }
      const name = slug.slice(prefix.length);
      // A nested section is a sibling group in the registry, not a child entry
      // of this one, so it is listed there instead.
      if (
        !name.includes("/") &&
        !pages.includes(name) &&
        !sections.includes(`${dir}/${name}`)
      ) {
        errors.push(
          `${slug} is not listed in ${dir}/meta.json — it renders nowhere in the nav`,
        );
      }
    }
  }

  return {
    name: `Tier shape (${SERVER_TIER})`,
    errors,
    report: [`  ${sections.length} sections`],
  };
}
