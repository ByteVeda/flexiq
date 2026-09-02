import { mountsForRelPath } from "../../../app/lib/doc-slugs.ts";
import { SDK_IDS } from "../../../app/lib/sdk-registry.ts";
import { SECTION_SKELETON } from "../section-skeleton.mjs";

// (f) One nav shape across the three SDK trees.
//
// The sidebar skeleton is exactly the kind of thing that drifts back the moment
// nobody is looking: a page lands somewhere plausible, a group gets invented to
// hold it, and six months later switching SDK reorganises the product. So the
// shape is committed (../section-skeleton.mjs) and this is blocking.
//
// Four ways a tree can be wrong, all of them errors:
//   - a section is missing, or its title differs from the other trees';
//   - `pages` names something the skeleton does not, or reorders it;
//   - a listed page resolves to no MDX (a typo silently drops it from the nav);
//   - an MDX file sits in the section unlisted, which is the same drop.

const SUFFIX = "/meta.json";

function indexOfSubsequence(pages, canonical) {
  let at = 0;
  for (const page of pages) {
    const found = canonical.indexOf(page, at);
    if (found === -1) {
      return page;
    }
    at = found + 1;
  }
  return null;
}

export function checkSectionShape(files) {
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

  for (const sdk of SDK_IDS) {
    for (const section of SECTION_SKELETON) {
      const dir = `${sdk}/${section.dir}`;
      const meta = metas.get(dir);
      if (!meta) {
        errors.push(
          `${dir}/meta.json is missing — every SDK carries every section`,
        );
        continue;
      }
      if (meta.title !== section.title) {
        errors.push(
          `${dir}/meta.json title is "${meta.title}", not "${section.title}" — an SDK omits a page, it does not rename the group`,
        );
      }
      const pages = meta.pages ?? [];
      const stray = indexOfSubsequence(pages, section.pages);
      if (stray !== null) {
        errors.push(
          `${dir}/meta.json lists "${stray}" out of order or unknown — add it to scripts/parity/section-skeleton.mjs for all three trees, or drop it`,
        );
      }
      const hasIndex = slugs.has(`/${dir}`);
      if (hasIndex !== pages.includes("index")) {
        errors.push(
          hasIndex
            ? `${dir}/meta.json omits its own "index" page`
            : `${dir}/meta.json lists "index" but ${dir}/index.mdx does not exist`,
        );
      }
      for (const page of pages) {
        if (page === "index") {
          continue;
        }
        const child = `${dir}/${page}`;
        if (!slugs.has(`/${child}`) && !metas.has(child)) {
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
        if (!name.includes("/") && !pages.includes(name)) {
          errors.push(
            `${slug} is not listed in ${dir}/meta.json — it renders nowhere in the nav`,
          );
        }
      }
    }
  }

  return {
    name: "Section shape (one skeleton, three trees)",
    errors,
    report: [`  ${SECTION_SKELETON.length} sections × ${SDK_IDS.length} SDKs`],
  };
}
