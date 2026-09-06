import { Card, Cards } from "@/components/mdx/card";
import { useActiveSdk } from "@/hooks";
import { type NavNode, navForSdk } from "@/lib";
import { docMeta } from "@/lib/manifest";

/**
 * The site root's map of the documentation.
 *
 * Built from `navForSdk` — the same section list the sidebar renders, which is
 * itself `SDK_PROFILES[sdk].navSections` — so a section added to the registry
 * appears here without a second edit. Descriptions come from each section index
 * page's own frontmatter rather than being restated.
 *
 * Deliberately headless. Eight labelled cards on a page titled "FlexiQ
 * documentation" do not need a heading telling the reader they are looking at
 * documentation, and the page already carries three section heads above this.
 */
export function SectionGrid() {
  const sdk = useActiveSdk();
  const sections = navForSdk(sdk);

  return (
    <section className="section" id="docs-map">
      <div className="wrap">
        <div className="reveal">
          <Cards>
            {sections.map((section) => {
              const href = sectionHref(section);
              if (!href) {
                return null;
              }
              return (
                <Card
                  key={section.title}
                  title={section.title}
                  href={href}
                  description={docMeta(href)?.description}
                />
              );
            })}
          </Cards>
        </div>
      </div>
    </section>
  );
}

/**
 * Where a section card points. A section has its own `href` only when it has an
 * index page; the rest are directories, so fall through to the first page they
 * actually contain rather than dropping the section from the map.
 */
function sectionHref(node: NavNode): string | undefined {
  if (node.href) {
    return node.href;
  }
  for (const child of node.children ?? []) {
    const found = sectionHref(child);
    if (found) {
      return found;
    }
  }
  return undefined;
}
