import { useEffect, useState } from "react";
import { useLocation } from "react-router";
import { useActiveSdk } from "@/hooks";
import type { Sdk } from "@/lib";

interface Heading {
  id: string;
  text: string;
  level: number;
}

/** Headings inside an inactive `SdkOnly` block ship in the HTML but are hidden
 *  via CSS, so skip any heading whose nearest `data-sdk-variant` ancestor isn't
 *  the active SDK — otherwise the TOC lists every SDK's questions at once. */
function visibleHeadingEls(article: Element, sdk: Sdk): HTMLElement[] {
  return [...article.querySelectorAll<HTMLElement>("h2[id], h3[id]")].filter(
    (el) => {
      const variant = el.closest("[data-sdk-variant]");
      return !variant || variant.getAttribute("data-sdk-variant") === sdk;
    },
  );
}

function toHeading(el: HTMLElement): Heading {
  return {
    id: el.id,
    text: el.textContent ?? "",
    level: el.tagName === "H3" ? 3 : 2,
  };
}

/** On-this-page TOC, built from the rendered article headings with scroll-spy. */
export function Toc() {
  const { pathname } = useLocation();
  const sdk = useActiveSdk();
  const [headings, setHeadings] = useState<Heading[]>([]);
  const [active, setActive] = useState<string>("");

  // The article is a lazily-loaded MDX chunk, so its headings are NOT in the DOM
  // when this effect first runs after a client-side navigation. Re-scan on every
  // article mutation (via MutationObserver) so the TOC fills in once the page
  // content actually mounts, and re-arm scroll-spy whenever the heading set changes.
  // biome-ignore lint/correctness/useExhaustiveDependencies: re-scan on path or SDK change
  useEffect(() => {
    const article = document.querySelector(".article");
    if (!article) {
      setHeadings([]);
      return;
    }
    let spy: IntersectionObserver | null = null;
    let lastKey = "";

    const scan = () => {
      const els = visibleHeadingEls(article, sdk);
      const next = els.map(toHeading);
      const key = next.map((h) => h.id).join("|");
      if (key === lastKey) {
        return;
      }
      lastKey = key;
      setHeadings(next);
      spy?.disconnect();
      if (!next.length) {
        return;
      }
      spy = new IntersectionObserver(
        (entries) => {
          for (const entry of entries) {
            if (entry.isIntersecting) {
              setActive(entry.target.id);
            }
          }
        },
        { rootMargin: "-80px 0px -70% 0px" },
      );
      for (const el of els) {
        spy.observe(el);
      }
    };

    scan();
    const content = new MutationObserver(scan);
    content.observe(article, { childList: true, subtree: true });
    return () => {
      content.disconnect();
      spy?.disconnect();
    };
  }, [pathname, sdk]);

  if (headings.length === 0) {
    return <aside className="toc" aria-hidden="true" />;
  }

  return (
    <aside className="toc">
      {/* h2, not h4: this sits after the article's headings in document order,
          so anything below the last content level reads as a skipped level. */}
      <h2>On this page</h2>
      <div id="toc-list">
        {headings.map((h) => (
          <a
            key={h.id}
            href={`#${h.id}`}
            className={`toc-link ${h.level === 3 ? "sub" : ""} ${h.id === active ? "active" : ""}`.trim()}
          >
            {/* Top-level entries are auto-numbered by CSS counter; strip any
                authored "N." prefix (some pages number their own headings) so
                the number isn't shown twice. */}
            {h.level === 2 ? h.text.replace(/^\d+[.)]\s+/, "") : h.text}
          </a>
        ))}
      </div>
    </aside>
  );
}
