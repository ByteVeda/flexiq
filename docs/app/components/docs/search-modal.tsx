import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router";
import type { Sdk } from "@/hooks";
import {
  browseDocs,
  prefetchSearchIndex,
  type SearchHit,
  searchDocs,
} from "@/lib/search";

// Section glyphs mirror the prototype's command-palette icons.
const SECTION_ICON: { match: string; path: string }[] = [
  { match: "getting", path: "M13 2L3 14h7l-1 8 10-12h-7l1-8z" },
  {
    match: "guide",
    path: "M4 19.5A2.5 2.5 0 0 1 6.5 17H20M4 19.5A2.5 2.5 0 0 0 6.5 22H20V2H6.5A2.5 2.5 0 0 0 4 4.5v15z",
  },
  {
    match: "architecture",
    path: "M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5",
  },
  { match: "api", path: "M16 18l6-6-6-6M8 6l-6 6 6 6" },
  { match: "node", path: "M12 2l8.5 5v10L12 22 3.5 17V7z" },
  { match: "reference", path: "M16 18l6-6-6-6M8 6l-6 6 6 6" },
];

function sectionIconPath(section: string): string {
  const key = section.toLowerCase();
  return (
    SECTION_ICON.find((s) => key.includes(s.match))?.path ??
    "M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20z"
  );
}

function SectionIcon({ section }: { section: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={sectionIconPath(section)} />
    </svg>
  );
}

interface Group {
  section: string;
  items: SearchHit[];
}

/** Group ranked hits by section, preserving first-appearance order. */
function groupBySection(hits: SearchHit[]): Group[] {
  const groups: Group[] = [];
  const byName = new Map<string, Group>();
  for (const hit of hits) {
    let group = byName.get(hit.section);
    if (!group) {
      group = { section: hit.section, items: [] };
      byName.set(hit.section, group);
      groups.push(group);
    }
    group.items.push(hit);
  }
  return groups;
}

/** ⌘K command palette: browse-by-section on open, ranked search as you type. */
export function SearchModal({
  open,
  onClose,
  sdk,
}: {
  open: boolean;
  onClose: () => void;
  sdk?: Sdk;
}) {
  const navigate = useNavigate();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const listRef = useRef<HTMLDivElement>(null);

  // Browsing is synchronous (it reads the manifest already in memory); a real
  // query awaits the code-split full-text index, so results arrive as state.
  const [hits, setHits] = useState<SearchHit[]>(() => browseDocs(sdk));
  const [pending, setPending] = useState(false);
  const groups = useMemo(() => groupBySection(hits), [hits]);
  const flat = useMemo(() => groups.flatMap((g) => g.items), [groups]);

  // Warm the index chunk while the user is still reaching for the keyboard.
  useEffect(() => {
    if (open) {
      prefetchSearchIndex();
    }
  }, [open]);

  useEffect(() => {
    if (!query.trim()) {
      setPending(false);
      setHits(browseDocs(sdk));
      return;
    }
    let current = true;
    setPending(true);
    searchDocs(query, sdk).then(
      (results) => {
        if (current) {
          setHits(results);
          setPending(false);
        }
      },
      () => {
        if (current) {
          setHits([]);
          setPending(false);
        }
      },
    );
    // A slower earlier query must not overwrite a later one's results.
    return () => {
      current = false;
    };
  }, [query, sdk]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: reset selection as results change
  useEffect(() => {
    setActive(0);
  }, [query]);

  useEffect(() => {
    if (!open) {
      setQuery("");
    }
  }, [open]);

  // Keep the active row in view during keyboard navigation.
  useEffect(() => {
    listRef.current
      ?.querySelector(`[data-idx="${active}"]`)
      ?.scrollIntoView({ block: "nearest" });
  }, [active]);

  if (!open) {
    return null;
  }

  const go = (slug: string) => {
    onClose();
    navigate(slug);
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      onClose();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((i) => Math.min(i + 1, flat.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && flat[active]) {
      go(flat[active].id);
    }
  };

  let idx = -1;
  return (
    <div
      className="cmdk"
      role="dialog"
      aria-modal="true"
      aria-label="Search docs"
    >
      <button
        type="button"
        className="cmdk-backdrop"
        aria-label="Close search"
        onClick={onClose}
      />
      {/* biome-ignore lint/a11y/noStaticElementInteractions: key handling for the palette */}
      <div className="cmdk-panel" onKeyDown={onKeyDown}>
        <div className="cmdk-inputrow">
          <svg
            className="cmdk-ic"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth={2}
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.3-4.3" />
          </svg>
          <input
            // biome-ignore lint/a11y/noAutofocus: expected focus for a command palette
            autoFocus
            className="cmdk-input"
            type="text"
            placeholder="Search the docs…"
            autoComplete="off"
            spellCheck={false}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <button type="button" className="cmdk-esc" onClick={onClose}>
            esc
          </button>
        </div>
        <div className="cmdk-results" role="listbox" ref={listRef}>
          {flat.length === 0 ? (
            <div className="cmdk-empty">
              {pending ? "Searching…" : `No results for “${query}”.`}
            </div>
          ) : (
            groups.map((group) => (
              <Fragment key={group.section}>
                <div className="cmdk-group">{group.section}</div>
                {group.items.map((hit) => {
                  idx += 1;
                  const i = idx;
                  return (
                    <button
                      key={hit.id}
                      type="button"
                      data-idx={i}
                      className={`cmdk-item ${i === active ? "active" : ""}`.trim()}
                      onMouseEnter={() => setActive(i)}
                      onClick={() => go(hit.id)}
                    >
                      <span className="cmdk-it-ic">
                        <SectionIcon section={group.section} />
                      </span>
                      <span className="cmdk-it-body">
                        <span className="cmdk-it-t">{hit.title}</span>
                        <span className="cmdk-it-d">{hit.description}</span>
                      </span>
                      <span className="cmdk-it-go">↵</span>
                    </button>
                  );
                })}
              </Fragment>
            ))
          )}
        </div>
        <div className="cmdk-foot">
          <span>
            <kbd>↑</kbd>
            <kbd>↓</kbd> navigate
          </span>
          <span>
            <kbd>↵</kbd> open
          </span>
          <span>
            <kbd>esc</kbd> close
          </span>
        </div>
      </div>
    </div>
  );
}
