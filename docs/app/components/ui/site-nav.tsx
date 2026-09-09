import { Check, ChevronDown, Menu, Search } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import { ThemeToggle } from "@/components/ui/theme-toggle";
import { useActiveTier } from "@/hooks";
import {
  isSdk,
  SERVER_TIER,
  type Tier,
  tierForPath,
  tierLabels,
  tierStore,
  tierSwitchTarget,
} from "@/lib";

// lucide dropped brand glyphs, so the GitHub mark is inlined.
function GithubMark() {
  return (
    <svg
      viewBox="0 0 24 24"
      width={17}
      height={17}
      fill="currentColor"
      aria-hidden="true"
    >
      <path d="M12 .5C5.7.5.5 5.7.5 12c0 5.1 3.3 9.4 7.9 10.9.6.1.8-.2.8-.6v-2c-3.2.7-3.9-1.5-3.9-1.5-.5-1.3-1.3-1.7-1.3-1.7-1.1-.7.1-.7.1-.7 1.2.1 1.8 1.2 1.8 1.2 1 .1.8 1.7 2.5 1.4.1-.7.4-1.2.7-1.5-2.5-.3-5.2-1.3-5.2-5.7 0-1.3.5-2.3 1.2-3.1-.1-.3-.5-1.5.1-3.1 0 0 1-.3 3.3 1.2a11.5 11.5 0 0 1 6 0C17.3 5 18.3 5.3 18.3 5.3c.6 1.6.2 2.8.1 3.1.8.8 1.2 1.8 1.2 3.1 0 4.4-2.7 5.4-5.2 5.7.4.4.8 1.1.8 2.2v3.3c0 .3.2.7.8.6a11.5 11.5 0 0 0 7.9-10.9C23.5 5.7 18.3.5 12 .5z" />
    </svg>
  );
}

// Switcher options come from the tier registry, so a new tier appears here
// automatically (add its glyph to TIER_ICONS alongside the registry row).
const TIER_LABELS = tierLabels();

/** Simplified single-color marks (lucide carries no brand glyphs). */
const TIER_ICONS: Record<Tier, React.ReactNode> = {
  python: (
    // The two-snake mark, monochrome.
    <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M11.9 1.8c-1.3 0-2.5.1-3.6.3-1.6.3-2.5 1.2-2.5 2.6v2.1h6v.9H4.9c-1.5 0-2.9 1-2.9 4.2 0 3.3 1.4 4.3 2.9 4.3h1.7v-2.4c0-1.7 1.5-3.2 3.2-3.2h4.4c1.4 0 2.5-1.2 2.5-2.6V4.7c0-1.4-1-2.3-2.4-2.6-1-.2-1.6-.3-2.4-.3zM9.1 3.5c.5 0 .9.4.9.9s-.4.9-.9.9-.9-.4-.9-.9.4-.9.9-.9z" />
      <path d="M12.1 22.2c1.3 0 2.5-.1 3.6-.3 1.6-.3 2.5-1.2 2.5-2.6v-2.1h-6v-.9h7c1.5 0 2.9-1 2.9-4.2 0-3.3-1.4-4.3-2.9-4.3h-1.7v2.4c0 1.7-1.5 3.2-3.2 3.2H9.9c-1.4 0-2.5 1.2-2.5 2.6v3.3c0 1.4 1 2.3 2.4 2.6 1 .2 1.6.3 2.3.3zm2.8-1.7c-.5 0-.9-.4-.9-.9s.4-.9.9-.9.9.4.9.9-.4.9-.9.9z" />
    </svg>
  ),
  node: (
    // The hexagon mark.
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M12 2.2 20.5 7v10L12 21.8 3.5 17V7z" />
    </svg>
  ),
  java: (
    // The coffee cup, monochrome.
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      aria-hidden="true"
    >
      <path d="M17 10h1.5a2.5 2.5 0 0 1 0 5H17" />
      <path d="M4 10h13v6a4 4 0 0 1-4 4H8a4 4 0 0 1-4-4z" />
      <path d="M8 2.5c-1 1.2-1 2.3 0 3.5M12 2.5c-1 1.2-1 2.3 0 3.5" />
    </svg>
  ),
  [SERVER_TIER]: (
    // Two stacked rack units — the one option here that is a process, not a
    // language.
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="3" y="4" width="18" height="7" rx="1.5" />
      <rect x="3" y="13" width="18" height="7" rx="1.5" />
      <path d="M7 7.5h.01M7 16.5h.01" />
    </svg>
  ),
};

/** Global tier dropdown ("Docs for"). The choice goes to `tierStore`, which
 *  routes a language to the SDK store (flipping inline variants and the docs
 *  nav) and holds anything else itself — `<html data-sdk>` has no meaning for a
 *  tier that is not a language and would blank every `<SdkOnly>` on the page. A
 *  custom listbox so each option can carry its glyph. */
function TierSelect() {
  const tier = useActiveTier();
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  // Close on click-outside and on Escape.
  useEffect(() => {
    if (!open) {
      return;
    }
    function onPointerDown(event: PointerEvent) {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
      }
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  function select(target: Tier) {
    setOpen(false);
    if (target === tier) {
      return;
    }
    tierStore.set(target);
    // A page in no tier (`/architecture/*`, `/about/*`) stays put when the
    // choice is a language — the page is the same one either way. The server
    // tier is the exception: it is a destination, not a variant of this page.
    if (target === SERVER_TIER || tierForPath(pathname)) {
      navigate(tierSwitchTarget(pathname, target));
    }
  }

  const active = TIER_LABELS.find((l) => l.id === tier) ?? TIER_LABELS[0];
  return (
    <div className="sdk-dd" ref={rootRef}>
      {/* "Docs for", not "Choose SDK": one of the four options is a binary. */}
      <span className="sdk-dd-label" id="sdk-dd-label">
        Docs for
      </span>
      <button
        type="button"
        className="sdk-dd-btn"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-labelledby="sdk-dd-label"
        onClick={() => setOpen((o) => !o)}
      >
        <span className="sdk-dd-icon">{TIER_ICONS[active.id]}</span>
        <span className="sdk-dd-name">{active.label}</span>
        <ChevronDown className="sdk-caret" size={13} aria-hidden="true" />
      </button>
      {open ? (
        <div className="sdk-dd-menu" role="listbox" aria-label="Documentation">
          {TIER_LABELS.map(({ id, label }) => (
            <button
              key={id}
              type="button"
              role="option"
              aria-selected={id === tier}
              className={`sdk-dd-opt ${id === tier ? "active" : ""}`.trim()}
              onClick={() => select(id)}
            >
              <span className="sdk-dd-icon">{TIER_ICONS[id]}</span>
              <span>{label}</span>
              {id === tier ? (
                <Check className="sdk-dd-check" size={13} aria-hidden="true" />
              ) : null}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

interface NavLink {
  label: string;
  /** Relative to the tier's own prefix in the two per-tier lists; absolute in
   *  `SHARED_LINKS`. */
  href: string;
}

/**
 * Deliberately short. The sidebar already lists every top-level section on the
 * same screen, so the bar carries only what a reader reaches for out of order:
 * the one concept page worth reading before anything else, the two references
 * you jump to mid-task, and what changed.
 */
const SDK_LINKS: NavLink[] = [
  { label: "Concepts", href: "getting-started/concepts" },
  { label: "API", href: "api-reference" },
  { label: "Examples", href: "more/examples" },
];

/**
 * The same three slots for the server tier, which has none of those pages: the
 * two doors as a caller meets them — producer side, executor side — and then
 * running one.
 *
 * It needs its own list rather than the SDK one resolved against the stored
 * language, which is what put `/python/…` in the bar of a page whose sidebar,
 * breadcrumb and switcher all said `flexiq-server`.
 */
const SERVER_LINKS: NavLink[] = [
  { label: "Clients", href: "clients" },
  { label: "Executors", href: "custom-executors" },
  { label: "Operate", href: "operate" },
];

/** Tier-neutral pages: the same link from whichever door you came through. */
const SHARED_LINKS: NavLink[] = [
  { label: "Changelog", href: "/about/changelog" },
];

/** The bar for one tier: its own three under its prefix, then the shared ones. */
function navLinks(tier: Tier): NavLink[] {
  const own = isSdk(tier) ? SDK_LINKS : SERVER_LINKS;
  return [
    ...own.map((l) => ({ label: l.label, href: `/${tier}/${l.href}` })),
    ...SHARED_LINKS,
  ];
}

/** Sticky top navigation, shared by the landing and docs shells. `onMenu` is
 *  passed only by the docs shell — it renders the mobile button that opens the
 *  sidebar drawer (the landing has no sidebar, so it omits it). */
export function SiteNav({
  onSearch,
  onMenu,
  showTierSelect = true,
}: {
  onSearch?: () => void;
  onMenu?: () => void;
  // Landing hides it — the hero language tabs already own SDK selection there.
  showTierSelect?: boolean;
}) {
  // The tier, not the SDK: on `/server/*` the SDK is whatever language the
  // reader last picked, and resolving the bar against it links out of the tier.
  // On the landing and on any tierless page the two are the same value.
  const tier = useActiveTier();
  // Basename-relative, so this stays `/` under DOCS_BASE_PATH too.
  const atRoot = useLocation().pathname === "/";
  return (
    <nav className="nav">
      {onMenu ? (
        <button
          type="button"
          className="menu-btn"
          onClick={onMenu}
          aria-label="Open navigation menu"
        >
          <Menu size={18} />
        </button>
      ) : null}
      {/* The mark alone on the index — the hero right beneath it already says
          FlexiQ. On every other page the brand is the way back, so it says so.
          The label carries the accessible name either way; the image is
          decorative. */}
      <Link
        to="/"
        className="brand"
        aria-label={atRoot ? "FlexiQ documentation" : "Home"}
      >
        <img
          className="logo"
          src={`${import.meta.env.BASE_URL}logo.png`}
          alt=""
          aria-hidden="true"
        />
        {atRoot ? null : <span className="home">Home</span>}
      </Link>
      <div className="navlinks">
        {navLinks(tier).map((l) => (
          <Link key={l.href} to={l.href}>
            {l.label}
          </Link>
        ))}
      </div>
      <div className="navright">
        {showTierSelect ? <TierSelect /> : null}
        <button type="button" className="kbar" onClick={onSearch}>
          <Search size={14} />
          <span>Search</span>
          <kbd>⌘K</kbd>
        </button>
        <ThemeToggle />
        <a
          className="icon-btn"
          href="https://github.com/ByteVeda/flexiq"
          aria-label="GitHub repository"
          target="_blank"
          rel="noreferrer"
        >
          <GithubMark />
        </a>
      </div>
    </nav>
  );
}
