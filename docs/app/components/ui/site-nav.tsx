import { Check, ChevronDown, Menu, Search } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import { ThemeToggle } from "@/components/ui/theme-toggle";
import { useActiveSdk, useSdk } from "@/hooks";
import { forcedSdkForPath, type Sdk, sdkLabels, sdkSwitchTarget } from "@/lib";

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

// Switcher options come from the SDK registry, so a new language appears here
// automatically (add its glyph to SDK_ICONS alongside the registry row).
const SDK_LABELS = sdkLabels();

/** Simplified single-color language marks (lucide carries no brand glyphs). */
const SDK_ICONS: Record<Sdk, React.ReactNode> = {
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
};

/** Global SDK dropdown ("Choose SDK"). Sets the shared store (flips inline
 *  variants + the docs nav); on an SDK-specific page it also navigates to the
 *  counterpart page, on a shared page it stays put. A custom listbox so each
 *  option can carry its language glyph. */
function SdkSelect() {
  const { sdk, setSdk } = useSdk();
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

  function select(target: Sdk) {
    setOpen(false);
    if (target === sdk) {
      return;
    }
    setSdk(target);
    if (forcedSdkForPath(pathname)) {
      navigate(sdkSwitchTarget(pathname, target));
    }
  }

  const active = SDK_LABELS.find((l) => l.id === sdk) ?? SDK_LABELS[0];
  return (
    <div className="sdk-dd" ref={rootRef}>
      <span className="sdk-dd-label" id="sdk-dd-label">
        Choose SDK
      </span>
      <button
        type="button"
        className="sdk-dd-btn"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-labelledby="sdk-dd-label"
        onClick={() => setOpen((o) => !o)}
      >
        <span className="sdk-dd-icon">{SDK_ICONS[active.id]}</span>
        <span className="sdk-dd-name">{active.label}</span>
        <ChevronDown className="sdk-caret" size={13} aria-hidden="true" />
      </button>
      {open ? (
        <div className="sdk-dd-menu" role="listbox" aria-label="SDK">
          {SDK_LABELS.map(({ id, label }) => (
            <button
              key={id}
              type="button"
              role="option"
              aria-selected={id === sdk}
              className={`sdk-dd-opt ${id === sdk ? "active" : ""}`.trim()}
              onClick={() => select(id)}
            >
              <span className="sdk-dd-icon">{SDK_ICONS[id]}</span>
              <span>{label}</span>
              {id === sdk ? (
                <Check className="sdk-dd-check" size={13} aria-hidden="true" />
              ) : null}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

// `sdk` links are SDK-relative (prefixed with the active /python|/node); the rest
// are shared, SDK-neutral pages.
const LINKS: { label: string; href: string; sdk?: boolean }[] = [
  { label: "Getting Started", href: "getting-started/installation", sdk: true },
  { label: "Guides", href: "guides", sdk: true },
  { label: "Architecture", href: "/architecture" },
  { label: "API", href: "api-reference", sdk: true },
  { label: "Changelog", href: "/resources/changelog" },
];

/** Sticky top navigation, shared by the landing and docs shells. `onMenu` is
 *  passed only by the docs shell — it renders the mobile button that opens the
 *  sidebar drawer (the landing has no sidebar, so it omits it). */
export function SiteNav({
  onSearch,
  onMenu,
  showSdkSelect = true,
}: {
  onSearch?: () => void;
  onMenu?: () => void;
  // Landing hides it — the hero language tabs already own SDK selection there.
  showSdkSelect?: boolean;
}) {
  const sdk = useActiveSdk();
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
      <Link to="/" className="brand">
        <img
          className="logo"
          src={`${import.meta.env.BASE_URL}logo.png`}
          alt=""
          aria-hidden="true"
        />
        <span>
          task<span className="bto">ito</span>
        </span>
      </Link>
      <div className="navlinks">
        {LINKS.map((l) => (
          <Link key={l.href} to={l.sdk ? `/${sdk}/${l.href}` : l.href}>
            {l.label}
          </Link>
        ))}
      </div>
      <div className="navright">
        {showSdkSelect ? <SdkSelect /> : null}
        <button type="button" className="kbar" onClick={onSearch}>
          <Search size={14} />
          <span>Search</span>
          <kbd>⌘K</kbd>
        </button>
        <ThemeToggle />
        <a
          className="icon-btn"
          href="https://github.com/ByteVeda/taskito"
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
