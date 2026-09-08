import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router";
import { useActiveSdk } from "@/hooks";
import { type NavNode, navForSdk } from "@/lib";

function containsHref(node: NavNode, current: string): boolean {
  return (
    node.href === current ||
    (node.children?.some((c) => containsHref(c, current)) ?? false)
  );
}

function Caret({
  open,
  onToggle,
  title,
}: {
  open: boolean;
  onToggle: () => void;
  title: string;
}) {
  return (
    <button
      type="button"
      className="nav-caret"
      data-open={open || undefined}
      aria-expanded={open}
      aria-label={`${open ? "Collapse" : "Expand"} ${title}`}
      onClick={onToggle}
    >
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M9 18l6-6-6-6" />
      </svg>
    </button>
  );
}

function NavLink({ node, current }: { node: NavNode; current: string }) {
  if (!node.href) {
    return <span className="nav-item nav-item-label">{node.title}</span>;
  }
  return (
    <Link
      to={node.href}
      className={`nav-item ${node.href === current ? "active" : ""}`.trim()}
    >
      {node.title}
    </Link>
  );
}

/** A subsection with children — collapsible at any depth, auto-opens around the
 *  active page. A subsection with an index page links to it and leaves toggling
 *  to the caret; one without has no destination, so its label toggles instead. */
function NavSection({ node, current }: { node: NavNode; current: string }) {
  // Includes `node.href === current`, so landing on a subsection's own index
  // page opens it rather than showing a collapsed group you are already inside.
  const active = containsHref(node, current);
  const [open, setOpen] = useState(active);
  useEffect(() => {
    if (active) {
      setOpen(true);
    }
  }, [active]);
  const toggle = () => setOpen((o) => !o);
  const label = `nav-item nav-sub-toggle ${
    node.href === current ? "active" : ""
  }`.trim();
  return (
    <div className="nav-subsection">
      <div className="nav-sub-head">
        {node.href ? (
          <Link to={node.href} className={label}>
            {node.title}
          </Link>
        ) : (
          <button
            type="button"
            className={label}
            aria-expanded={open}
            onClick={toggle}
          >
            {node.title}
          </button>
        )}
        <Caret open={open} onToggle={toggle} title={node.title} />
      </div>
      {open ? <NavTree nodes={node.children ?? []} current={current} /> : null}
    </div>
  );
}

function NavTree({ nodes, current }: { nodes: NavNode[]; current: string }) {
  return (
    <div className="nav-sub">
      {nodes.map((node) =>
        node.children?.length ? (
          <NavSection key={node.title} node={node} current={current} />
        ) : (
          <NavLink
            key={node.href ?? node.title}
            node={node}
            current={current}
          />
        ),
      )}
    </div>
  );
}

/** Top-level group — collapsible, default-open for the section holding the page.
 *
 *  The header links to the section's own index page when there is one, and the
 *  caret beside it does the expanding. Every section but `about` has an index,
 *  and while the header was a toggle those pages were reachable from the section
 *  grid and prev/next but from nowhere in the sidebar. */
function NavGroup({ group, current }: { group: NavNode; current: string }) {
  const active = containsHref(group, current);
  const [open, setOpen] = useState(active);
  useEffect(() => {
    if (active) {
      setOpen(true);
    }
  }, [active]);
  const hasChildren = Boolean(group.children?.length);
  return (
    <div className="nav-group">
      <div className="gt">
        {group.href ? (
          <Link
            to={group.href}
            className={`gt-link ${group.href === current ? "active" : ""}`.trim()}
          >
            {group.title}
          </Link>
        ) : hasChildren ? (
          <button
            type="button"
            className="gt-toggle"
            aria-expanded={open}
            onClick={() => setOpen((o) => !o)}
          >
            {group.title}
          </button>
        ) : (
          <span>{group.title}</span>
        )}
        {hasChildren ? (
          <Caret
            open={open}
            onToggle={() => setOpen((o) => !o)}
            title={group.title}
          />
        ) : null}
      </div>
      {hasChildren && open ? (
        <NavTree nodes={group.children ?? []} current={current} />
      ) : null}
    </div>
  );
}

export function Sidebar({
  onSearch,
  open = false,
  onClose,
}: {
  onSearch?: () => void;
  /** Drawer open state — only affects the mobile (≤860px) overlay layout. */
  open?: boolean;
  onClose?: () => void;
}) {
  const { pathname } = useLocation();
  const current = pathname.replace(/\/$/, "") || "/";
  const sdk = useActiveSdk();
  return (
    <>
      {/* Backdrop sits under the drawer on mobile; tapping it closes the menu. */}
      <button
        type="button"
        className={`sidebar-backdrop ${open ? "open" : ""}`.trim()}
        aria-label="Close navigation menu"
        tabIndex={open ? 0 : -1}
        onClick={onClose}
      />
      <aside className={`sidebar ${open ? "open" : ""}`.trim()}>
        <button type="button" className="side-search" onClick={onSearch}>
          Search docs
          <span className="sk">
            <kbd>⌘</kbd>
            <kbd>K</kbd>
          </span>
        </button>
        <nav id="sidenav">
          {navForSdk(sdk).map((group) => (
            <NavGroup key={group.title} group={group} current={current} />
          ))}
        </nav>
      </aside>
    </>
  );
}
