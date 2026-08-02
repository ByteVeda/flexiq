import { Link } from "react-router";
import { useActiveSdk } from "@/hooks";
import { VERSION } from "@/lib/version";

type FootLink = {
  label: string;
  /** When `sdk` is set, this is an SDK-relative path (prefixed with /python|/node). */
  href: string;
  external?: boolean;
  sdk?: boolean;
};

const COLS: { title: string; links: FootLink[] }[] = [
  {
    title: "Docs",
    links: [
      {
        label: "Getting Started",
        href: "getting-started/installation",
        sdk: true,
      },
      { label: "Guides", href: "guides", sdk: true },
      { label: "Architecture", href: "/architecture" },
      { label: "API Reference", href: "api-reference", sdk: true },
    ],
  },
  {
    title: "More",
    links: [
      { label: "Examples", href: "more/examples", sdk: true },
      { label: "Celery comparison", href: "/resources/comparison" },
      { label: "FAQ", href: "/resources/faq" },
      { label: "Changelog", href: "/resources/changelog" },
    ],
  },
  {
    title: "Project",
    links: [
      {
        label: "GitHub",
        href: "https://github.com/ByteVeda/taskito",
        external: true,
      },
      {
        label: "Releases",
        href: "https://github.com/ByteVeda/taskito/releases",
        external: true,
      },
      {
        label: "Issues",
        href: "https://github.com/ByteVeda/taskito/issues",
        external: true,
      },
      {
        label: "PyPI",
        href: "https://pypi.org/project/taskito/",
        external: true,
      },
    ],
  },
];

export function Footer() {
  const sdk = useActiveSdk();
  return (
    <footer className="foot">
      <div className="foot-grid">
        <div className="foot-brand">
          <span className="brand">
            <img
              className="logo"
              src={`${import.meta.env.BASE_URL}logo.png`}
              alt=""
              aria-hidden="true"
            />
            <span className="brand-word">
              taski<span className="bto">to</span>
            </span>
          </span>
          <p>
            A Rust-powered task queue with native SDKs. One engine, no broker
            required — just SQLite, Postgres, or Redis.
          </p>
        </div>
        {COLS.map((col) => (
          <div key={col.title} className="foot-col">
            <h4>{col.title}</h4>
            {col.links.map((l) =>
              l.external ? (
                <a key={l.label} href={l.href} target="_blank" rel="noreferrer">
                  {l.label}
                </a>
              ) : (
                <Link key={l.label} to={l.sdk ? `/${sdk}/${l.href}` : l.href}>
                  {l.label}
                </Link>
              ),
            )}
          </div>
        ))}
      </div>
      <div className="foot-bottom">
        <span>© 2026 ByteVeda · taskito</span>
        <span>MIT License · v{VERSION}</span>
      </div>
    </footer>
  );
}
