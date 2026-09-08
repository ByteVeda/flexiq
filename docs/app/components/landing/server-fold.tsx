import { Link } from "react-router";
import { RawHtml } from "@/components/ui";
import { useActiveSdk } from "@/hooks";
import { highlightShell } from "@/lib/highlight-lite";
import { SERVER_PANES, type ServerPane } from "@/lib/landing-content";
import { CopyButton } from "./copy-button";
import { SectionHead } from "./sections";

/**
 * The one place on the docs index that says FlexiQ has a network door.
 *
 * Everything above this reads as an embedded library — a process that opens the
 * database itself — which leaves a reader whose producer has no native binding
 * with no reason to keep going. Two transcripts answer that and then hand off:
 * the trade-off belongs to `modules/server`, which argues it properly, so the
 * closing line links there rather than restating the case.
 */
export function ServerFold() {
  const sdk = useActiveSdk();

  return (
    <section className="section srv" id="server-mode">
      <div className="wrap">
        <SectionHead
          kicker="Server mode"
          title={
            <>
              Enqueue from{" "}
              <span
                style={{
                  color: "var(--indigo-br)",
                  fontFamily: "var(--mono)",
                  fontSize: ".84em",
                }}
              >
                curl
              </span>
              , not just an SDK
            </>
          }
          lead="flexiq-server puts the producer API on the network: gRPC, and the same calls as plain HTTP with JSON bodies on the same listener. A shell script enqueues onto the queue your workers already drain — no binding to install, and no database credential of its own."
        />

        <div className="srv-panes">
          {SERVER_PANES.map((pane) => (
            <Pane key={pane.filename} pane={pane} />
          ))}
        </div>

        <div className="srv-note reveal">
          <p>
            Embedded mode stays the default — one process, straight to the
            database — and for a single-language deployment that is still the
            better door.
          </p>
          <div className="doclinks">
            <Link className="doclink" to={`/${sdk}/modules/server`}>
              When to reach for server mode <Arrow />
            </Link>
            <Link className="doclink" to={`/${sdk}/modules/clients`}>
              Write a client without an SDK <Arrow />
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}

/** One transcript: the command in a terminal card, what it printed beneath it. */
function Pane({ pane }: { pane: ServerPane }) {
  return (
    <div className="srv-pane reveal">
      <div className="term">
        <div className="tbar">
          <div className="dots">
            <i />
            <i />
            <i />
          </div>
          <div className="tabname">
            <b>{pane.filename}</b>
          </div>
          <div className="runtag">{pane.tag}</div>
          <CopyButton text={pane.code} />
        </div>
        <RawHtml as="pre" className="code" html={highlightShell(pane.code)} />
      </div>
      <div className="out">
        {pane.output.map((line) => (
          <div className="oline show" key={line.text}>
            <span className={line.glyphKind}>{line.glyph}</span>
            <span className="var">{line.text}</span>
            {line.value ? <span className="v">{line.value}</span> : null}
          </div>
        ))}
      </div>
    </div>
  );
}

function Arrow() {
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
      <path d="M5 12h14" />
      <path d="m12 5 7 7-7 7" />
    </svg>
  );
}
