import { useState } from "react";
import { Link } from "react-router";
import { RawHtml } from "@/components/ui";
import { useSdk } from "@/hooks";
import { sdkProfile } from "@/lib";
import {
  highlightJava,
  highlightPython,
  highlightTs,
} from "@/lib/highlight-lite";
import { HERO_COMING_SOON, HERO_PANES } from "@/lib/landing-content";

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="hcopy"
      onClick={() => {
        navigator.clipboard?.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1300);
      }}
    >
      <span className="lbl">{copied ? "Copied" : "Copy"}</span>
    </button>
  );
}

export function Hero() {
  const { sdk, setSdk } = useSdk();
  // The selected snippet IS the global SDK — clicking a tab sets it, so the hero
  // copy, the install/quickstart links, and the docs sidebar switch all follow.
  const active = HERO_PANES.find((p) => p.sdk === sdk) ?? HERO_PANES[0];
  const codeHtml =
    active.lang === "ts"
      ? highlightTs(active.code)
      : active.lang === "java"
        ? highlightJava(active.code)
        : highlightPython(active.code);

  return (
    <section className="hero">
      <div className="left">
        <h1>
          Keep your app fast —{" "}
          <span className="grad">run slow work in the background.</span>
        </h1>
        <p className="sub">
          A task queue with <b>no message broker</b>. Your app hands slow work —
          sending email, processing uploads, running pipelines — to a background
          worker and gets the result later; the queue, results, and schedules
          all live in{" "}
          <b>
            one <code>SQLite</code> file
          </b>{" "}
          (scale to <code>Postgres</code>). First-class{" "}
          <b>Python, Node, and Java</b> over one Rust core.
        </p>
        <div className="btns">
          <Link className="btn pri" to={active.docHref}>
            Quickstart →
          </Link>
          <a className="btn gho" href="https://github.com/ByteVeda/flexiq">
            GitHub ↗
          </a>
        </div>
        <div className="metarow">
          <span>No broker</span>
          <span>Rust core</span>
          <span>MIT licensed</span>
          <span>Python · Node · Java</span>
        </div>
      </div>

      <div className="right">
        <div className="term">
          <div className="tbar">
            <div className="dots">
              <i />
              <i />
              <i />
            </div>
            <div className="tabname">
              <b>{active.filename}</b>
            </div>
            <div className="runtag">
              <span className="ld" />
              worker · live
            </div>
          </div>
          <div className="langtabs">
            {HERO_PANES.map((p) => (
              <button
                key={p.sdk}
                type="button"
                aria-pressed={p.sdk === sdk}
                className={`langtab ${p.sdk === sdk ? "active" : ""}`.trim()}
                onClick={() => setSdk(p.sdk)}
              >
                {sdkProfile(p.sdk).label}
              </button>
            ))}
            {HERO_COMING_SOON.map((name) => (
              <button key={name} type="button" className="langtab" disabled>
                {name}
                <span className="tag soon">Soon</span>
              </button>
            ))}
            <CopyButton text={active.code} />
          </div>
          <div id="hero-panes">
            <RawHtml as="pre" className="code" html={codeHtml} />
          </div>
        </div>

        <div className="out">
          <div className="outset">
            {active.output.map((line) => (
              <div className="oline show" key={line.text}>
                <span className={line.glyphKind}>{line.glyph}</span>
                <span className="var">{line.text}</span>
                {line.value ? <span className="v">{line.value}</span> : null}
                {line.timing ? <span className="t">{line.timing}</span> : null}
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
