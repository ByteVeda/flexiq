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
          Flexi<span className="bto">Q</span>{" "}
          <span className="grad">documentation</span>
        </h1>
        <p className="sub">
          Guides, API reference and architecture for the task queue. Pick your
          language — the snippet, the links and the sidebar all follow it.
        </p>
        <div className="btns">
          <Link className="btn pri" to={active.docHref}>
            Quickstart →
          </Link>
          {/* GitHub already sits in the nav; the second slot is better spent on
              the section a reader lands here for after the quickstart. */}
          <Link className="btn gho" to={`/${sdk}/modules`}>
            Read Modules →
          </Link>
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
