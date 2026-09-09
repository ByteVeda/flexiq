import { Link } from "react-router";
import { RawHtml } from "@/components/ui";
import { useSdk } from "@/hooks";
import { isSdk, type Tier, tierProfile } from "@/lib";
import {
  highlightJava,
  highlightPython,
  highlightTs,
} from "@/lib/highlight-lite";
import {
  HERO_COMING_SOON,
  HERO_PANES,
  type HeroPane,
} from "@/lib/landing-content";
import { CopyButton } from "./copy-button";

/** A lookup rather than a ternary chain: the dialect is pane data, so adding one
 *  should be a row here and not another nested branch. */
const HIGHLIGHT: Record<HeroPane["lang"], (code: string) => string> = {
  py: highlightPython,
  ts: highlightTs,
  java: highlightJava,
};

export function Hero() {
  const { sdk, setSdk } = useSdk();
  // A tab is a *tier*, not a language: today every one of them happens to be an
  // SDK, and clicking one sets the global SDK so the hero copy, the docs links
  // and the sidebar all follow.
  const active = HERO_PANES.find((p) => p.tier === sdk) ?? HERO_PANES[0];
  const codeHtml = HIGHLIGHT[active.lang](active.code);

  // Routed on `isSdk` rather than on the tier's name: only a language belongs in
  // the SDK store, which is also the `<html data-sdk>` value.
  function select(tier: Tier) {
    if (isSdk(tier)) {
      setSdk(tier);
    }
  }

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
          <Link className="btn pri" to={active.primary.href}>
            {active.primary.label} →
          </Link>
          {/* GitHub already sits in the nav; the second slot is better spent on
              the section a reader lands here for after the quickstart. Both
              hrefs come off the pane — a tier that is not a language has no
              `/<sdk>/…` page to compose one from. */}
          <Link className="btn gho" to={active.secondary.href}>
            {active.secondary.label} →
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
                key={p.tier}
                type="button"
                aria-pressed={p.tier === sdk}
                className={`langtab ${p.tier === sdk ? "active" : ""}`.trim()}
                onClick={() => select(p.tier)}
              >
                {tierProfile(p.tier).label}
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
