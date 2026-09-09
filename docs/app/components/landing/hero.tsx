import { useState } from "react";
import { Link } from "react-router";
import { RawHtml } from "@/components/ui";
import { useSdk } from "@/hooks";
import { isSdk, type Tier, tierProfile } from "@/lib";
import {
  highlightJava,
  highlightPython,
  highlightShell,
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
  sh: highlightShell,
};

export function Hero() {
  const { sdk, setSdk } = useSdk();
  // A tab is a *tier*, and only three of the four are languages. Picking a
  // language sets the global SDK, so the docs links and the sidebar follow it.
  // The server tier cannot go there: the store's value is also the
  // `<html data-sdk>` one, where CSS uses it to pick which `<SdkOnly>` variant
  // to show, and a value that is no language matches none of them. So it is
  // pinned to this component instead, and the SDK the rest of the site reads
  // stays whatever it was.
  const [pinnedTier, setPinnedTier] = useState<Tier | null>(null);
  const tier = pinnedTier ?? sdk;
  const active = HERO_PANES.find((p) => p.tier === tier) ?? HERO_PANES[0];
  const codeHtml = HIGHLIGHT[active.lang](active.code);

  // Routed on `isSdk` rather than on the tier's name, so a second non-language
  // tier needs no second branch here.
  function select(next: Tier) {
    if (isSdk(next)) {
      setSdk(next);
      setPinnedTier(null);
      return;
    }
    setPinnedTier(next);
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
          language and the snippet, the links and the sidebar all follow it — or
          pick the server, if the process enqueuing has no binding at all.
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
              {active.runtag}
            </div>
          </div>
          <div className="langtabs">
            {HERO_PANES.map((p) => (
              <button
                key={p.tier}
                type="button"
                aria-pressed={p.tier === tier}
                className={`langtab ${p.tier === tier ? "active" : ""}`.trim()}
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
