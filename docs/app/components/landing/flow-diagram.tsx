import type { ReactNode } from "react";

/**
 * The `.delay()`-to-result flow, drawn in the same grammar as the server-door
 * demo further down the page: a box per participant, each with a figure and the
 * two or three lines that say what it actually does, joined by named wires.
 *
 * Deliberately not interactive. The demo below it is the thing you scrub — a
 * second playhead on the same screen competes with it, and this diagram is the
 * one-glance answer a reader wants before they decide to scrub anything. The
 * only motion is the sparks on the wires, which is decorative and switched off
 * with the rest under `prefers-reduced-motion`.
 */

/** One mono line in a box's body. With `k`, it renders as a key/value pair —
 *  the shape a stored row reads in, and how the demo's `jobs` box shows one. */
export interface FlowLine {
  k?: string;
  v: string;
  /** Render the value as code rather than prose. */
  code?: boolean;
}

export interface FlowStage {
  /** Kicker above the title. */
  label: string;
  title: string;
  /** Mono sub-label: what it is built on, or how many of it there are. */
  sub: string;
  figure: ReactNode;
  lines: FlowLine[];
  /** Label on the wire leaving this stage; the last stage has none. */
  wire?: string;
  /** The two boxes that are FlexiQ's own, rather than yours or your store's. */
  accent?: boolean;
}

export function FlowDiagram({
  stages,
  returnLabel,
}: {
  stages: FlowStage[];
  /** The rail under the row: the result's way back to where it started. */
  returnLabel: string;
}) {
  return (
    <div className="hiw">
      <div className="hiw-row">
        {stages.map((stage, index) => (
          <Stage key={stage.label} stage={stage} index={index} />
        ))}
      </div>
      <div className="hiw-return">
        <span className="hiw-spark hiw-back" />
        <span className="hiw-returnlbl">{returnLabel}</span>
      </div>
    </div>
  );
}

/** One box plus the wire that leaves it — the last stage has no wire, so the
 *  row ends on a box rather than an arrow pointing at nothing. */
function Stage({ stage, index }: { stage: FlowStage; index: number }) {
  return (
    <>
      <div className={`hiw-box${stage.accent ? " accent" : ""}`}>
        <div className="hiw-hd">
          <span className="hiw-kicker">{stage.label}</span>
          <b>{stage.title}</b>
          <span className="hiw-sub">{stage.sub}</span>
        </div>
        <div className="hiw-fig">{stage.figure}</div>
        <dl className="hiw-lines">
          {stage.lines.map((line) => (
            <div className="hiw-line" key={line.v}>
              {line.k ? <dt>{line.k}</dt> : null}
              <dd className={line.code ? "hiw-mono" : ""}>{line.v}</dd>
            </div>
          ))}
        </dl>
      </div>
      {stage.wire ? (
        <div
          className="hiw-wire"
          style={{ "--wd": `${index * 0.5}s` } as React.CSSProperties}
        >
          <span className="hiw-wirelbl">{stage.wire}</span>
          <span className="hiw-spark" />
        </div>
      ) : null}
    </>
  );
}

/* The four figures. Each is a 120×56 line drawing in `currentColor`, so a box
   tints its own by setting a colour — no per-figure palette to keep in step. */

/** Your code: an editor window with a caret on the call. */
export function CodeFigure() {
  return (
    <svg className="hiwfig" viewBox="0 0 120 56" role="presentation">
      <rect
        x="14"
        y="6"
        width="92"
        height="44"
        rx="6"
        fill="var(--panel2)"
        stroke="currentColor"
        strokeWidth="1.4"
        opacity="0.9"
      />
      <path
        d="M14 17h92"
        stroke="currentColor"
        strokeWidth="1.2"
        opacity="0.5"
      />
      <circle cx="22" cy="11.5" r="1.8" fill="currentColor" opacity="0.55" />
      <circle cx="29" cy="11.5" r="1.8" fill="currentColor" opacity="0.55" />
      <g fill="currentColor">
        <rect x="22" y="25" width="34" height="3.4" rx="1.7" opacity="0.85" />
        <rect x="60" y="25" width="20" height="3.4" rx="1.7" opacity="0.4" />
        <rect x="22" y="33" width="24" height="3.4" rx="1.7" opacity="0.4" />
        <rect x="22" y="41" width="46" height="3.4" rx="1.7" opacity="0.85" />
        <rect className="hiwfig-caret" x="71" y="40.4" width="2" height="5" />
      </g>
    </svg>
  );
}

/** The store: a cylinder with the row's fields banded across it. */
export function StoreFigure() {
  return (
    <svg className="hiwfig" viewBox="0 0 120 56" role="presentation">
      <path
        d="M38 12v32a22 6 0 0 0 44 0V12"
        fill="var(--panel2)"
        stroke="currentColor"
        strokeWidth="1.4"
      />
      <g fill="currentColor">
        <rect x="46" y="20" width="28" height="3.2" rx="1.6" opacity="0.85" />
        <rect x="46" y="27" width="28" height="3.2" rx="1.6" opacity="0.85" />
        <rect x="46" y="34" width="18" height="3.2" rx="1.6" opacity="0.45" />
      </g>
      <ellipse
        cx="60"
        cy="12"
        rx="22"
        ry="6"
        fill="var(--panel3)"
        stroke="currentColor"
        strokeWidth="1.4"
      />
    </svg>
  );
}

/** The scheduler: a dial, and the batch it is about to hand out. */
export function SchedulerFigure() {
  return (
    <svg className="hiwfig" viewBox="0 0 120 56" role="presentation">
      <circle
        cx="42"
        cy="28"
        r="17"
        fill="var(--panel2)"
        stroke="currentColor"
        strokeWidth="1.4"
      />
      <path
        d="M42 18v10l7 4"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        fill="none"
      />
      {/* the claimed batch leaving, one bar per job */}
      <g fill="currentColor">
        {[0, 1, 2].map((i) => (
          <rect
            key={i}
            className="hiwfig-batch"
            x={68}
            y={16 + i * 9}
            width="30"
            height="5"
            rx="2.5"
            style={{ "--k": i } as React.CSSProperties}
          />
        ))}
      </g>
    </svg>
  );
}

/** The pool: a die with six cores, the ones with work lit. */
export function PoolFigure() {
  return (
    <svg className="hiwfig" viewBox="0 0 120 56" role="presentation">
      <rect
        x="36"
        y="8"
        width="48"
        height="40"
        rx="7"
        fill="var(--panel2)"
        stroke="currentColor"
        strokeWidth="1.4"
      />
      {/* pins */}
      <g stroke="currentColor" strokeWidth="1.4" opacity="0.45">
        {[44, 54, 64, 74].map((x) => (
          <path key={`t${x}`} d={`M${x} 8V3`} />
        ))}
        {[44, 54, 64, 74].map((x) => (
          <path key={`b${x}`} d={`M${x} 48v5`} />
        ))}
      </g>
      <g fill="currentColor">
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <rect
            key={i}
            className="hiwfig-core"
            x={44 + (i % 3) * 13}
            y={18 + Math.floor(i / 3) * 13}
            width="9"
            height="9"
            rx="2.5"
            style={{ "--k": i } as React.CSSProperties}
          />
        ))}
      </g>
    </svg>
  );
}
