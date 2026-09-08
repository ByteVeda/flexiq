/*
 * The four machines in the server-door demo, drawn rather than labelled.
 *
 * Each figure is a small standalone SVG in its own 120×78 viewBox, driven only
 * by props, so the demo stays a state machine and the drawing stays a drawing.
 * Every colour is a token — `--c` is the current stage's tone, which is how one
 * figure turns red on a refusal without a second set of shapes.
 *
 * Motion lives in `demos.css` (`.sdfig-*`), not here: a class can be switched
 * off wholesale by the reduced-motion query, an inline animation cannot.
 */

/** A laptop. The screen holds the request, then the status line that came back. */
export function ClientFigure({
  sending,
  status,
  tone,
}: {
  sending: boolean;
  status?: "ok" | "bad";
  tone: string;
}) {
  return (
    <svg
      className="sdfig"
      viewBox="0 0 120 78"
      role="img"
      aria-label="A laptop composing an HTTP request"
    >
      <title>Client</title>
      {/* lid */}
      <rect
        x="17"
        y="5"
        width="86"
        height="55"
        rx="6"
        fill="var(--panel3)"
        stroke="var(--line2)"
        strokeWidth="1.5"
      />
      <rect x="22" y="10" width="76" height="45" rx="3" fill="var(--bg)" />
      {/* the request, as three lines of type */}
      <rect x="28" y="17" width="34" height="3.5" rx="1.75" fill={tone} />
      <rect x="28" y="25" width="52" height="3" rx="1.5" fill="var(--line3)" />
      <rect x="28" y="32" width="44" height="3" rx="1.5" fill="var(--line3)" />
      <rect x="28" y="39" width="30" height="3" rx="1.5" fill="var(--line3)" />
      {sending ? (
        <rect
          className="sdfig-caret"
          x="60"
          y="38"
          width="4"
          height="5"
          fill={tone}
        />
      ) : null}
      {/* the answer, once there is one */}
      {status ? (
        <rect
          x="28"
          y="46"
          width="46"
          height="4"
          rx="2"
          fill={tone}
          opacity="0.85"
        />
      ) : null}
      {/* base */}
      <path
        d="M8 62h104l5 8a3 3 0 0 1-2.6 4.4H5.6A3 3 0 0 1 3 70z"
        fill="var(--panel2)"
        stroke="var(--line2)"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
      <rect
        x="50"
        y="66"
        width="20"
        height="2.6"
        rx="1.3"
        fill="var(--line3)"
      />
    </svg>
  );
}

/** A rack: three units, one per thing the door does, plus a fan that spins
 *  while the listener is handling the call. */
export function ServerFigure({
  steps,
  failedStep,
  running,
  tone,
}: {
  steps: Set<number>;
  failedStep?: number;
  running: boolean;
  tone: string;
}) {
  return (
    <svg
      className="sdfig"
      viewBox="0 0 120 78"
      role="img"
      aria-label="A server rack of three units, one lit per completed step"
    >
      <title>flexiq-server</title>
      <rect
        x="14"
        y="4"
        width="92"
        height="70"
        rx="8"
        fill="var(--panel2)"
        stroke="var(--line2)"
        strokeWidth="1.5"
      />
      {[0, 1, 2].map((i) => {
        const n = i + 1;
        const on = steps.has(n);
        const bad = failedStep === n;
        const c = bad ? "var(--red)" : on ? tone : "var(--line3)";
        const y = 11 + i * 21;
        return (
          <g key={n}>
            <rect
              x="20"
              y={y}
              width="80"
              height="17"
              rx="4"
              fill="var(--panel3)"
              stroke={on || bad ? c : "var(--line)"}
              strokeWidth={on || bad ? 1.4 : 1}
            />
            {/* vents */}
            {[0, 1, 2, 3, 4].map((v) => (
              <rect
                key={v}
                x={26 + v * 5}
                y={y + 5}
                width="2.4"
                height="7"
                rx="1.2"
                fill="var(--line3)"
                opacity="0.8"
              />
            ))}
            {/* status LEDs */}
            <circle
              className={on || bad ? "sdfig-led on" : "sdfig-led"}
              cx="86"
              cy={y + 8.5}
              r="3"
              fill={c}
            />
            <circle cx="94" cy={y + 8.5} r="3" fill="var(--line3)" />
          </g>
        );
      })}
      {/* fan on the front panel, turning while a call is in flight */}
      <g className={running ? "sdfig-fan spin" : "sdfig-fan"}>
        <circle
          cx="107"
          cy="39"
          r="7.5"
          fill="var(--panel3)"
          stroke="var(--line2)"
        />
        <path
          d="M107 33.5v5M107 44.5v-5M101.5 39h5M112.5 39h-5"
          stroke="var(--line3)"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      </g>
    </svg>
  );
}

/** A database cylinder. Each field the row gains fills one band on its face. */
export function StoreFigure({
  filled,
  total,
  tone,
}: {
  filled: number;
  total: number;
  tone: string;
}) {
  return (
    <svg
      className="sdfig"
      viewBox="0 0 120 78"
      role="img"
      aria-label={`A database cylinder with ${filled} of ${total} row fields written`}
    >
      <title>jobs</title>
      <path
        d="M26 15v48a34 9 0 0 0 68 0V15"
        fill="var(--panel2)"
        stroke="var(--line2)"
        strokeWidth="1.5"
      />
      {/* one band per field, filling left to right as the row is built */}
      {Array.from({ length: total }).map((_, i) => {
        const on = i < filled;
        const y = 26 + i * 8;
        return (
          <rect
            // biome-ignore lint/suspicious/noArrayIndexKey: fixed band count
            key={i}
            x="34"
            y={y}
            width={on ? 52 : 22}
            height="4"
            rx="2"
            fill={on ? tone : "var(--line3)"}
            opacity={on ? 0.9 : 0.5}
          />
        );
      })}
      <ellipse
        cx="60"
        cy="15"
        rx="34"
        ry="9"
        fill="var(--panel3)"
        stroke="var(--line2)"
        strokeWidth="1.5"
      />
    </svg>
  );
}

/** A processor die with six cores — the pool, idle until there is a row. */
export function WorkerFigure({ busy, tone }: { busy: boolean; tone: string }) {
  return (
    <svg
      className="sdfig"
      viewBox="0 0 120 78"
      role="img"
      aria-label="A six-core processor, idle until a job is claimed"
    >
      <title>worker</title>
      {/* pins */}
      {[0, 1, 2, 3].map((i) => (
        <g key={i} stroke="var(--line3)" strokeWidth="2" strokeLinecap="round">
          <path d={`M${38 + i * 12} 12v-6`} />
          <path d={`M${38 + i * 12} 66v6`} />
          <path d={`M28 ${24 + i * 10}h-6`} />
          <path d={`M92 ${24 + i * 10}h6`} />
        </g>
      ))}
      <rect
        x="28"
        y="12"
        width="64"
        height="54"
        rx="7"
        fill="var(--panel2)"
        stroke="var(--line2)"
        strokeWidth="1.5"
      />
      <rect
        x="36"
        y="20"
        width="48"
        height="38"
        rx="4"
        fill="var(--panel3)"
        stroke="var(--line)"
      />
      {Array.from({ length: 6 }).map((_, i) => (
        <rect
          // biome-ignore lint/suspicious/noArrayIndexKey: fixed core grid
          key={i}
          className={busy ? "sdfig-core busy" : "sdfig-core"}
          style={{ "--k": i % 3 } as React.CSSProperties}
          x={41 + (i % 3) * 14}
          y={26 + Math.floor(i / 3) * 18}
          width="10"
          height="12"
          rx="2"
          fill={busy ? tone : "var(--line3)"}
        />
      ))}
    </svg>
  );
}
