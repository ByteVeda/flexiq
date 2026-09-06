export function SectionHead({
  kicker,
  title,
  lead,
}: {
  kicker: string;
  title: React.ReactNode;
  lead?: string;
}) {
  return (
    <div className="section-head reveal">
      <div className="kicker">{kicker}</div>
      <h2>{title}</h2>
      {lead ? <p>{lead}</p> : null}
    </div>
  );
}

/** Inner SVG paths for each station's icon (matches the prototype's flow diagram). */
function DiagramIcon({ children }: { children: React.ReactNode }) {
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
      {children}
    </svg>
  );
}

type DiagramStation = {
  label: string;
  title: string;
  hint: string;
  accent?: boolean;
  pool?: boolean;
  icon?: React.ReactNode;
};

const STATIONS: DiagramStation[] = [
  {
    label: "YOUR CODE",
    title: "enqueue",
    hint: ".delay()",
    icon: (
      <>
        <polyline points="16 18 22 12 16 6" />
        <polyline points="8 6 2 12 8 18" />
      </>
    ),
  },
  {
    label: "QUEUE",
    title: "store",
    hint: "SQLite · PG",
    icon: (
      <>
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M3 5v14a9 3 0 0 0 18 0V5" />
        <path d="M3 12a9 3 0 0 0 18 0" />
      </>
    ),
  },
  {
    label: "SCHEDULER",
    title: "dispatch",
    hint: "Rust · Tokio",
    accent: true,
    icon: (
      <>
        <path d="M12 6v6l4 2" />
        <circle cx="12" cy="12" r="9" />
      </>
    ),
  },
  {
    label: "WORKERS",
    title: "execute",
    hint: "6 · pool",
    accent: true,
    pool: true,
  },
];

export function HowItWorks() {
  return (
    <section className="section how">
      <div className="wrap">
        <SectionHead
          kicker="How it works"
          title={
            <>
              From{" "}
              <span
                style={{
                  color: "var(--indigo-br)",
                  fontFamily: "var(--mono)",
                  fontSize: ".84em",
                }}
              >
                .delay()
              </span>{" "}
              to result
            </>
          }
          lead="Your application code enqueues a job. The Rust scheduler hands it to a worker. The result lands back in the shared store — same core, same queue, no broker in the middle, whichever SDK you called it from."
        />
        <div className="diagram reveal">
          <div className="flowdiag">
            {STATIONS.map((s, i) => (
              <Station
                key={s.label}
                station={s}
                last={i === STATIONS.length - 1}
                index={i}
              />
            ))}
          </div>
          <div className="returnlane">
            <span className="rlabel">result written back to the store</span>
          </div>
        </div>
      </div>
    </section>
  );
}

function Station({
  station,
  last,
  index,
}: {
  station: (typeof STATIONS)[number];
  last: boolean;
  index: number;
}) {
  return (
    <>
      <div className={`station ${station.accent ? "accent" : ""}`.trim()}>
        <div className="srow">
          {station.pool ? (
            <div className="dpool">
              {Array.from({ length: 6 }).map((_, k) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: fixed decorative dot row
                <span key={k} style={{ "--k": k } as React.CSSProperties} />
              ))}
            </div>
          ) : (
            <div className="dicon">
              <DiagramIcon>{station.icon}</DiagramIcon>
            </div>
          )}
          <div className="smeta">
            <span className="slabel">{station.label}</span>
            <span className="stitle">{station.title}</span>
            <span className="shint">{station.hint}</span>
          </div>
        </div>
      </div>
      {last ? null : (
        <div
          className="wire"
          style={{ "--wd": `${index * 0.5}s` } as React.CSSProperties}
        >
          <span className="spark" />
        </div>
      )}
    </>
  );
}
