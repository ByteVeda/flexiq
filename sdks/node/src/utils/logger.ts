// Tiny zero-dependency leveled logger. Writes to stderr by default so it never
// pollutes stdout (the CLI's `--json` output and piped data stay clean). Level
// is read once from `FLEXIQ_LOG_LEVEL` and is overridable at runtime; the sink
// is pluggable for tests or custom transports.

/** Severity, from most to least verbose. `silent` disables all output. */
export type LogLevel = "debug" | "info" | "warn" | "error" | "silent";

/** A message, or a thunk evaluated only when the level passes the threshold. */
export type LogMessage = string | (() => string);

/** Receives every formatted line that clears the level threshold. */
export type LogSink = (level: Exclude<LogLevel, "silent">, line: string) => void;

export interface Logger {
  debug(message: LogMessage, ...meta: unknown[]): void;
  info(message: LogMessage, ...meta: unknown[]): void;
  warn(message: LogMessage, ...meta: unknown[]): void;
  error(message: LogMessage, ...meta: unknown[]): void;
  /** Derive a namespaced child logger (`[taskito:webhooks]`). */
  child(namespace: string): Logger;
}

const SEVERITY: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  silent: Number.POSITIVE_INFINITY,
};

const defaultSink: LogSink = (_level, line) => {
  process.stderr.write(`${line}\n`);
};

function isLogLevel(value: unknown): value is LogLevel {
  // `Object.hasOwn` avoids matching inherited props (e.g. `FLEXIQ_LOG_LEVEL=toString`).
  return typeof value === "string" && Object.hasOwn(SEVERITY, value);
}

function levelFromEnv(): LogLevel {
  const raw = process.env.FLEXIQ_LOG_LEVEL?.toLowerCase();
  return isLogLevel(raw) ? raw : "warn";
}

// Shared mutable config: every logger (root + children) reads through it, so
// setLogLevel / setLogSink take effect globally and immediately.
const config = { level: levelFromEnv(), sink: defaultSink };

/** Set the global threshold; messages below it are dropped (and never built). */
export function setLogLevel(level: LogLevel): void {
  if (!isLogLevel(level)) {
    throw new TypeError(`invalid log level: ${String(level)}`);
  }
  config.level = level;
}

/** Replace the output sink (default: stderr). Useful for capture in tests. */
export function setLogSink(sink: LogSink): void {
  if (typeof sink !== "function") {
    throw new TypeError("log sink must be a function");
  }
  config.sink = sink;
}

function formatMeta(value: unknown): string {
  if (value instanceof Error) {
    return value.stack ?? `${value.name}: ${value.message}`;
  }
  if (typeof value === "object" && value !== null) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function emit(
  namespace: string | undefined,
  level: Exclude<LogLevel, "silent">,
  message: LogMessage,
  meta: unknown[],
): void {
  if (SEVERITY[level] < SEVERITY[config.level]) {
    return;
  }
  const tag = namespace ? `taskito:${namespace}` : "taskito";
  const text = typeof message === "function" ? message() : message;
  const parts = [new Date().toISOString(), `[${tag}]`, level.toUpperCase(), text];
  for (const item of meta) {
    parts.push(formatMeta(item));
  }
  config.sink(level, parts.join(" "));
}

function make(namespace?: string): Logger {
  return {
    debug: (message, ...meta) => emit(namespace, "debug", message, meta),
    info: (message, ...meta) => emit(namespace, "info", message, meta),
    warn: (message, ...meta) => emit(namespace, "warn", message, meta),
    error: (message, ...meta) => emit(namespace, "error", message, meta),
    child: (child) => make(namespace ? `${namespace}:${child}` : child),
  };
}

/** The root logger. Prefer {@link createLogger} for a namespaced one. */
export const logger: Logger = make();

/** Create a namespaced logger (`createLogger("worker")` → `[taskito:worker]`). */
export function createLogger(namespace: string): Logger {
  return make(namespace);
}
