// W3C trace context carried in a job's metadata (cross-SDK contract).
//
// The carrier is the metadata JSON object itself: `traceparent` and `tracestate`
// at its top level, so any OpenTelemetry propagator reads and writes it as a
// plain text map. Every SDK merges by the same rule:
//
// - absent or empty metadata becomes an object holding just the carrier;
// - an object gains the carrier keys unless it already names `traceparent` or
//   `tracestate` — the caller's own context wins whole — and never loses or
//   overwrites a key of its own;
// - anything else (an array, a string, text that is not JSON) is left untouched.
//
// An object's own bytes are kept: the keys are spliced in after its opening
// brace instead of the document being re-serialized.

const TRACEPARENT = "traceparent";
const TRACESTATE = "tracestate";

function parseObject(metadata: string): Record<string, unknown> | undefined {
  let value: unknown;
  try {
    value = JSON.parse(metadata);
  } catch {
    // Free-form text is valid metadata; it just cannot carry a context.
    return undefined;
  }
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

/** `metadata` with `carrier`'s keys added, by the rule above. @internal */
export function mergeTraceCarrier(
  metadata: string | undefined,
  carrier: Readonly<Record<string, string>>,
): string | undefined {
  const keys = Object.keys(carrier);
  if (keys.length === 0) {
    return metadata;
  }
  if (metadata === undefined || metadata.trim() === "") {
    return JSON.stringify(carrier);
  }
  const existing = parseObject(metadata);
  if (
    existing === undefined ||
    Object.hasOwn(existing, TRACEPARENT) ||
    Object.hasOwn(existing, TRACESTATE)
  ) {
    return metadata;
  }
  const entries = keys
    .filter((key) => !Object.hasOwn(existing, key))
    .map((key) => `${JSON.stringify(key)}:${JSON.stringify(carrier[key])}`)
    .join(",");
  if (entries === "") {
    return metadata;
  }
  // A parsed object's first non-whitespace character is its `{`.
  const brace = metadata.length - metadata.trimStart().length + 1;
  const separator = Object.keys(existing).length > 0 ? "," : "";
  return `${metadata.slice(0, brace)}${entries}${separator}${metadata.slice(brace)}`;
}

/**
 * The string members of `metadata`'s object, for a propagator to read. Empty
 * for metadata that is absent or not a JSON object: such a job starts a trace
 * of its own rather than failing. @internal
 */
export function traceCarrier(metadata: string | undefined): Record<string, string> {
  const existing = metadata ? parseObject(metadata) : undefined;
  const carrier: Record<string, string> = {};
  for (const [key, value] of Object.entries(existing ?? {})) {
    if (typeof value === "string") {
      carrier[key] = value;
    }
  }
  return carrier;
}
