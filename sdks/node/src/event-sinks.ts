// Event sinks: job lifecycle events a worker sends out as CloudEvents. The
// configuration is one JSON document, the same across the cross-SDK contract,
// so its keys stay snake_case here. The native core parses and validates it.

/** Which events a sink receives. Each list is an allowlist; empty or absent admits all. */
export interface EventSinkFilter {
  /** Namespaces; `"default"` names the default namespace. */
  namespaces?: string[];
  /** Queue names. */
  queues?: string[];
  /** Task names. */
  tasks?: string[];
  /** Event types by short name, e.g. `"job.dead"`. */
  types?: string[];
}

/** Buffering, retries and batching, shared by every sink kind. */
export interface EventSinkDelivery {
  /** Events buffered before new ones are dropped (default 10000). */
  buffer?: number;
  /** Attempts per batch before it is dropped, first included (default 5). */
  max_attempts?: number;
  /** Events sent per request or pipeline (default 1, at most 1000). */
  max_batch?: number;
}

/** CloudEvents POSTed over HTTP. */
export interface HttpEventSink {
  kind: "http";
  /** Metrics label and log name; unique in the document. */
  name: string;
  /** Where events are POSTed. `https`, or `http` to loopback only. */
  url: string;
  /** Hosts and CIDRs the URL may resolve to. Required and non-empty. */
  allow: string[];
  /** Permit a loopback destination (and cleartext `http` to it). */
  allow_loopback?: boolean;
  /** Environment variable holding a bearer token. */
  bearer_token_env?: string;
  /** Environment variable holding the HMAC signing secret. */
  hmac_secret_env?: string;
  /** Per-request budget in ms (default 10000). */
  timeout_ms?: number;
  /** Connect budget in ms (default 5000). */
  connect_timeout_ms?: number;
  filter?: EventSinkFilter;
  /** Send job payloads. Off by default: arguments can be personal data. */
  include_payload?: boolean;
  delivery?: EventSinkDelivery;
}

/** Events appended to a Redis stream with `XADD`. */
export interface RedisStreamsEventSink {
  kind: "redis_streams";
  /** Metrics label and log name; unique in the document. */
  name: string;
  /** Environment variable holding the `redis://` URL (it carries the password). */
  url_env: string;
  /** Stream key events are appended to. */
  stream: string;
  /** Approximate cap on the stream's length (default 100000). */
  max_len?: number;
  filter?: EventSinkFilter;
  /** Send job payloads. Off by default: arguments can be personal data. */
  include_payload?: boolean;
  delivery?: EventSinkDelivery;
}

/** Kafka SASL credentials, each named by the environment variable holding it. */
export interface KafkaEventSinkSasl {
  /** `"plain"` needs `tls`: it sends the password as is. */
  mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
  /** Environment variable holding the username. */
  username_env: string;
  /** Environment variable holding the password. */
  password_env: string;
}

/** Events produced to a Kafka topic, keyed by job id. */
export interface KafkaEventSink {
  kind: "kafka";
  /** Metrics label and log name; unique in the document. */
  name: string;
  /** Bootstrap brokers, `host:port`. */
  brokers: string[];
  /** Topic records are produced to. */
  topic: string;
  /** Connect over TLS (default false). */
  tls?: boolean;
  /** PEM file of CA certificates to trust instead of the bundled web roots. Needs `tls`. */
  ca_file?: string;
  sasl?: KafkaEventSinkSasl;
  /** Budget for one batch in ms, connecting included (default 10000). */
  timeout_ms?: number;
  filter?: EventSinkFilter;
  /** Send job payloads. Off by default: arguments can be personal data. */
  include_payload?: boolean;
  delivery?: EventSinkDelivery;
}

/** Events published to a NATS subject. */
export interface NatsEventSink {
  kind: "nats";
  /** Metrics label and log name; unique in the document. */
  name: string;
  /** Environment variable holding the server URL or a comma-separated list (it can carry credentials). */
  url_env: string;
  /** Environment variable holding the contents of a `.creds` file. */
  credentials_env?: string;
  /** PEM file of CA certificates to trust instead of the bundled web roots. */
  ca_file?: string;
  /** Subject template; may name `{namespace}`, `{queue}`, `{task}` and `{type}`. */
  subject: string;
  /** `"jetstream"` waits for the stream's ack (default); `"core"` publishes and flushes. */
  mode?: "jetstream" | "core";
  /** Budget for one batch in ms, connecting included (default 10000). */
  timeout_ms?: number;
  filter?: EventSinkFilter;
  /** Send job payloads. Off by default: arguments can be personal data. */
  include_payload?: boolean;
  delivery?: EventSinkDelivery;
}

/** One event sink. */
export type EventSink = HttpEventSink | RedisStreamsEventSink | KafkaEventSink | NatsEventSink;

/** The event-sinks configuration document. */
export interface EventSinksConfig {
  /** CloudEvents `source` stamped on every event (default `"/flexiq"`). */
  source?: string;
  /** Where events go. At least one. */
  sinks: EventSink[];
}

/** One event sink's counters at one moment. */
export interface EventSinkStats {
  /** The sink's configured name. */
  name: string;
  /** The sink's `kind`, e.g. `"http"`. */
  kind: string;
  /** Events the destination accepted. */
  delivered: number;
  /** Events dropped because the sink's buffer was full. */
  droppedBufferFull: number;
  /** Events the destination refused outright. */
  droppedRejected: number;
  /** Events dropped after every delivery attempt failed. */
  droppedFailed: number;
  /** Events dropped because the worker was stopping. */
  droppedShutdown: number;
  /** Events accepted but not yet delivered or dropped (approximate). */
  queued: number;
}

/** The document as JSON text, or `undefined` when no sinks are configured. */
export function eventSinksDocument(
  config: EventSinksConfig | string | undefined,
): string | undefined {
  if (config === undefined || typeof config === "string") {
    return config;
  }
  return JSON.stringify(config);
}
