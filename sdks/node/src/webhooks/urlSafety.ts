import { lookup } from "node:dns/promises";
import { isIP, type LookupFunction } from "node:net";
import { UnsafeWebhookUrlError } from "./errors";

/**
 * Outbound URL safety checks for dashboard-configured webhooks (SSRF guard).
 *
 * Anyone who can write a webhook subscription could otherwise point it at
 * `http://169.254.169.254/...` and turn the worker into an SSRF proxy for cloud
 * metadata, loopback services, or the RFC1918 intranet. Delivery to loopback,
 * link-local, RFC1918, CGNAT, and ULA destinations is refused by default; set
 * `FLEXIQ_WEBHOOKS_ALLOW_PRIVATE` (truthy) to disable the guard when developing
 * against a local endpoint.
 *
 * Two layers: {@link assertSafeWebhookUrl} rules on the URL itself (scheme,
 * host name, literal address) without touching DNS, and {@link createSafeLookup}
 * rules on the resolved address at the moment the socket dials it.
 *
 * Mirrors the cross-SDK `validate_webhook_url` contract.
 */

const ALLOW_ENV_VAR = "FLEXIQ_WEBHOOKS_ALLOW_PRIVATE";

// Names that never leave this host regardless of DNS, which an address check
// (run only after resolution) would miss for names that fail to resolve.
const BLOCKED_HOSTNAMES = new Set([
  "localhost",
  "localhost.localdomain",
  "ip6-localhost",
  "ip6-loopback",
]);
const BLOCKED_SUFFIXES = [".localhost", ".local", ".internal", ".intranet", ".lan", ".private"];

const FALSY_ENV_VALUES = new Set(["", "0", "false", "no", "off"]);

/** Destinations that never belong to a third-party HTTP endpoint. */
const BLOCKED_V4_CIDRS: ReadonlyArray<readonly [string, number]> = [
  ["0.0.0.0", 8], // this-network / unspecified
  ["10.0.0.0", 8],
  ["100.64.0.0", 10], // CGNAT
  ["127.0.0.0", 8], // loopback
  ["169.254.0.0", 16], // link-local, incl. cloud metadata
  ["172.16.0.0", 12],
  ["192.0.0.0", 24], // IETF protocol assignments
  ["192.0.2.0", 24], // documentation
  ["192.168.0.0", 16],
  ["198.18.0.0", 15], // benchmarking
  ["198.51.100.0", 24], // documentation
  ["203.0.113.0", 24], // documentation
  ["224.0.0.0", 4], // multicast
  ["240.0.0.0", 4], // reserved + broadcast
];

const BLOCKED_V6_CIDRS: ReadonlyArray<readonly [string, number]> = [
  ["::", 128], // unspecified
  ["::1", 128], // loopback
  ["fc00::", 7], // unique-local
  ["fe80::", 10], // link-local
  ["ff00::", 8], // multicast
  ["2001:db8::", 32], // documentation
];

export interface UrlSafetyOptions {
  /** Bypass the private-address checks. Defaults to the environment variable. */
  allowPrivate?: boolean;
}

export interface SafeLookupOptions extends UrlSafetyOptions {
  /**
   * Bound on the resolution itself. A request timeout only starts once a socket
   * is assigned, so without this an unresponsive resolver stalls the delivery
   * indefinitely. Unbounded when unset.
   */
  timeoutMs?: number;
}

/**
 * Validate `raw` on its face — bad scheme, missing host, known-local name, or a
 * literal private address — without resolving DNS.
 *
 * Deliberately synchronous so registration and every delivery attempt can run
 * it. Whether a *named* host points somewhere private is decided by
 * {@link createSafeLookup} when the socket resolves it, which is both the
 * authoritative answer and the one that cannot be raced.
 */
export function assertSafeWebhookUrl(raw: string, options?: UrlSafetyOptions): void {
  const hostname = parseHostname(raw);
  if (allowPrivate(options)) {
    return;
  }
  assertHostnameAllowed(hostname);
  if (isIP(hostname) !== 0) {
    assertAddressAllowed(hostname, hostname);
  }
}

/**
 * A `net.LookupFunction` that fails the connection when the host resolves to a
 * blocked address.
 *
 * Checking a name up front cannot bind the transport, which resolves it again
 * when it dials — the rebinding window is between the two. Handing this lookup
 * to the request closes it: the address the socket connects to *is* the address
 * that was checked, and the original hostname still drives the Host header and
 * TLS SNI.
 *
 * Only a blocked address is an {@link UnsafeWebhookUrlError}; a resolver
 * failure stays an ordinary error so callers can retry it.
 */
export function createSafeLookup(options?: SafeLookupOptions): LookupFunction {
  return (hostname, lookupOptions, callback) => {
    withTimeout(
      lookup(hostname, { ...lookupOptions, all: true, verbatim: true }),
      options?.timeoutMs,
      `resolving ${hostname}`,
    )
      .then((addresses) => {
        if (!allowPrivate(options)) {
          for (const { address } of addresses) {
            assertAddressAllowed(address, hostname);
          }
        }
        if (lookupOptions.all) {
          callback(null, addresses as never);
          return;
        }
        const first = addresses[0];
        if (!first) {
          throw new UnsafeWebhookUrlError(`could not resolve webhook host ${hostname}`);
        }
        callback(null, first.address as never, first.family);
      })
      .catch((cause: unknown) => {
        callback(cause instanceof Error ? cause : new Error(String(cause)), "", 0);
      });
  };
}

/** Reject `promise` once `timeoutMs` elapses; pass it through when unset. */
function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number | undefined,
  what: string,
): Promise<T> {
  if (timeoutMs === undefined) {
    return promise;
  }
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`timed out after ${timeoutMs}ms ${what}`)),
      timeoutMs,
    );
    promise.then(resolve, reject).finally(() => clearTimeout(timer));
  });
}

/** Whether the guard is disabled, from the option or the environment. */
function allowPrivate(options?: UrlSafetyOptions): boolean {
  if (options?.allowPrivate !== undefined) {
    return options.allowPrivate;
  }
  const value = process.env[ALLOW_ENV_VAR];
  return value !== undefined && !FALSY_ENV_VALUES.has(value.trim().toLowerCase());
}

/**
 * Parse `raw` and return its hostname (IPv6 brackets stripped).
 *
 * Scheme and host are validated even when the guard is bypassed, so a
 * misconfigured URL gets the same feedback either way.
 */
function parseHostname(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new UnsafeWebhookUrlError("webhook url is not a valid URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new UnsafeWebhookUrlError("webhook url scheme must be http or https");
  }
  if (!url.hostname) {
    throw new UnsafeWebhookUrlError("webhook url must include a host");
  }
  return stripBrackets(url.hostname);
}

/** `URL.hostname` keeps the brackets around an IPv6 literal; drop them. */
function stripBrackets(hostname: string): string {
  return hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
}

function assertHostnameAllowed(hostname: string): void {
  const lowered = hostname.toLowerCase();
  const blocked =
    BLOCKED_HOSTNAMES.has(lowered) || BLOCKED_SUFFIXES.some((suffix) => lowered.endsWith(suffix));
  if (blocked) {
    throw new UnsafeWebhookUrlError(`webhook url host ${hostname} is a local-only name`);
  }
}

function assertAddressAllowed(address: string, hostname: string): void {
  if (isBlockedAddress(address)) {
    throw new UnsafeWebhookUrlError(
      `webhook url host ${hostname} resolves to the private or reserved address ${address}`,
    );
  }
}

function isBlockedAddress(address: string): boolean {
  const bytes = ipBytes(address);
  if (!bytes) {
    // An address we cannot parse is not an address we are willing to POST to.
    return true;
  }
  if (bytes.length === 4) {
    return matchesAny(bytes, BLOCKED_V4_CIDRS);
  }
  // IPv4-mapped IPv6 (::ffff:a.b.c.d) must be judged as the v4 address it carries.
  const mapped = mappedV4(bytes);
  return mapped ? matchesAny(mapped, BLOCKED_V4_CIDRS) : matchesAny(bytes, BLOCKED_V6_CIDRS);
}

function matchesAny(bytes: Uint8Array, cidrs: ReadonlyArray<readonly [string, number]>): boolean {
  return cidrs.some(([network, prefixLength]) => {
    const networkBytes = ipBytes(network);
    return networkBytes ? inNetwork(bytes, networkBytes, prefixLength) : false;
  });
}

/** Whether `bytes` shares its first `prefixLength` bits with `network`. */
function inNetwork(bytes: Uint8Array, network: Uint8Array, prefixLength: number): boolean {
  if (bytes.length !== network.length) {
    return false;
  }
  const wholeBytes = prefixLength >> 3;
  for (let index = 0; index < wholeBytes; index += 1) {
    if (bytes[index] !== network[index]) {
      return false;
    }
  }
  const remainingBits = prefixLength & 7;
  if (remainingBits === 0) {
    return true;
  }
  const mask = 0xff << (8 - remainingBits);
  return ((bytes[wholeBytes] ?? 0) & mask) === ((network[wholeBytes] ?? 0) & mask);
}

/** The embedded IPv4 address of an `::ffff:a.b.c.d` mapping, if this is one. */
function mappedV4(bytes: Uint8Array): Uint8Array | undefined {
  const prefix = Uint8Array.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff]);
  for (let index = 0; index < prefix.length; index += 1) {
    if (bytes[index] !== prefix[index]) {
      return undefined;
    }
  }
  return bytes.slice(12);
}

/** Raw bytes of an IPv4/IPv6 literal; `undefined` when `ip` is not an address. */
function ipBytes(ip: string): Uint8Array | undefined {
  const family = isIP(ip);
  if (family === 4) {
    return Uint8Array.from(ip.split(".").map(Number));
  }
  return family === 6 ? v6Bytes(ip) : undefined;
}

/** Expand a (possibly `::`-compressed) IPv6 literal to its 16 bytes. */
function v6Bytes(ip: string): Uint8Array | undefined {
  const halves = ip.split("::");
  if (halves.length > 2) {
    return undefined;
  }
  const head = v6Groups(halves[0] ?? "");
  const tail = halves.length === 2 ? v6Groups(halves[1] ?? "") : [];
  if (!head || !tail) {
    return undefined;
  }
  const gap = 16 - head.length - tail.length;
  if (halves.length === 1 ? gap !== 0 : gap < 0) {
    return undefined;
  }
  return Uint8Array.from([...head, ...new Array<number>(gap).fill(0), ...tail]);
}

function v6Groups(part: string): number[] | undefined {
  if (!part) {
    return [];
  }
  const bytes: number[] = [];
  for (const group of part.split(":")) {
    // A trailing dotted quad (::ffff:1.2.3.4) contributes four bytes.
    if (group.includes(".")) {
      if (isIP(group) !== 4) {
        return undefined;
      }
      bytes.push(...group.split(".").map(Number));
      continue;
    }
    const value = Number.parseInt(group, 16);
    if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
      return undefined;
    }
    bytes.push(value >> 8, value & 0xff);
  }
  return bytes;
}
