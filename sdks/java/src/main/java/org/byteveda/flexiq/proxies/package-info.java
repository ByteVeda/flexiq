/**
 * Pass non-serializable resources through a job payload by reference.
 *
 * <p>Java's typed, JSON-serialized payloads don't lend themselves to Python's
 * implicit argument proxying, so this is explicit: deconstruct a value into a
 * signed, serializable {@link org.byteveda.flexiq.proxies.ProxyRef} on the
 * producer ({@link org.byteveda.flexiq.proxies.Proxies#deconstruct}), carry it
 * in the payload, and reconstruct it in the handler
 * ({@link org.byteveda.flexiq.proxies.Proxies#reconstruct}). Refs are
 * HMAC-signed; a {@link org.byteveda.flexiq.proxies.ProxyHandler} per resource
 * type does the (de)construction, with an allowlist where it matters
 * (e.g. {@link org.byteveda.flexiq.proxies.FileProxyHandler}).
 */
@NullMarked
package org.byteveda.flexiq.proxies;

import org.jspecify.annotations.NullMarked;
