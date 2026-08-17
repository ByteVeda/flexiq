package org.byteveda.flexiq.proxies;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import org.byteveda.flexiq.errors.ProxyException;
import org.jspecify.annotations.Nullable;

/**
 * A unit-of-work wrapper over {@link Proxies} adding identity dedup and a
 * cleanup lifecycle. Within one session:
 *
 * <ul>
 *   <li>{@link #deconstruct(Object)} memoizes by (instance identity, purpose) —
 *       deconstructing the same object again returns the same {@link ProxyRef}
 *       without calling the handler twice. The first call's TTL wins per
 *       (instance, purpose); use a new session to refresh expiry.
 *   <li>{@link #reconstruct(ProxyRef)} memoizes by the ref's signature — every
 *       ref to the same underlying resource resolves to the same instance, and
 *       the handler reconstructs it once. Signature, expiry, and purpose are
 *       re-verified on every call, memo hit or not.
 *   <li>{@link #close()} runs {@link ProxyHandler#cleanup} once per unique
 *       reconstructed instance, in reverse reconstruction order (LIFO).
 * </ul>
 *
 * <p>Not thread-safe — confine a session to the thread that created it. A
 * session models one producer batch or one task invocation.
 */
public final class ProxySession implements AutoCloseable {
    private static final Logger LOG = System.getLogger(ProxySession.class.getName());

    private final Proxies proxies;
    private final IdentityHashMap<Object, Map<String, ProxyRef>> deconstructed = new IdentityHashMap<>();
    private final Map<String, Object> reconstructed = new HashMap<>();
    private final Deque<Runnable> cleanups = new ArrayDeque<>();
    private boolean closed;

    ProxySession(Proxies proxies) {
        this.proxies = proxies;
    }

    /** Deconstruct {@code value} (no expiry or purpose), deduped by instance identity. */
    public ProxyRef deconstruct(Object value) {
        return deconstruct(value, null, null);
    }

    /** Deconstruct {@code value} with a TTL, deduped by instance identity. */
    public ProxyRef deconstruct(Object value, @Nullable Duration ttl) {
        return deconstruct(value, ttl, null);
    }

    /**
     * Deconstruct {@code value} bound to {@code ttl}/{@code purpose} (both
     * nullable), deduped by (instance identity, purpose).
     */
    public ProxyRef deconstruct(Object value, @Nullable Duration ttl, @Nullable String purpose) {
        ensureOpen();
        if (value == null) {
            throw new ProxyException("cannot deconstruct null");
        }
        Map<String, ProxyRef> byPurpose = deconstructed.computeIfAbsent(value, key -> new HashMap<>());
        ProxyRef cached = byPurpose.get(purpose);
        if (cached != null) {
            return cached;
        }
        ProxyRef ref = proxies.deconstruct(value, ttl, purpose);
        byPurpose.put(purpose, ref);
        return ref;
    }

    /** Verify and reconstruct {@code ref}, deduped by its signature. */
    public Object reconstruct(ProxyRef ref) {
        return reconstruct(ref, null);
    }

    /**
     * Verify (always — including memo hits, so a ref that expires mid-session
     * stops resolving) and reconstruct {@code ref}, deduped by its signature.
     */
    public Object reconstruct(ProxyRef ref, @Nullable String expectedPurpose) {
        ensureOpen();
        ProxyHandler<Object> handler = proxies.handlerFor(ref.handler());
        proxies.verify(ref, expectedPurpose);
        String signature = ref.signature();
        if (reconstructed.containsKey(signature)) {
            return reconstructed.get(signature);
        }
        Object value = handler.reconstruct(ref.reference());
        reconstructed.put(signature, value);
        cleanups.push(() -> handler.cleanup(value));
        return value;
    }

    /** {@link #reconstruct(ProxyRef)} cast to the caller's type. */
    @SuppressWarnings("unchecked")
    public <T> T resolve(ProxyRef ref) {
        return (T) reconstruct(ref);
    }

    /** {@link #reconstruct(ProxyRef, String)} cast to the caller's type. */
    @SuppressWarnings("unchecked")
    public <T> T resolve(ProxyRef ref, String expectedPurpose) {
        return (T) reconstruct(ref, expectedPurpose);
    }

    /**
     * Run {@link ProxyHandler#cleanup} for every reconstructed instance in
     * reverse order (LIFO), once each. A cleanup failure is logged and never
     * skips the rest; a JVM-level {@link Error} is rethrown only after the
     * remaining cleanups have drained. Idempotent.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Error fatal = null;
        while (!cleanups.isEmpty()) {
            Runnable cleanup = cleanups.pop();
            try {
                cleanup.run();
            } catch (RuntimeException e) {
                // Cleanup must never fail the rest of the teardown — record and continue.
                LOG.log(Level.WARNING, "proxy cleanup failed", e);
            } catch (Error e) {
                // Drain the remaining cleanups first, then let the error propagate —
                // aborting here would abandon them for good (closed is already set).
                if (fatal == null) {
                    fatal = e;
                }
                LOG.log(Level.WARNING, "proxy cleanup threw an error", e);
            }
        }
        deconstructed.clear();
        reconstructed.clear();
        if (fatal != null) {
            throw fatal;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new ProxyException("proxy session is closed");
        }
    }
}
