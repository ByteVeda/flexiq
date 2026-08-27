package org.byteveda.flexiq.proxies;

import java.util.Map;

/**
 * Deconstructs a non-serializable resource of type {@code T} into a serializable
 * reference, and reconstructs it on the worker. Register handlers with a
 * {@link Proxies} registry.
 *
 * @param <T> the resource type this handler proxies
 */
public interface ProxyHandler<T> {
    /**
     * Stable id stored in the {@link ProxyRef} and used to find this handler on the worker.
     *
     * @return the id, which producer and worker must agree on for a ref to resolve
     */
    String id();

    /**
     * Whether this handler can proxy {@code value}.
     *
     * @param value a candidate offered by {@link Proxies#deconstruct(Object)}
     * @return {@code true} to claim it; the first handler that claims it wins
     */
    boolean handles(Object value);

    /**
     * Reduce {@code value} to a serializable reference (e.g. a file path, a config map).
     *
     * @param value the resource, already accepted by {@link #handles}
     * @return reference data that survives the wire and is enough to rebuild from
     */
    Map<String, Object> deconstruct(T value);

    /**
     * Rebuild the resource from a reference produced by {@link #deconstruct}.
     *
     * @param reference the data {@link #deconstruct} produced, its signature already verified
     * @return the resource, live on this side
     */
    T reconstruct(Map<String, Object> reference);

    /**
     * Release a resource this handler reconstructed, invoked (LIFO, once per
     * unique instance) when a {@link ProxySession} that produced it closes.
     * Direct {@link Proxies#reconstruct(ProxyRef)} calls have no lifecycle and
     * never trigger this. Default: no-op.
     *
     * @param value an instance this handler reconstructed during the session
     */
    default void cleanup(T value) {}
}
