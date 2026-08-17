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
    /** Stable id stored in the {@link ProxyRef} and used to find this handler on the worker. */
    String id();

    /** Whether this handler can proxy {@code value}. */
    boolean handles(Object value);

    /** Reduce {@code value} to a serializable reference (e.g. a file path, a config map). */
    Map<String, Object> deconstruct(T value);

    /** Rebuild the resource from a reference produced by {@link #deconstruct}. */
    T reconstruct(Map<String, Object> reference);

    /**
     * Release a resource this handler reconstructed, invoked (LIFO, once per
     * unique instance) when a {@link ProxySession} that produced it closes.
     * Direct {@link Proxies#reconstruct(ProxyRef)} calls have no lifecycle and
     * never trigger this. Default: no-op.
     */
    default void cleanup(T value) {}
}
