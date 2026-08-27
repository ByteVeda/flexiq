package org.byteveda.flexiq.internal;

/**
 * A single seam for per-task context propagation. Backed by a {@link ThreadLocal}
 * today (the SDK targets Java 17); if the floor later rises to a JDK with a
 * stable {@code ScopedValue}, only this class changes — callers are unaffected.
 *
 * <p>Each task runs on a pooled worker thread, so callers must {@link #set} on
 * entry and {@link #clear} in a {@code finally} on exit to avoid leaking context
 * into the next task scheduled on that thread.
 *
 * @param <T> the value carried for the duration of a task
 */
public final class ScopeContext<T> {
    /** An empty seam; nothing is bound until {@link #set} is called on a thread. */
    public ScopeContext() {}

    private final ThreadLocal<T> holder = new ThreadLocal<>();

    /**
     * The value bound on the current thread, or {@code null} if none.
     *
     * @return the bound value, or {@code null} outside a scope
     */
    public T get() {
        return holder.get();
    }

    /**
     * Bind {@code value} on the current thread.
     *
     * @param value what {@link #get} returns until {@link #clear} runs
     */
    public void set(T value) {
        holder.set(value);
    }

    /** Unbind any value on the current thread. */
    public void clear() {
        holder.remove();
    }
}
