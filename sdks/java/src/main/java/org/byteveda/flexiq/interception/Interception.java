package org.byteveda.flexiq.interception;

/**
 * The outcome of an {@link Interceptor}: one of the interception strategies.
 *
 * <ul>
 *   <li>{@link Pass} — enqueue the payload unchanged.
 *   <li>{@link Convert} — replace the payload (e.g. with a proxy reference).
 *   <li>{@link Redirect} — enqueue a different task (and payload) instead.
 *   <li>{@link Reject} — block the enqueue with a reason.
 * </ul>
 */
public sealed interface Interception
        permits Interception.Pass, Interception.Convert, Interception.Redirect, Interception.Reject {

    /** Enqueue the original payload unchanged. */
    record Pass() implements Interception {}

    /**
     * Enqueue {@code payload} in place of the original.
     *
     * @param payload what to enqueue instead
     */
    record Convert(Object payload) implements Interception {}

    /**
     * Enqueue {@code taskName} with {@code payload} instead of the original task.
     *
     * @param taskName the task to enqueue instead
     * @param payload the payload to enqueue it with
     */
    record Redirect(String taskName, Object payload) implements Interception {}

    /**
     * Block the enqueue; {@code reason} is surfaced on the thrown exception.
     *
     * @param reason why the enqueue was blocked
     */
    record Reject(String reason) implements Interception {}

    /**
     * Enqueue the original payload unchanged.
     *
     * @return a {@link Pass}
     */
    static Interception pass() {
        return new Pass();
    }

    /**
     * Enqueue {@code payload} in place of the original.
     *
     * @param payload what to enqueue instead
     * @return a {@link Convert}
     */
    static Interception convert(Object payload) {
        return new Convert(payload);
    }

    /**
     * Enqueue a different task instead of the original.
     *
     * @param taskName the task to enqueue instead
     * @param payload the payload to enqueue it with
     * @return a {@link Redirect}
     */
    static Interception redirect(String taskName, Object payload) {
        return new Redirect(taskName, payload);
    }

    /**
     * Block the enqueue.
     *
     * @param reason why, surfaced on the thrown exception
     * @return a {@link Reject}
     */
    static Interception reject(String reason) {
        return new Reject(reason);
    }
}
