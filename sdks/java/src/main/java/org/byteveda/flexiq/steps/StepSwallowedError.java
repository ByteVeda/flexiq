package org.byteveda.flexiq.steps;

/**
 * The task body caught a step control signal and returned anyway.
 *
 * <p>{@link StepControlSignal} extends {@link Error} so an ordinary
 * {@code catch (Exception e)} cannot do this — but {@code catch (Throwable t)}
 * can, and some frameworks do it on the application's behalf. Whatever the body
 * went on to do then ran without an execution claim, or on a memo answering a
 * different question, so the attempt cannot be trusted and is failed here.
 *
 * <p>Raised by the worker after the handler returns, never by the step API.
 */
public final class StepSwallowedError extends StepError {
    private static final long serialVersionUID = 1L;

    /**
     * Raised by the worker once the latch shows a signal was caught and dropped.
     *
     * @param message which signal was swallowed, and what to do instead
     */
    public StepSwallowedError(String message) {
        super(message, false);
    }
}
