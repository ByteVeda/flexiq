package org.byteveda.flexiq.middleware;

import org.byteveda.flexiq.events.OutcomeEvent;

/**
 * Cross-cutting hooks around enqueue, task execution, and job outcomes. All are
 * optional. {@code onEnqueue} runs on the producer before serialization;
 * {@code before}/{@code after}/{@code onError} wrap execution; the outcome hooks
 * fire after the core decides the result. Register with {@link org.byteveda.flexiq.FlexiQ#use}.
 *
 * <p><b>Every {@code before} is matched by exactly one of {@code after} or
 * {@code onSleep}.</b> An attempt that ended in a durable {@code step.sleep} has
 * not finished, so it gets {@code onSleep}; anything that opens state in
 * {@code before} and implements only {@code after} leaks it whenever a task
 * sleeps, and the worker warns once when it sees that shape.
 */
public interface Middleware {
    default void onEnqueue(EnqueueContext context) {}

    default void before(TaskContext context) {}

    default void after(TaskContext context, Object result) {}

    default void onError(TaskContext context, Throwable error) {}

    /**
     * The attempt ended in a durable {@code step.sleep}: no result, no failure.
     *
     * <p>The job is {@code Pending} at {@code wakeAt} and will run again from
     * its memoized steps, so close whatever {@code before} opened rather than
     * completing it. {@code after(context, null)} would read as "the task
     * returned null" — a success to a timer, a span or a counter — which is
     * exactly what this hook exists to avoid.
     *
     * @param wakeAt the deadline the job was rescheduled to, in Unix milliseconds
     */
    default void onSleep(TaskContext context, long wakeAt) {}

    default void onCompleted(OutcomeEvent event) {}

    default void onRetry(OutcomeEvent event) {}

    default void onDeadLetter(OutcomeEvent event) {}

    default void onCancel(OutcomeEvent event) {}
}
