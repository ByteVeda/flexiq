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
    /**
     * On the producer, before the payload is serialized.
     *
     * @param context the job being enqueued; rewrite its payload or options here,
     *     or throw to abort the enqueue
     */
    default void onEnqueue(EnqueueContext context) {}

    /**
     * On the worker, before the handler runs.
     *
     * @param context this execution, whose {@code attributes()} carry state to the
     *     matching {@code after} or {@code onSleep}
     */
    default void before(TaskContext context) {}

    /**
     * On the worker, after the handler returned.
     *
     * @param context this execution, the same instance {@code before} saw
     * @param result what the handler returned; {@code null} for a void handler
     */
    default void after(TaskContext context, Object result) {}

    /**
     * On the worker, after the handler threw.
     *
     * @param context this execution, the same instance {@code before} saw
     * @param error what the handler threw, before the core classifies it
     */
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
     * @param context this execution, the same instance {@code before} saw
     * @param wakeAt the deadline the job was rescheduled to, in Unix milliseconds
     */
    default void onSleep(TaskContext context, long wakeAt) {}

    /**
     * The job finished successfully.
     *
     * @param event the outcome the core recorded
     */
    default void onCompleted(OutcomeEvent event) {}

    /**
     * The attempt failed and the job will be tried again.
     *
     * @param event the outcome the core recorded, carrying the attempt's error
     */
    default void onRetry(OutcomeEvent event) {}

    /**
     * The job exhausted its retries and was moved to the dead-letter queue.
     *
     * @param event the outcome the core recorded, carrying the final error
     */
    default void onDeadLetter(OutcomeEvent event) {}

    /**
     * The job was cancelled rather than run to a verdict.
     *
     * @param event the outcome the core recorded
     */
    default void onCancel(OutcomeEvent event) {}
}
