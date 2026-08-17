package org.byteveda.flexiq.middleware;

import org.byteveda.flexiq.events.OutcomeEvent;

/**
 * Cross-cutting hooks around enqueue, task execution, and job outcomes. All are
 * optional. {@code onEnqueue} runs on the producer before serialization;
 * {@code before}/{@code after}/{@code onError} wrap execution; the outcome hooks
 * fire after the core decides the result. Register with {@link org.byteveda.flexiq.FlexiQ#use}.
 */
public interface Middleware {
    default void onEnqueue(EnqueueContext context) {}

    default void before(TaskContext context) {}

    default void after(TaskContext context, Object result) {}

    default void onError(TaskContext context, Throwable error) {}

    default void onCompleted(OutcomeEvent event) {}

    default void onRetry(OutcomeEvent event) {}

    default void onDeadLetter(OutcomeEvent event) {}

    default void onCancel(OutcomeEvent event) {}
}
