package org.byteveda.taskito.interception;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * What the registered interceptors would do with one enqueue, without enqueuing
 * anything. Returned by {@code Taskito.analyzeArguments}.
 *
 * @param taskName task that would be enqueued — differs from the input after a
 *     {@link Interception.Redirect}
 * @param payload payload that would be serialized, after every convert/redirect
 * @param outcomes one entry per interceptor that ran, in registration order
 * @param rejected true when a real enqueue would throw
 *     {@link org.byteveda.taskito.errors.InterceptionException}. {@code taskName} and
 *     {@code payload} then hold the original input — the chain stopped part-way, and
 *     {@code outcomes} shows how far it got.
 * @param rejectionReason message the real enqueue would throw; {@code null} unless
 *     {@code rejected}
 */
public record InterceptionAnalysis(
        String taskName,
        @Nullable Object payload,
        List<Interception> outcomes,
        boolean rejected,
        @Nullable String rejectionReason) {

    public InterceptionAnalysis {
        outcomes = List.copyOf(outcomes);
    }
}
