package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A gate returned {@code Skip} for an enqueue, so no job was created. Thrown by
 * {@code enqueue}; callers that expect skips should use {@code tryEnqueue},
 * which returns an empty {@code Optional} instead.
 */
public class EnqueueSkippedException extends FlexiQException {
    public EnqueueSkippedException(String message) {
        super(message);
    }
}
