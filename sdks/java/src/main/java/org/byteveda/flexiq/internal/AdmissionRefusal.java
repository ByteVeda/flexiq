package org.byteveda.flexiq.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.byteveda.flexiq.errors.QueueFullException;
import org.jspecify.annotations.Nullable;

/**
 * Reads back an admission refusal the core decided.
 *
 * <p>A debounced enqueue is the one enqueue whose {@code maxPending} cap is applied by storage:
 * a call that lands on an open window inserts nothing, and only the debounce write knows which
 * of the two it is. The counts behind that refusal come back on the failure message, anchored
 * on its end so a queue name can never be mistaken for the two integers.
 */
public final class AdmissionRefusal {

    private static final Pattern QUEUE_FULL = Pattern.compile("\\bis full: (\\d+) pending >= max_pending (\\d+)$");

    private AdmissionRefusal() {}

    /**
     * The rejection {@code failure} describes, or {@code null} for any other failure — which the
     * caller rethrows untouched rather than reporting as a full queue.
     *
     * @param failure the failure the native enqueue reported
     * @param queue the queue the enqueue targeted
     * @return the equivalent rejection, or {@code null} if this was not one
     */
    public static @Nullable QueueFullException from(Throwable failure, String queue) {
        String message = failure.getMessage();
        if (message == null) {
            return null;
        }
        Matcher match = QUEUE_FULL.matcher(message);
        return match.find()
                ? new QueueFullException(queue, Long.parseLong(match.group(1)), Long.parseLong(match.group(2)))
                : null;
    }
}
