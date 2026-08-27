package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * Two handlers claim one task name, so registering both would silently leave one
 * of them unreachable — jobs for it would run the other one's code.
 *
 * <p>Raised by {@code discover()} rather than resolved by a rule: within one
 * compilation the {@code @TaskHandler} processor fails the build instead, and
 * across jars there is no basis for preferring either side.
 */
public class DuplicateTaskException extends FlexiQException {
    /**
     * Two handlers claiming one task name.
     *
     * @param message the task name both handlers claim, and where each came from
     */
    public DuplicateTaskException(String message) {
        super(message);
    }
}
