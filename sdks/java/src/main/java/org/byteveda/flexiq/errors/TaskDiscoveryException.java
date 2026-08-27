package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A handler provider on the classpath could not be loaded or could not build its
 * handlers.
 *
 * <p>Fails {@code discover()} instead of being skipped: a worker that quietly
 * discovers most of its handlers dead-letters every job for the rest, which is
 * far harder to notice than a start-up failure naming the provider.
 */
public class TaskDiscoveryException extends FlexiQException {
    /**
     * A provider that could not be loaded or could not build its handlers.
     *
     * @param message which provider failed, so start-up names it
     * @param cause the load or construction failure underneath
     */
    public TaskDiscoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
