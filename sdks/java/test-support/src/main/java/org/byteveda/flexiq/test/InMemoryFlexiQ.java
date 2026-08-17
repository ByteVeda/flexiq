package org.byteveda.flexiq.test;

import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.serialization.Serializer;

/**
 * Opens a {@link FlexiQ} backed by an {@link InMemoryQueueBackend} — no JNI, no
 * disk. Intended for fast unit tests of producers, handlers, retries, and
 * dead-lettering. Workflows are not supported in-memory.
 */
public final class InMemoryFlexiQ {
    private InMemoryFlexiQ() {}

    /** A queue over a fresh in-memory backend using the default JSON serializer. */
    public static FlexiQ open() {
        return FlexiQ.builder().open(new InMemoryQueueBackend());
    }

    /** A queue over a fresh in-memory backend with a custom serializer. */
    public static FlexiQ open(Serializer serializer) {
        return FlexiQ.builder().serializer(serializer).open(new InMemoryQueueBackend());
    }
}
