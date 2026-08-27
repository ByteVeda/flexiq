package org.byteveda.flexiq.worker;

import java.util.List;
import org.byteveda.flexiq.model.TopicMessage;

/**
 * The narrow read/ack surface a managed {@link LogConsumerThread} needs over a log
 * topic's cursor. Implemented by the owning client so the poll loop depends only on
 * these two operations, not the whole client.
 */
public interface LogTopicReader {

    /**
     * Pull up to {@code limit} messages after the subscription's cursor, oldest first.
     *
     * @param topic the log being read
     * @param name the consumer whose cursor to read from
     * @param limit the most messages to return
     * @return the messages, oldest first; empty when the consumer is caught up
     */
    List<TopicMessage> readTopic(String topic, String name, int limit);

    /**
     * Advance the subscription's cursor to {@code cursor}; false when nothing moved.
     *
     * @param topic the log being read
     * @param name the consumer whose cursor to advance
     * @param cursor the id of the last message handled
     * @return whether the cursor moved
     */
    boolean ackTopic(String topic, String name, String cursor);
}
