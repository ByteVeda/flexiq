package org.byteveda.flexiq.worker;

import java.util.List;
import org.byteveda.flexiq.model.TopicMessage;

/**
 * The narrow read/ack surface a managed {@link LogConsumerThread} needs over a log
 * topic's cursor. Implemented by the owning client so the poll loop depends only on
 * these two operations, not the whole client.
 */
public interface LogTopicReader {

    /** Pull up to {@code limit} messages after the subscription's cursor, oldest first. */
    List<TopicMessage> readTopic(String topic, String name, int limit);

    /** Advance the subscription's cursor to {@code cursor}; false when nothing moved. */
    boolean ackTopic(String topic, String name, String cursor);
}
