package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Topic;
import org.byteveda.flexiq.model.TopicMessage;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Topics registry: declaring a log topic retains its publishes even with zero
 * subscribers, so a consumer that subscribes later still sees them. The core
 * behaviour is unit-tested in Rust; this drives the Java surface end-to-end.
 */
class TopicRegistryTest {

    // readTopic returns the raw payload bytes; decode them with the queue's
    // default serializer to recover the published value.
    private static final Serializer SERIALIZER = new JsonSerializer();

    private FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    void declaredTopicRetainsPublishesWithoutSubscriber(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.declareTopic("events");

            // Publish before any subscriber exists — no fan-out jobs, but the
            // messages are retained because the topic is declared.
            assertTrue(queue.publish("events", "a").isEmpty());
            assertTrue(queue.publish("events", "b").isEmpty());

            // A consumer that subscribes later still sees the earlier messages.
            queue.subscribeLog("events", "late");
            List<TopicMessage> msgs = queue.readTopic("events", "late");
            assertEquals(List.of("a", "b"), decodeAll(msgs));
        }
    }

    @Test
    void listsDeclaredTopicsAndRoundTripsRetention(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.declareTopic("plain");
            queue.declareTopic("bounded", Duration.ofSeconds(2));

            Map<String, Topic> byName = new HashMap<>();
            for (Topic topic : queue.listDeclaredTopics()) {
                byName.put(topic.name(), topic);
            }

            assertEquals("log", byName.get("plain").mode());
            assertNull(byName.get("plain").retentionMs());
            assertEquals("log", byName.get("bounded").mode());
            assertEquals(2000L, byName.get("bounded").retentionMs());
        }
    }

    private static List<String> decodeAll(List<TopicMessage> msgs) {
        List<String> out = new ArrayList<>(msgs.size());
        for (TopicMessage msg : msgs) {
            out.add(SERIALIZER.deserialize(msg.payload, String.class));
        }
        return out;
    }
}
