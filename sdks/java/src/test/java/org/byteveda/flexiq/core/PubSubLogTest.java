package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.model.TopicLogStat;
import org.byteveda.flexiq.model.TopicMessage;
import org.byteveda.flexiq.pubsub.PublishOptions;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.serialization.Serializer;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * S28 — log topics: one stored message per publish, pulled via a per-subscription
 * cursor. The core cursor algorithm is unit-tested in Rust; this drives the Java
 * surface end-to-end (subscribeLog / readTopic / ackTopic / topicLogStats).
 */
class PubSubLogTest {

    // readTopic returns the raw payload bytes (like getResult); decode them with
    // the queue's default serializer to recover the published value.
    private static final Serializer SERIALIZER = new JsonSerializer();

    private FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    void publishStoresOneMessagePerCallReadInOrder(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "analytics");

            // No fan-out jobs: one stored message per publish, regardless of readers.
            assertTrue(queue.publish(
                            "events",
                            "a",
                            PublishOptions.builder()
                                    .notes(Map.of("tenant", "acme"))
                                    .build())
                    .isEmpty());
            assertTrue(queue.publish("events", "b").isEmpty());
            assertEquals(0, queue.stats().pending);

            List<TopicMessage> msgs = queue.readTopic("events", "analytics");
            assertEquals(List.of("a", "b"), decodeAll(msgs));
            // The caller's notes ride along on the stored message.
            assertEquals("acme", msgs.get(0).notes.get("tenant"));
        }
    }

    @Test
    void readIsEmptyBeforeAnyPublish(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "analytics");
            assertTrue(queue.readTopic("events", "analytics").isEmpty());
        }
    }

    @Test
    void ackAdvancesCursorAndIsMonotonic(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            for (int i = 0; i < 3; i++) {
                queue.publish("events", String.valueOf(i));
            }
            List<TopicMessage> msgs = queue.readTopic("events", "c");
            assertEquals(List.of("0", "1", "2"), decodeAll(msgs));

            // Ack through the middle message: the next read starts after it.
            assertTrue(queue.ackTopic("events", "c", msgs.get(1).id));
            assertEquals(List.of("2"), decodeAll(queue.readTopic("events", "c")));

            // Acking an older cursor never rewinds.
            assertFalse(queue.ackTopic("events", "c", msgs.get(0).id));
            assertEquals(List.of("2"), decodeAll(queue.readTopic("events", "c")));
        }
    }

    @Test
    void unackedReadIsAtLeastOnce(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            queue.publish("events", "x");
            // Reading without acking (e.g. a crash mid-process) re-delivers.
            assertEquals(List.of("x"), decodeAll(queue.readTopic("events", "c")));
            assertEquals(List.of("x"), decodeAll(queue.readTopic("events", "c")));
        }
    }

    @Test
    void limitBoundsThePage(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            for (int i = 0; i < 5; i++) {
                queue.publish("events", String.valueOf(i));
            }
            List<TopicMessage> first = queue.readTopic("events", "c", 2);
            assertEquals(List.of("0", "1"), decodeAll(first));
            queue.ackTopic("events", "c", first.get(first.size() - 1).id);
            assertEquals(List.of("2", "3"), decodeAll(queue.readTopic("events", "c", 2)));
        }
    }

    @Test
    void lagReflectsUnacked(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            for (int i = 0; i < 3; i++) {
                queue.publish("events", String.valueOf(i));
            }

            TopicLogStat stat = single(queue.topicLogStats());
            assertEquals("events", stat.topic);
            assertEquals("c", stat.subscription);
            assertNull(stat.cursor);
            assertEquals(3, stat.lag);

            List<TopicMessage> msgs = queue.readTopic("events", "c");
            queue.ackTopic("events", "c", msgs.get(msgs.size() - 1).id);
            TopicLogStat caughtUp = single(queue.topicLogStats());
            assertEquals(0, caughtUp.lag);
            assertNull(caughtUp.oldestUnackedAgeMs);
        }
    }

    @Test
    @Timeout(30)
    void logAndFanoutSubscribersCoexist(@TempDir Path dir) {
        Task<String> onEvent = Task.of("pubsublog.on_event", String.class);
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("events", onEvent); // fan-out: one job per publish
            queue.subscribeLog("events", "log"); // log: one stored message per publish

            Worker worker = queue.worker().handle(onEvent, payload -> payload).start();
            try (worker) {
                List<Job> deliveries = queue.publish("events", "seen");

                // The fan-out subscriber ran its job to completion...
                assertEquals(1, deliveries.size());
                Job done = queue.awaitJob(deliveries.get(0).id, Duration.ofSeconds(10))
                        .orElseThrow();
                assertEquals(JobStatus.COMPLETE, done.status);
                assertEquals("seen", queue.getResult(done.id, String.class).orElseThrow());

                // ...and the same publish stored one message for the log subscriber.
                assertEquals(List.of("seen"), decodeAll(queue.readTopic("events", "log")));
            }
        }
    }

    private static List<String> decodeAll(List<TopicMessage> msgs) {
        List<String> out = new ArrayList<>(msgs.size());
        for (TopicMessage msg : msgs) {
            out.add(SERIALIZER.deserialize(msg.payload, String.class));
        }
        return out;
    }

    private static <T> T single(List<T> list) {
        assertEquals(1, list.size());
        return list.get(0);
    }
}
