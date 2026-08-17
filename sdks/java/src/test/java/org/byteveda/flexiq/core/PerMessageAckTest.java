package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.TopicMessage;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Per-message ack: lease / ack / nack / redelivery over a log topic. Unlike the
 * S28 cursor read, each message is leased and tracked individually, so a nack or
 * an expired lease redelivers just that message. The core lease algorithm is
 * unit-tested in Rust; this drives the Java surface end-to-end.
 */
class PerMessageAckTest {

    // leaseTopic returns raw payload bytes; decode them with the queue's default
    // serializer to recover the published value.
    private static final Serializer SERIALIZER = new JsonSerializer();

    private FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    void leaseSkipsInFlightMessages(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            for (int i = 0; i < 3; i++) {
                queue.publish("events", String.valueOf(i));
            }

            // First lease takes the two oldest; both are in-flight for 30s.
            List<TopicMessage> first = queue.leaseTopic("events", "c", 2, Duration.ofSeconds(30));
            assertEquals(List.of("0", "1"), decodeAll(first));

            // Second lease sees only the still-available third message.
            assertEquals(List.of("2"), decodeAll(queue.leaseTopic("events", "c")));
        }
    }

    @Test
    void nackRedeliversButAckDoesNot(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            queue.publish("events", "a");
            queue.publish("events", "b");

            List<TopicMessage> leased = queue.leaseTopic("events", "c");
            assertEquals(List.of("a", "b"), decodeAll(leased));

            // Ack the first (done forever), nack the second (redeliver now).
            assertTrue(queue.ackMessage("events", "c", leased.get(0).id));
            assertTrue(queue.nackMessage("events", "c", leased.get(1).id));

            assertEquals(List.of("b"), decodeAll(queue.leaseTopic("events", "c")));
        }
    }

    @Test
    void ackAndNackReturnFalseWithoutLease(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            queue.publish("events", "x");

            // No lease taken yet, so there is no un-acked delivery to ack or nack.
            String id = queue.readTopic("events", "c").get(0).id;
            assertFalse(queue.ackMessage("events", "c", id));
            assertFalse(queue.nackMessage("events", "c", id));
        }
    }

    @Test
    @Timeout(30)
    void expiredLeaseRedelivers(@TempDir Path dir) throws InterruptedException {
        try (FlexiQ queue = open(dir)) {
            queue.subscribeLog("events", "c");
            queue.publish("events", "x");

            Duration visibility = Duration.ofMillis(100);
            // Leased now; a second immediate lease sees nothing (still in-flight).
            assertEquals(List.of("x"), decodeAll(queue.leaseTopic("events", "c", 100, visibility)));
            assertTrue(queue.leaseTopic("events", "c", 100, visibility).isEmpty());

            // Poll until the lease expires and the un-acked message is available again.
            assertEquals(List.of("x"), pollForLease(queue, visibility));
        }
    }

    /** Poll leaseTopic until it yields a message or the deadline passes (redelivery timing). */
    private static List<String> pollForLease(FlexiQ queue, Duration visibility) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            List<TopicMessage> leased = queue.leaseTopic("events", "c", 100, visibility);
            if (!leased.isEmpty()) {
                return decodeAll(leased);
            }
            Thread.sleep(20);
        }
        return List.of();
    }

    private static List<String> decodeAll(List<TopicMessage> msgs) {
        List<String> out = new ArrayList<>(msgs.size());
        for (TopicMessage msg : msgs) {
            out.add(SERIALIZER.deserialize(msg.payload, String.class));
        }
        return out;
    }
}
