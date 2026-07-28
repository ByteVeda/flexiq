package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.pubsub.LogConsumerOptions;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * S28 managed log consumer: a daemon thread pulls a log topic's cursor, invokes the
 * handler per message, and advances the cursor. The core cursor algorithm is
 * unit-tested in Rust; this drives the Java poll loop and its {@code onError} policy
 * end-to-end. All consumers share one worker (one startup, not one per case — a heavy
 * worker lifecycle per test starves cold CI runners); assertions poll aggregate
 * collections rather than rendezvous between threads.
 */
class LogConsumerTest {

    @Test
    @Timeout(90)
    void managedConsumersInOneWorker(@TempDir Path dir) throws Exception {
        List<String> invoke = new CopyOnWriteArrayList<>();
        List<String> retry = new CopyOnWriteArrayList<>();
        List<String> skip = new CopyOnWriteArrayList<>();
        try (Taskito queue = Taskito.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.logConsumer("t-invoke", "worker", String.class, invoke::add);
            queue.logConsumer(
                    "t-retry",
                    "worker",
                    String.class,
                    value -> {
                        retry.add(value);
                        if (value.equals("boom")) {
                            throw new IllegalStateException("handler blew up");
                        }
                    },
                    LogConsumerOptions.builder()
                            .pollIntervalMs(50)
                            .onError("retry")
                            .build());
            queue.logConsumer(
                    "t-skip",
                    "worker",
                    String.class,
                    value -> {
                        skip.add(value);
                        if (value.equals("boom")) {
                            throw new IllegalStateException("handler blew up");
                        }
                    },
                    LogConsumerOptions.builder()
                            .pollIntervalMs(50)
                            .onError("skip")
                            .build());

            queue.publish("t-invoke", "a");
            queue.publish("t-invoke", "b");
            queue.publish("t-invoke", "c");
            for (String value : List.of("ok", "boom", "after")) {
                queue.publish("t-retry", value);
                queue.publish("t-skip", value);
            }

            Worker worker = queue.worker().start();
            try (worker) {
                // Plain delivery: every handled message is acked, so the cursor catches up.
                pollUntil(() -> invoke.size() == 3);
                assertEquals(List.of("a", "b", "c"), invoke);
                pollUntil(() -> lag(queue, "t-invoke") == 0);

                // retry never acks past the failure, so "boom" re-reads every poll.
                pollUntil(() -> count(retry, "boom") >= 3);
                assertEquals(1, count(retry, "ok"), "the acked predecessor is not redelivered");
                assertEquals(0, count(retry, "after"), "the poison blocks the rest of the batch");

                // skip acks past the poison, so it runs once and the batch drains fully.
                pollUntil(() -> skip.contains("after"));
                pollUntil(() -> lag(queue, "t-skip") == 0);
                assertEquals(List.of("ok", "boom", "after"), skip);
            }
        }
    }

    private static long lag(Taskito queue, String topic) {
        return queue.topicLogStats().stream()
                .filter(s -> s.topic.equals(topic) && s.subscription.equals("worker"))
                .findFirst()
                .orElseThrow()
                .lag;
    }

    private static long count(List<String> values, String target) {
        return values.stream().filter(target::equals).count();
    }

    /** Poll a condition to a generous deadline (cold runners are slow), then assert it. */
    private static void pollUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && !condition.getAsBoolean()) {
            Thread.sleep(25);
        }
        assertTrue(condition.getAsBoolean(), "condition not met within 30s");
    }
}
