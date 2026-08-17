package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.internal.JniQueueBackend;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.model.Subscription;
import org.byteveda.flexiq.model.TopicStat;
import org.byteveda.flexiq.pubsub.PublishOptions;
import org.byteveda.flexiq.pubsub.SubscriptionOptions;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class PubSubTest {

    private static final Task<String> SEND_EMAIL = Task.of("pubsub.send_email", String.class);
    private static final Task<String> TRACK_ORDER = Task.of("pubsub.track_order", String.class);

    private FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    void subscriptionNameDefaultsToTaskName(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            List<Subscription> subs = queue.listSubscriptions("orders");
            assertEquals(1, subs.size());
            Subscription sub = subs.get(0);
            assertEquals("orders", sub.topic);
            assertEquals(SEND_EMAIL.name(), sub.name);
            assertEquals(SEND_EMAIL.name(), sub.taskName);
            assertEquals("default", sub.queue);
            assertTrue(sub.active);
            assertTrue(sub.durable);
            assertEquals(List.of("orders"), queue.listTopics());
        }
    }

    @Test
    void redeclareUpdatesRoutingInsteadOfDuplicating(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe(
                    "orders",
                    SEND_EMAIL,
                    SubscriptionOptions.builder().name("email").build());
            queue.subscribe(
                    "orders",
                    TRACK_ORDER,
                    SubscriptionOptions.builder().name("email").queue("emails").build());
            List<Subscription> subs = queue.listSubscriptions("orders");
            assertEquals(1, subs.size());
            assertEquals(TRACK_ORDER.name(), subs.get(0).taskName);
            assertEquals("emails", subs.get(0).queue);
        }
    }

    @Test
    void unsubscribeReportsWhetherAnythingWasRemoved(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            assertTrue(queue.unsubscribe("orders", SEND_EMAIL.name()));
            assertFalse(queue.unsubscribe("orders", SEND_EMAIL.name()));
            assertTrue(queue.listSubscriptions().isEmpty());
        }
    }

    @Test
    void publishWithoutSubscribersIsANoOp(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            assertTrue(queue.publish("orders", "o-1").isEmpty());
            assertEquals(0, queue.stats().pending);
        }
    }

    @Test
    void publishFansOutOnePerSubscriberWithStampedNotes(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            queue.subscribe("orders", TRACK_ORDER);

            List<Job> deliveries = queue.publish(
                    "orders",
                    "o-1",
                    PublishOptions.builder().notes(Map.of("tenant", "acme")).build());

            assertEquals(2, deliveries.size());
            for (Job job : deliveries) {
                Map<String, Object> notes = job.notesMap().orElseThrow();
                assertEquals("orders", notes.get("topic"));
                assertEquals("acme", notes.get("tenant"));
            }
            List<String> tasks = new ArrayList<>();
            deliveries.forEach(job -> tasks.add(job.taskName));
            Collections.sort(tasks);
            assertEquals(List.of(SEND_EMAIL.name(), TRACK_ORDER.name()), tasks);
        }
    }

    @Test
    void pauseBlocksDeliveriesAndResumeRestoresThem(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            queue.subscribe("orders", TRACK_ORDER);

            assertTrue(queue.pauseSubscription("orders", TRACK_ORDER.name()));
            List<Job> paused = queue.publish("orders", "o-1");
            assertEquals(1, paused.size());
            assertEquals(SEND_EMAIL.name(), paused.get(0).taskName);
            // The paused row stays registered (visible in the all-topics listing).
            assertEquals(1, queue.listSubscriptions("orders").size());
            assertEquals(2, queue.listSubscriptions().size());

            assertTrue(queue.resumeSubscription("orders", TRACK_ORDER.name()));
            assertEquals(2, queue.publish("orders", "o-2").size());
            assertFalse(queue.pauseSubscription("orders", "unknown"));
        }
    }

    @Test
    void idempotencyKeyDedupesPerSubscriber(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            queue.subscribe("orders", TRACK_ORDER);
            PublishOptions keyed =
                    PublishOptions.builder().idempotencyKey("evt-42").build();

            List<Job> first = queue.publish("orders", "o-1", keyed);
            List<Job> second = queue.publish("orders", "o-1", keyed);
            assertEquals(2, first.size());
            assertEquals(2, second.size());
            assertEquals(ids(first), ids(second));
            assertEquals(2, queue.stats().pending);

            // A subscriber added after the first publish still gets its own copy.
            queue.subscribe(
                    "orders",
                    SEND_EMAIL,
                    SubscriptionOptions.builder().name("audit").build());
            List<Job> third = queue.publish("orders", "o-1", keyed);
            assertEquals(3, third.size());
            assertEquals(3, queue.stats().pending);
        }
    }

    @Test
    void deliveriesHonorSubscriberTaskDefaultsAndPublishOverrides(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL.priority(7).maxRetries(2));
            queue.subscribe("orders", TRACK_ORDER);

            for (Job job : queue.publish("orders", "o-1")) {
                if (job.taskName.equals(SEND_EMAIL.name())) {
                    assertEquals(7, job.priority);
                    assertEquals(2, job.maxRetries);
                } else {
                    assertEquals(0, job.priority);
                }
            }

            // An explicit publish-level override beats the subscriber's defaults.
            List<Job> overridden = queue.publish(
                    "orders", "o-2", PublishOptions.builder().priority(9).build());
            overridden.forEach(job -> assertEquals(9, job.priority));
        }
    }

    @Test
    void producerOnlyPublishAppliesPersistedRowDeliverySettings(@TempDir Path dir) throws Exception {
        String options = new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString()));
        JniQueueBackend backend = JniQueueBackend.open(options);
        try (FlexiQ producer = FlexiQ.builder().open(backend)) {
            // A subscriber elsewhere persisted its own delivery settings on the row.
            backend.registerSubscription("orders", "sink", SEND_EMAIL.name(), "default", true, null, 5, 7, 30_000L);

            // A producer with no local knowledge of SEND_EMAIL still fans the
            // delivery out with the subscriber's persisted settings.
            List<Job> deliveries = producer.publish("orders", "o-1");
            assertEquals(1, deliveries.size());
            Job delivery = deliveries.get(0);
            assertEquals(SEND_EMAIL.name(), delivery.taskName);
            assertEquals(5, delivery.priority);
            assertEquals(7, delivery.maxRetries);
            assertEquals(30_000L, delivery.timeoutMs);
        }
    }

    @Test
    @Timeout(30)
    void ephemeralSubscriptionRegistersWithARunningWorker(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe(
                    "orders",
                    SEND_EMAIL,
                    SubscriptionOptions.builder().durable(false).build());
            // No owning worker yet, so the subscription is not registered.
            assertTrue(queue.publish("orders", "o-1").isEmpty());

            Worker worker =
                    queue.worker().handle(SEND_EMAIL, payload -> payload).start();
            try (worker) {
                List<Job> deliveries = queue.publish("orders", "o-2");
                assertEquals(1, deliveries.size());
                Job done = queue.awaitJob(deliveries.get(0).id, Duration.ofSeconds(10))
                        .orElseThrow();
                assertEquals(JobStatus.COMPLETE, done.status);
                assertEquals("o-2", queue.getResult(done.id, String.class).orElseThrow());
            }
        }
    }

    @Test
    @Timeout(30)
    void reapSparesFreshEphemeralRowsWithinTheRegistrationGrace(@TempDir Path dir) throws Exception {
        String options = new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString()));
        JniQueueBackend backend = JniQueueBackend.open(options);
        try (FlexiQ queue = FlexiQ.builder().open(backend)) {
            Worker worker = queue.worker().start();
            try (worker) {
                String liveWorkerId = queue.listWorkers().get(0).workerId;
                backend.registerSubscription("orders", "live", SEND_EMAIL.name(), "default", false, liveWorkerId);
                backend.registerSubscription("orders", "ghost", SEND_EMAIL.name(), "default", false, "gone-worker");
                backend.registerSubscription("orders", "durable", SEND_EMAIL.name(), "default", true, null);

                // Even the dead-owned row is inside the core's registration
                // grace window, so nothing is eligible yet — a starting
                // worker's rows must not be raced away before its liveness
                // becomes visible. (Post-grace reaping is covered by the
                // core's storage tests, which can backdate rows.)
                assertEquals(0, backend.reapEphemeralSubscriptions());
                List<String> names = new ArrayList<>();
                queue.listSubscriptions("orders").forEach(sub -> names.add(sub.name));
                Collections.sort(names);
                assertEquals(List.of("durable", "ghost", "live"), names);
            }
        }
    }

    @Test
    void ephemeralRegistrationWithoutAnOwnerIsRejected(@TempDir Path dir) throws Exception {
        String options = new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString()));
        JniQueueBackend backend = JniQueueBackend.open(options);
        FlexiQ queue = FlexiQ.builder().open(backend);
        try (queue) {
            FlexiQException rejected = assertThrows(
                    FlexiQException.class,
                    () -> backend.registerSubscription("orders", "sink", SEND_EMAIL.name(), "default", false, null));
            assertTrue(rejected.getMessage().contains("requires ownerWorkerId"));
        }
    }

    @Test
    @Timeout(30)
    void pauseSurvivesRedeclareAndWorkerStart(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            assertTrue(queue.pauseSubscription("orders", SEND_EMAIL.name()));

            // Re-declaring must not resume the paused subscription.
            queue.subscribe("orders", SEND_EMAIL);
            assertTrue(queue.publish("orders", "o-1").isEmpty());

            // Neither must a worker start re-registering the declaration.
            Worker worker =
                    queue.worker().handle(SEND_EMAIL, payload -> payload).start();
            try (worker) {
                assertTrue(queue.publish("orders", "o-2").isEmpty());
            }
        }
    }

    @Test
    @Timeout(30)
    void unsubscribeDropsTheLocalDeclarationSoWorkerStartDoesNotResurrectIt(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);
            assertTrue(queue.unsubscribe("orders", SEND_EMAIL.name()));
            Worker worker =
                    queue.worker().handle(SEND_EMAIL, payload -> payload).start();
            try (worker) {
                assertTrue(queue.listSubscriptions("orders").isEmpty());
                assertTrue(queue.publish("orders", "o-1").isEmpty());
            }
        }
    }

    @Test
    void topicStatsReportsOneBacklogRowPerSubscription(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe(
                    "orders",
                    SEND_EMAIL,
                    SubscriptionOptions.builder().name("email").build());
            queue.subscribe(
                    "orders",
                    TRACK_ORDER,
                    SubscriptionOptions.builder().name("analytics").build());
            queue.publish("orders", "o-1");
            queue.publish("orders", "o-2");

            Map<String, TopicStat> stats = bySubscription(queue.topicStats("orders"));
            assertEquals(Set.of("email", "analytics"), stats.keySet());
            TopicStat email = stats.get("email");
            assertEquals("orders", email.topic);
            assertEquals(SEND_EMAIL.name(), email.taskName);
            assertEquals("default", email.queue);
            assertTrue(email.active);
            assertTrue(email.durable);
            assertEquals(2, email.pending);
            assertEquals(0, email.running);
            assertEquals(0, email.dead);
            assertNotNull(email.oldestPendingAgeMs);
            assertTrue(email.oldestPendingAgeMs >= 0);
            assertEquals(2, stats.get("analytics").pending);
        }
    }

    @Test
    void topicStatsReportsZerosForAnIdleSubscription(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe("orders", SEND_EMAIL);

            List<TopicStat> stats = queue.topicStats("orders");
            assertEquals(1, stats.size());
            TopicStat idle = stats.get(0);
            assertEquals(0, idle.pending);
            assertEquals(0, idle.running);
            assertEquals(0, idle.dead);
            assertNull(idle.oldestPendingAgeMs);
        }
    }

    @Test
    void topicStatsFiltersByTopicAndIgnoresNonPubSubJobs(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.subscribe(
                    "orders",
                    SEND_EMAIL,
                    SubscriptionOptions.builder().name("email").build());
            queue.enqueue(Task.of("pubsub.plain", String.class), "p-1");

            assertEquals(1, queue.topicStats().size());
            assertEquals("email", queue.topicStats().get(0).subscription);
            assertTrue(queue.topicStats("other-topic").isEmpty());
            // A null topic is "no filter", not "a topic nothing matches".
            assertEquals(1, queue.topicStats(null).size());
        }
    }

    @Test
    @Timeout(30)
    void topicStatsCountsAFailedDeliveryAsDead(@TempDir Path dir) throws Exception {
        Task<String> flaky = Task.of("pubsub.flaky", String.class).maxRetries(0);
        try (FlexiQ queue = open(dir)) {
            queue.subscribe(
                    "orders", flaky, SubscriptionOptions.builder().name("flaky").build());

            CountDownLatch dead = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(flaky, (String payload) -> {
                        throw new IllegalStateException("boom");
                    })
                    .on(EventName.DEAD, event -> dead.countDown())
                    .start();
            try (worker) {
                queue.publish("orders", "o-1");
                assertTrue(dead.await(20, TimeUnit.SECONDS), "the delivery should dead-letter");
            }

            TopicStat stat = queue.topicStats("orders").get(0);
            assertEquals(1, stat.dead);
            assertEquals(0, stat.pending);
        }
    }

    private static Map<String, TopicStat> bySubscription(List<TopicStat> stats) {
        Map<String, TopicStat> bySubscription = new LinkedHashMap<>();
        stats.forEach(stat -> bySubscription.put(stat.subscription, stat));
        return bySubscription;
    }

    private static List<String> ids(List<Job> jobs) {
        List<String> ids = new ArrayList<>();
        jobs.forEach(job -> ids.add(job.id));
        Collections.sort(ids);
        return ids;
    }
}
