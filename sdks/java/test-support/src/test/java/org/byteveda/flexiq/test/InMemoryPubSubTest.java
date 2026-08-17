package org.byteveda.flexiq.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.model.Subscription;
import org.byteveda.flexiq.pubsub.PublishOptions;
import org.byteveda.flexiq.pubsub.SubscriptionOptions;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class InMemoryPubSubTest {

    private static final Task<String> EMAIL = Task.of("im.pubsub.email", String.class);
    private static final Task<String> AUDIT = Task.of("im.pubsub.audit", String.class);

    @Test
    @Timeout(20)
    void publishFansOutAndDeliversToEachSubscriberHandler() throws Exception {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            queue.subscribe("orders", AUDIT);
            Worker worker = queue.worker()
                    .handle(EMAIL, payload -> "email:" + payload)
                    .handle(AUDIT, payload -> "audit:" + payload)
                    .start();
            try (worker) {
                List<Job> deliveries = queue.publish("orders", "o-1");
                assertEquals(2, deliveries.size());
                for (Job delivery : deliveries) {
                    Job done =
                            queue.awaitJob(delivery.id, Duration.ofSeconds(10)).orElseThrow();
                    assertEquals(JobStatus.COMPLETE, done.status);
                    assertEquals(
                            done.taskName.equals(EMAIL.name()) ? "email:o-1" : "audit:o-1",
                            queue.getResult(done.id, String.class).orElseThrow());
                }
            }
        }
    }

    @Test
    void publishWithoutSubscribersIsANoOp() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            assertTrue(queue.publish("orders", "o-1").isEmpty());
        }
    }

    @Test
    @Timeout(20)
    void redeclareUpsertsOnTopicAndName() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe(
                    "orders", EMAIL, SubscriptionOptions.builder().name("sink").build());
            queue.subscribe(
                    "orders", AUDIT, SubscriptionOptions.builder().name("sink").build());
            List<Subscription> subs = queue.listSubscriptions("orders");
            assertEquals(1, subs.size());
            assertEquals(AUDIT.name(), subs.get(0).taskName);

            // The redeclare replaced (not appended to) the local declaration:
            // a worker start re-registers exactly one subscription.
            Worker worker = queue.worker().handle(AUDIT, payload -> payload).start();
            try (worker) {
                assertEquals(1, queue.listSubscriptions("orders").size());
                assertEquals(1, queue.publish("orders", "o-1").size());
            }
        }
    }

    @Test
    void unsubscribeReportsWhetherAnythingWasRemoved() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            assertTrue(queue.unsubscribe("orders", EMAIL.name()));
            assertFalse(queue.unsubscribe("orders", EMAIL.name()));
        }
    }

    @Test
    @Timeout(20)
    void unsubscribeDropsTheLocalDeclarationSoWorkerStartDoesNotResurrectIt() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            assertTrue(queue.unsubscribe("orders", EMAIL.name()));
            Worker worker = queue.worker().handle(EMAIL, payload -> payload).start();
            try (worker) {
                assertTrue(queue.listSubscriptions("orders").isEmpty());
                assertTrue(queue.publish("orders", "o-1").isEmpty());
            }
        }
    }

    @Test
    void pauseBlocksDeliveriesAndResumeRestoresThem() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            queue.subscribe("orders", AUDIT);

            assertTrue(queue.pauseSubscription("orders", AUDIT.name()));
            List<Job> paused = queue.publish("orders", "o-1");
            assertEquals(1, paused.size());
            assertEquals(EMAIL.name(), paused.get(0).taskName);
            assertEquals(1, queue.listSubscriptions("orders").size());
            assertEquals(2, queue.listSubscriptions().size());

            assertTrue(queue.resumeSubscription("orders", AUDIT.name()));
            assertEquals(2, queue.publish("orders", "o-2").size());
        }
    }

    @Test
    @Timeout(20)
    void pauseSurvivesRedeclareAndWorkerStart() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            assertTrue(queue.pauseSubscription("orders", EMAIL.name()));

            // Re-declaring must not resume the paused subscription.
            queue.subscribe("orders", EMAIL);
            assertTrue(queue.publish("orders", "o-1").isEmpty());

            // Neither must a worker start re-registering the declaration.
            Worker worker = queue.worker().handle(EMAIL, payload -> payload).start();
            try (worker) {
                assertTrue(queue.publish("orders", "o-2").isEmpty());
            }
        }
    }

    @Test
    void idempotencyKeyDedupesPerSubscriberButNotAcrossThem() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            queue.subscribe("orders", AUDIT);
            PublishOptions keyed =
                    PublishOptions.builder().idempotencyKey("evt-1").build();

            List<Job> first = queue.publish("orders", "o-1", keyed);
            List<Job> second = queue.publish("orders", "o-1", keyed);
            assertEquals(2, first.size());
            assertEquals(2, second.size());
            assertEquals(first.get(0).id, second.get(0).id);
            assertEquals(first.get(1).id, second.get(1).id);
            assertEquals(2, queue.stats().pending);
        }
    }

    @Test
    void deliveriesCarryStampedNotesAndTaskDefaults() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL.priority(7).maxRetries(2));
            List<Job> deliveries = queue.publish(
                    "orders",
                    "o-1",
                    PublishOptions.builder().notes(Map.of("tenant", "acme")).build());

            assertEquals(1, deliveries.size());
            Job delivery = deliveries.get(0);
            assertEquals(7, delivery.priority);
            assertEquals(2, delivery.maxRetries);
            Map<String, Object> notes = delivery.notesMap().orElseThrow();
            assertEquals("orders", notes.get("topic"));
            assertEquals(EMAIL.name(), notes.get("subscription"));
            assertEquals("acme", notes.get("tenant"));

            // An explicit publish-level override beats the subscriber's defaults.
            List<Job> overridden = queue.publish(
                    "orders", "o-2", PublishOptions.builder().priority(9).build());
            assertEquals(9, overridden.get(0).priority);
        }
    }

    @Test
    void producerOnlyPublishAppliesPersistedRowDeliverySettings() {
        // A subscriber (in another process) persisted its own delivery settings
        // on the subscription row, as a durable subscribe() does.
        InMemoryQueueBackend backend = new InMemoryQueueBackend();
        backend.registerSubscription("orders", "sink", AUDIT.name(), "default", true, null, 5, 7, 30_000L);

        // A producer that never registered AUDIT publishes with no local task
        // knowledge; the delivery still carries the subscriber's own settings.
        try (FlexiQ producer = FlexiQ.builder().open(backend)) {
            List<Job> deliveries = producer.publish("orders", "o-1");
            assertEquals(1, deliveries.size());
            Job delivery = deliveries.get(0);
            assertEquals(AUDIT.name(), delivery.taskName);
            assertEquals(5, delivery.priority);
            assertEquals(7, delivery.maxRetries);
            assertEquals(30_000L, delivery.timeoutMs);
        }
    }

    @Test
    @Timeout(20)
    void ephemeralSubscriptionBindsToAWorkerAndIsReapedWhenItStops() {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            queue.subscribe(
                    "orders",
                    AUDIT,
                    SubscriptionOptions.builder().durable(false).build());

            // The ephemeral subscriber has no owning worker yet: durable only.
            assertEquals(1, queue.publish("orders", "o-1").size());

            Worker worker = queue.worker()
                    .handle(EMAIL, payload -> payload)
                    .handle(AUDIT, payload -> payload)
                    .start();
            try (worker) {
                assertEquals(2, queue.publish("orders", "o-2").size());
            }

            // Worker stopped: its ephemeral subscription is reaped, durable survives.
            List<Job> after = queue.publish("orders", "o-3");
            assertEquals(1, after.size());
            assertEquals(EMAIL.name(), after.get(0).taskName);
        }
    }

    @Test
    void reapRemovesDeadOwnedSubscriptionsOnlyAfterTheRegistrationGrace() {
        try (InMemoryQueueBackend backend = new InMemoryQueueBackend()) {
            backend.registerSubscription("orders", "ghost", EMAIL.name(), "default", false, "gone-worker");
            backend.registerSubscription("orders", "durable", EMAIL.name(), "default", true, null);

            // A just-registered dead-owned row is inside the grace window: the
            // owner's liveness may simply not be visible yet, so nothing is reaped.
            assertEquals(0, backend.reapEphemeralSubscriptions());
            assertTrue(backend.listSubscriptionsJson("orders").contains("ghost"));

            backend.backdateSubscription("orders", "ghost", 61_000);
            assertEquals(1, backend.reapEphemeralSubscriptions());
            assertEquals(0, backend.reapEphemeralSubscriptions());
            assertTrue(backend.listSubscriptionsJson("orders").contains("durable"));
            assertFalse(backend.listSubscriptionsJson("orders").contains("ghost"));
        }
    }

    @Test
    void redeclareDoesNotRefreshTheReapGraceWindow() {
        try (InMemoryQueueBackend backend = new InMemoryQueueBackend()) {
            backend.registerSubscription("orders", "ghost", EMAIL.name(), "default", false, "gone-worker");
            backend.backdateSubscription("orders", "ghost", 61_000);

            // The upsert preserves creation time, so re-declaring an aged
            // dead-owned row leaves it eligible for the very next reap.
            backend.registerSubscription("orders", "ghost", AUDIT.name(), "default", false, "gone-worker");
            assertEquals(1, backend.reapEphemeralSubscriptions());
        }
    }

    @Test
    void ephemeralRegistrationWithoutAnOwnerIsRejected() {
        try (InMemoryQueueBackend backend = new InMemoryQueueBackend()) {
            FlexiQException rejected = assertThrows(
                    FlexiQException.class,
                    () -> backend.registerSubscription("orders", "sink", EMAIL.name(), "default", false, null));
            assertTrue(rejected.getMessage().contains("requires ownerWorkerId"));
        }
    }

    @Test
    @Timeout(20)
    void publishExpiryCancelsDeliveriesThatOutliveIt() throws Exception {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            List<Job> deliveries = queue.publish(
                    "orders", "o-1", PublishOptions.builder().expiresMs(1L).build());
            assertEquals(1, deliveries.size());
            Thread.sleep(15);

            Worker worker = queue.worker().handle(EMAIL, payload -> payload).start();
            try (worker) {
                Job done = queue.awaitJob(deliveries.get(0).id, Duration.ofSeconds(10))
                        .orElseThrow();
                assertEquals(JobStatus.CANCELLED, done.status);
                assertEquals("expired before execution", done.error);
            }
        }
    }

    @Test
    @Timeout(20)
    void publishResultTtlExpiresStoredResults() throws Exception {
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.subscribe("orders", EMAIL);
            List<Job> deliveries = queue.publish(
                    "orders", "o-1", PublishOptions.builder().resultTtlMs(1L).build());
            assertEquals(1, deliveries.size());

            Worker worker = queue.worker().handle(EMAIL, payload -> payload).start();
            try (worker) {
                Job done = queue.awaitJob(deliveries.get(0).id, Duration.ofSeconds(10))
                        .orElseThrow();
                assertEquals(JobStatus.COMPLETE, done.status);
                Thread.sleep(15);
                assertTrue(queue.getResult(done.id, String.class).isEmpty());
            }
        }
    }
}
