package org.byteveda.flexiq.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.QueueFullException;
import org.byteveda.flexiq.errors.TaskError;
import org.byteveda.flexiq.errors.TaskErrors;
import org.byteveda.flexiq.model.DeadJob;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class InMemoryQueueBackendTest {

    @Test
    @Timeout(20)
    void runsJobToCompletion() throws Exception {
        Task<Integer> dbl = Task.of("im.double", Integer.class);
        try (FlexiQ queue = InMemoryFlexiQ.open()) { // no JNI, no disk
            String id = queue.enqueue(dbl, 21);
            Worker worker = queue.worker().handle(dbl, p -> p * 2).start();
            try (worker) {
                Job job = queue.awaitJob(id, Duration.ofSeconds(10)).orElseThrow();
                assertEquals(JobStatus.COMPLETE, job.status);
                assertEquals(42, queue.getResult(id, Integer.class).orElseThrow());
            }
        }
    }

    /** Payload with a field a debounce key template can address. */
    record Report(String userId, int revision) {}

    @Test
    @Timeout(20)
    void debouncedEnqueuesCollapseOntoOneJob() {
        // The fake honours the window too, so a test written against it sees the same
        // one-job-per-key outcome the native backend gives.
        Task<Report> report = Task.of("im.debounce", Report.class)
                .debounce(Duration.ofMinutes(5), "report:{userId}", Duration.ofMinutes(30));
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            String first = queue.enqueue(report, new Report("u1", 1));
            String second = queue.enqueue(report, new Report("u1", 2));
            String other = queue.enqueue(report, new Report("u2", 1));

            assertEquals(first, second, "a repeat enqueue lands on the open window");
            assertNotEquals(first, other, "a different key opens its own window");
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(20)
    void aFullQueueTakesADebouncedSlideButNotANewWindow() {
        // The cap rides down on a debounced enqueue because only the write knows whether it
        // inserts a row; the fake applies it on the same branch the core does, so a test
        // written against it never sees a rejection the native backend would not give.
        Task<Report> report = Task.of("im.debounce.cap", Report.class)
                .debounce(Duration.ofMinutes(5), "report:{userId}", Duration.ofMinutes(30));
        Task<String> noop = Task.of("im.debounce.cap.noop", String.class);
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            queue.maxPending("default", 2);
            String opened = queue.enqueue(report, new Report("u1", 1));
            queue.enqueue(noop, "filler");
            assertEquals(2, queue.countPendingByQueue("default"));

            assertEquals(opened, queue.enqueue(report, new Report("u1", 2)), "a full queue still slides");

            QueueFullException full =
                    assertThrows(QueueFullException.class, () -> queue.enqueue(report, new Report("u2", 1)));
            assertEquals("default", full.queue());
            assertEquals(2, full.pending());
            assertEquals(2, full.cap());
            assertEquals(2, queue.countPendingByQueue("default"));
        }
    }

    @Test
    @Timeout(20)
    void debounceWindowsAreScopedByNamespace() {
        // The core's find_debounce_target takes the namespace, so the same key under two
        // namespaces is two windows. The fake has to agree or it hides a real collision.
        Task<Report> report = Task.of("im.debounce.ns", Report.class)
                .debounce(Duration.ofMinutes(5), "report:{userId}", Duration.ofMinutes(30));
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            String tenantA = queue.enqueue(
                    report,
                    new Report("u1", 1),
                    report.options().toBuilder().namespace("a").build());
            String tenantB = queue.enqueue(
                    report,
                    new Report("u1", 1),
                    report.options().toBuilder().namespace("b").build());

            assertNotEquals(tenantA, tenantB, "one key in two namespaces is two windows");
        }
    }

    @Test
    @Timeout(20)
    void samePriorityJobsRunInEnqueueOrder() throws Exception {
        Task<Integer> order = Task.of("im.order", Integer.class);
        List<Integer> seen = Collections.synchronizedList(new ArrayList<>());
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            String last = null;
            for (int i = 0; i < 5; i++) {
                last = queue.enqueue(order, i);
            }
            // Single-threaded worker so claim order is observable as run order.
            Worker worker = queue.worker()
                    .concurrency(1)
                    .handle(order, p -> {
                        seen.add(p);
                        return p;
                    })
                    .start();
            try (worker) {
                queue.awaitJob(last, Duration.ofSeconds(10)).orElseThrow();
                // Production dequeues FIFO within a priority tier; the in-memory
                // backend must match, not follow hash-map iteration order.
                assertEquals(List.of(0, 1, 2, 3, 4), seen);
            }
        }
    }

    @Test
    @Timeout(20)
    void requeuedJobIgnoresTheStaleAttemptsOutcome() throws Exception {
        Task<Integer> stuck = Task.of("im.stuck", Integer.class);
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            String id = queue.enqueue(stuck, 1);
            Worker worker = queue.worker()
                    .handle(stuck, p -> {
                        running.countDown();
                        release.await();
                        return p;
                    })
                    .start();
            try (worker) {
                assertTrue(running.await(10, TimeUnit.SECONDS), "handler did not start");
                queue.queue("default").pause(); // don't let the worker re-claim it
                assertTrue(queue.requeueJob(id));

                release.countDown();
                // The old attempt finishes against a job it no longer owns. Like the
                // core — whose complete() filters on Running — its result is dropped.
                Thread.sleep(200);
                Job job = queue.getJob(id).orElseThrow();
                assertEquals(JobStatus.PENDING, job.status);
                assertTrue(queue.getResult(id).isEmpty(), "a stale attempt must not publish a result");
            }
        }
    }

    @Test
    @Timeout(20)
    void retriesThenDeadLetters() throws Exception {
        Task<Integer> boom = Task.of("im.boom", Integer.class).retries(2);
        try (FlexiQ queue = InMemoryFlexiQ.open()) {
            String id = queue.enqueue(boom, 1);
            Worker worker = queue.worker()
                    .handle(boom, p -> {
                        throw new IllegalStateException("boom");
                    })
                    .start();
            try (worker) {
                Job job = queue.awaitJob(id, Duration.ofSeconds(10)).orElseThrow();
                assertEquals(JobStatus.DEAD, job.status);
                assertEquals(2, job.retryCount);
                List<DeadJob> dead = queue.listDead(10, 0);
                assertTrue(dead.size() >= 1);
                // The bridge stores the canonical structured error; parity with the JNI path.
                TaskError error = TaskErrors.decode(dead.get(0).error);
                assertEquals(IllegalStateException.class.getName(), error.errtype);
                assertEquals("boom", error.message);
                assertFalse(error.traceback.isEmpty());
            }
        }
    }
}
