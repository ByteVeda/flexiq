package org.byteveda.taskito.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.TaskError;
import org.byteveda.taskito.errors.TaskErrors;
import org.byteveda.taskito.model.DeadJob;
import org.byteveda.taskito.model.Job;
import org.byteveda.taskito.model.JobStatus;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class InMemoryQueueBackendTest {

    @Test
    @Timeout(20)
    void runsJobToCompletion() throws Exception {
        Task<Integer> dbl = Task.of("im.double", Integer.class);
        try (Taskito queue = InMemoryTaskito.open()) { // no JNI, no disk
            String id = queue.enqueue(dbl, 21);
            Worker worker = queue.worker().handle(dbl, p -> p * 2).start();
            try (worker) {
                Job job = queue.awaitJob(id, Duration.ofSeconds(10)).orElseThrow();
                assertEquals(JobStatus.COMPLETE, job.status);
                assertEquals(42, queue.getResult(id, Integer.class).orElseThrow());
            }
        }
    }

    @Test
    @Timeout(20)
    void samePriorityJobsRunInEnqueueOrder() throws Exception {
        Task<Integer> order = Task.of("im.order", Integer.class);
        List<Integer> seen = Collections.synchronizedList(new ArrayList<>());
        try (Taskito queue = InMemoryTaskito.open()) {
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
        try (Taskito queue = InMemoryTaskito.open()) {
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
        try (Taskito queue = InMemoryTaskito.open()) {
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
