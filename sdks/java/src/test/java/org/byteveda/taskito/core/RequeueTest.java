package org.byteveda.taskito.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.model.JobStatus;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class RequeueTest {

    private static final Task<Integer> TASK = Task.of("requeue.task", Integer.class);

    @Test
    @Timeout(30)
    void requeuesStuckRunningJob(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rq.db").toString()).open()) {
            CountDownLatch running = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            String id = queue.enqueue(TASK, 1);
            Worker worker = queue.worker()
                    .handle(TASK, payload -> {
                        running.countDown();
                        release.await();
                        return payload;
                    })
                    .start();
            try (worker) {
                assertTrue(running.await(20, TimeUnit.SECONDS), "handler did not start");
                // Pause first, so the poller can't re-claim the job the moment the
                // requeue makes it pending again and flip the status under the assert.
                queue.queue("default").pause();

                assertTrue(queue.requeueJob(id), "a running job is requeued");
                assertEquals(JobStatus.PENDING, queue.getJob(id).orElseThrow().status);
                assertFalse(queue.requeueJob(id), "a job that is no longer running is not requeued");

                release.countDown();
            }
        }
    }

    @Test
    void requeueOfMissingJobReportsFalse(@TempDir Path dir) {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rqm.db").toString()).open()) {
            assertFalse(queue.requeueJob("no-such-job"));
        }
    }

    @Test
    void requeueOfPendingJobReportsFalse(@TempDir Path dir) {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("rqp.db").toString()).open()) {
            assertFalse(queue.requeueJob(queue.enqueue(TASK, 1)));
        }
    }
}
