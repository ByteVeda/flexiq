package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class DependsOnTest {

    @Test
    @Timeout(30)
    void dependentWaitsForDependency(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("dep.db").toString()).open()) {
            Task<String> a = Task.of("dep.a", String.class);
            Task<String> b = Task.of("dep.b", String.class);
            AtomicBoolean aDone = new AtomicBoolean(false);
            AtomicBoolean bSawADone = new AtomicBoolean(false);
            CountDownLatch bRan = new CountDownLatch(1);

            String idA = queue.enqueue(a, "a");
            queue.enqueue(b, "b", EnqueueOptions.builder().dependsOn(idA).build());

            Worker worker = queue.worker()
                    .handle(a, p -> {
                        aDone.set(true);
                        return p;
                    })
                    .handle(b, p -> {
                        bSawADone.set(aDone.get());
                        bRan.countDown();
                        return p;
                    })
                    .start();
            try (worker) {
                assertTrue(bRan.await(20, TimeUnit.SECONDS), "dependent should run once its dependency completes");
                assertTrue(bSawADone.get(), "the dependency must complete before the dependent runs");
            }
        }
    }

    @Test
    @Timeout(30)
    void cascadeCancelsDependentWhenDependencyFails(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("dep_cascade.db").toString()).open()) {
            Task<String> a = Task.of("dep.cascade.a", String.class).maxRetries(0);
            Task<String> b = Task.of("dep.cascade.b", String.class);
            AtomicBoolean bRan = new AtomicBoolean(false);

            String idA = queue.enqueue(a, "boom");
            String idB = queue.enqueue(
                    b, "b", EnqueueOptions.builder().dependsOn(idA).build());

            Worker worker = queue.worker()
                    .handle(a, p -> {
                        throw new RuntimeException("boom");
                    })
                    .handle(b, p -> {
                        bRan.set(true);
                        return p;
                    })
                    .start();
            try (worker) {
                Job jobB = queue.awaitJob(idB, Duration.ofSeconds(20)).orElseThrow();
                assertEquals(JobStatus.CANCELLED, jobB.status, "a failed dependency cascade-cancels the dependent");
                assertFalse(bRan.get(), "the dependent must never run");
            }
        }
    }
}
