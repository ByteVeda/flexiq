package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code enqueuedBy} names the token a server door authenticated. An in-process
 * enqueue presents no token, so nothing may claim a submitter for it.
 */
class EnqueuedByTest {

    @Test
    @Timeout(30)
    void anInProcessJobHasNoSubmitter(@TempDir Path dir) {
        Task<String> work = Task.of("work", String.class);
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            String id = queue.enqueue(work, "payload");
            assertNull(queue.getJob(id).orElseThrow().enqueuedBy);
        }
    }

    @Test
    void theCoreViewDecodesTheSubmitter() throws Exception {
        Job job = new ObjectMapper()
                .readValue("{\"id\":\"j1\",\"status\":\"pending\",\"enqueuedBy\":\"8a7fbf03e21cfa60\"}", Job.class);
        assertEquals("8a7fbf03e21cfa60", job.enqueuedBy);
    }
}
