package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.Queue;
import org.byteveda.flexiq.model.DispatchOrder;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobFilter;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.model.TaskLog;
import org.byteveda.flexiq.model.TaskLogLevel;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueueTest {

    private static final Task<Map<String, String>> SEND_EMAIL = Task.of("send_email", mapType());

    @SuppressWarnings("unchecked")
    private static Class<Map<String, String>> mapType() {
        return (Class<Map<String, String>>) (Class<?>) Map.class;
    }

    private FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open();
    }

    @Test
    void backendNameIsCaseInsensitive() {
        // A mixed-case backend must still be recognized by the native layer.
        try (FlexiQ client = FlexiQ.builder().backend("SQLite").url(":memory:").open()) {
            assertDoesNotThrow(client::stats);
        }
    }

    @Test
    void enqueueGetCancelStats(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            String id = queue.enqueue(
                    SEND_EMAIL,
                    Collections.singletonMap("to", "a@b.c"),
                    EnqueueOptions.builder().priority(5).queue("emails").build());
            assertFalse(id.isEmpty());

            Optional<Job> job = queue.getJob(id);
            assertTrue(job.isPresent());
            assertEquals("send_email", job.get().taskName);
            assertEquals(JobStatus.PENDING, job.get().status);
            assertEquals(5, job.get().priority);
            assertEquals("emails", job.get().queue);

            assertEquals(1, queue.stats().pending);

            assertTrue(queue.cancel(id));
            assertEquals(0, queue.stats().pending);
        }
    }

    @Test
    void getMissingJobIsEmpty(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            assertTrue(queue.getJob("nope").isEmpty());
        }
    }

    @Test
    void enqueueManyAndFilteredList(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            List<String> ids = queue.enqueueMany(
                    SEND_EMAIL.withOptions(
                            EnqueueOptions.builder().queue("emails").build()),
                    Arrays.asList(
                            Collections.singletonMap("to", "a"),
                            Collections.singletonMap("to", "b"),
                            Collections.singletonMap("to", "c")));
            assertEquals(3, ids.size());
            assertEquals(3, queue.statsByQueue("emails").pending);

            List<Job> jobs = queue.listJobs(JobFilter.builder()
                    .queue("emails")
                    .status(JobStatus.PENDING)
                    .build());
            assertEquals(3, jobs.size());
        }
    }

    @Test
    void enqueueManyDedupesActiveUniqueKeys(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            String first = queue.enqueue(
                    SEND_EMAIL,
                    Collections.singletonMap("to", "a"),
                    EnqueueOptions.builder().uniqueKey("k1").build());
            // The batch's duplicate key must resolve to the existing job instead
            // of rolling back the whole batch on the unique index.
            List<String> ids = queue.enqueueMany(
                    SEND_EMAIL,
                    Arrays.asList(Collections.singletonMap("to", "a"), Collections.singletonMap("to", "b")),
                    EnqueueOptions.builder().uniqueKey("k1").build());
            assertEquals(2, ids.size());
            assertEquals(first, ids.get(0));
            assertEquals(first, ids.get(1));
            assertEquals(1, queue.stats().pending);
        }
    }

    @Test
    void callsAfterCloseThrowInsteadOfTouchingFreedHandle(@TempDir Path dir) {
        FlexiQ queue = open(dir);
        queue.close();
        assertThrows(IllegalStateException.class, queue::stats);
    }

    @Test
    void settingsRoundTrip(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            queue.setSetting("theme", "dark");
            assertEquals("dark", queue.getSetting("theme").orElse(""));
            assertEquals("dark", queue.listSettings().get("theme"));
            assertTrue(queue.deleteSetting("theme"));
            assertTrue(queue.getSetting("theme").isEmpty());
        }
    }

    @Test
    void pauseAndResumeQueue(@TempDir Path dir) {
        try (FlexiQ client = open(dir)) {
            Queue emails = client.queue("emails");
            emails.pause();
            assertTrue(emails.isPaused());
            assertTrue(client.listPausedQueues().contains("emails"));
            emails.resume();
            assertFalse(emails.isPaused());
            assertFalse(client.listPausedQueues().contains("emails"));
        }
    }

    @Test
    void taskLogsRoundTrip(@TempDir Path dir) {
        try (FlexiQ queue = open(dir)) {
            String id = queue.enqueue("send_email", Collections.singletonMap("to", "a"));
            queue.writeTaskLog(id, "send_email", TaskLogLevel.INFO, "hello");
            List<TaskLog> logs = queue.getTaskLogs(id);
            assertEquals(1, logs.size());
            assertEquals("hello", logs.get(0).message);
        }
    }

    @Test
    void logFiltersTakeAWireStringAndStayLenient(@TempDir Path dir) {
        // The filters take a wire string rather than an enum so that `null` (no filter)
        // still compiles — an enum/String overload pair is ambiguous on a bare null.
        // An unrecognized level filters to nothing, since it usually arrives from a
        // dashboard query string.
        try (FlexiQ queue = open(dir)) {
            String id = queue.enqueue("send_email", Collections.singletonMap("to", "a"));
            queue.writeTaskLog(id, "send_email", TaskLogLevel.INFO, "hello");

            assertEquals(1, queue.queryTaskLogs(null, null, 0, 10).size());
            assertEquals(
                    1,
                    queue.queryTaskLogs(null, TaskLogLevel.INFO.wire(), 0, 10).size());
            assertTrue(queue.queryTaskLogs(null, "verbose", 0, 10).isEmpty());
        }
    }

    @Test
    void typedOverloadsRejectANullEnum(@TempDir Path dir) {
        // Without the guard these dereference the enum and surface an incidental NPE.
        try (FlexiQ queue = open(dir)) {
            assertThrows(IllegalArgumentException.class, () -> queue.dispatchOrder("default", (DispatchOrder) null));
            assertThrows(IllegalArgumentException.class, () -> queue.writeTaskLog("j", "t", (TaskLogLevel) null, "m"));
        }
    }

    @Test
    void unknownWorkflowStateFilterIsRejectedByTheCore(@TempDir Path dir) {
        // Unlike the log level, the state filter is validated in the core, so an
        // unrecognized value raises instead of filtering to nothing.
        try (FlexiQ queue = open(dir)) {
            assertEquals(0, queue.listWorkflowRuns(null, null, 10, 0).size());
            assertThrows(FlexiQException.class, () -> queue.listWorkflowRuns(null, "nonsense", 10, 0));
        }
    }
}
