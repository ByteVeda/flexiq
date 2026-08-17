package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobFilter;
import org.byteveda.flexiq.model.Page;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Keyset pagination walks a listing exactly once and terminates. */
class PaginationTest {

    /** Walk every page, collecting ids, so a dropped or repeated row shows up. */
    private static List<String> walk(Function<String, Page<Job>> fetch) {
        List<String> ids = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            Page<Job> page = fetch.apply(cursor);
            page.items.forEach(job -> ids.add(job.id));
            cursor = page.nextCursor;
            if (++pages > 20) {
                throw new IllegalStateException("pagination did not terminate");
            }
        } while (cursor != null);
        return ids;
    }

    @Test
    @Timeout(30)
    void walksEveryJobExactlyOnce(@TempDir Path dir) {
        Task<String> paged = Task.of("paged", String.class);

        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            Set<String> enqueued = new HashSet<>();
            for (int i = 0; i < 5; i++) {
                enqueued.add(queue.enqueue(paged, String.valueOf(i)));
            }

            List<String> seen = walk(
                    cursor -> queue.listJobsAfter(JobFilter.builder().limit(2).build(), cursor));

            assertEquals(5, seen.size(), "every job appears exactly once");
            assertEquals(enqueued, new HashSet<>(seen), "the walk covers exactly what was enqueued");
        }
    }

    @Test
    @Timeout(30)
    void aShortPageEndsTheWalk(@TempDir Path dir) {
        Task<String> paged = Task.of("paged", String.class);

        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            queue.enqueue(paged, "1");

            Page<Job> page = queue.listJobsAfter(JobFilter.builder().limit(10).build(), null);
            assertEquals(1, page.items.size());
            assertNull(page.nextCursor, "a page shorter than the limit is the last one");
        }
    }

    @Test
    @Timeout(30)
    void malformedCursorIsRejected(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("t.db").toString())
                .open()) {
            RuntimeException error = assertThrows(
                    RuntimeException.class,
                    () -> queue.listJobsAfter(JobFilter.builder().limit(2).build(), "nocolon"));
            assertTrue(
                    error.getMessage().contains("cursor"),
                    "a bad cursor must be rejected, not silently restarted: " + error.getMessage());
        }
    }
}
