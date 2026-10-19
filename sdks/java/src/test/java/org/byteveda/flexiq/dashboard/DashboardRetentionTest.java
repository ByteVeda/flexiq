package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.EffectiveRetention;
import org.byteveda.flexiq.model.RetentionOverride;
import org.byteveda.flexiq.model.RetentionPreview;
import org.byteveda.flexiq.model.RetentionSource;
import org.byteveda.flexiq.worker.Retention;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** The retention echo: the windows the elected cleaner publishes for this namespace. */
@Timeout(30)
class DashboardRetentionTest {

    private static final long DAY_MS = 86_400_000L;

    /** The document the elected cleaner publishes — see {@code BINDING_CONTRACT.md}. */
    private static final String PUBLISHED_KEY = "retention:effective:default";

    private static final String PUBLISHED = "{\"enabled\":true,\"defaulted\":true,\"namespace\":\"default\","
            + "\"reported_at\":1753200000000,\"windows\":{\"archived_jobs_ttl_ms\":604800000,"
            + "\"dead_letter_ttl_ms\":2592000000,\"task_logs_ttl_ms\":259200000,"
            + "\"task_metrics_ttl_ms\":604800000,\"job_errors_ttl_ms\":null}}";

    private static FlexiQ queue(Path dir) {
        return FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
    }

    @Test
    void effectiveRetentionIsEmptyUntilACleanerPublishes(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            // Unreported is not "off": no worker has swept, so nothing is known yet.
            assertTrue(queue.effectiveRetention().isEmpty());
        }
    }

    @Test
    void effectiveRetentionParsesThePublishedDocument(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            queue.setSetting(PUBLISHED_KEY, PUBLISHED);

            EffectiveRetention snapshot = queue.effectiveRetention().orElseThrow();
            assertTrue(snapshot.enabled);
            assertTrue(snapshot.defaulted);
            assertEquals("default", snapshot.namespace);
            assertEquals(1753200000000L, snapshot.reportedAt);
            assertEquals(3 * DAY_MS, snapshot.windows.taskLogsMs);
            assertEquals(30 * DAY_MS, snapshot.windows.deadLetterMs);
            // A table with no window is kept forever, not purged.
            assertEquals(null, snapshot.windows.jobErrorsMs);
        }
    }

    @Test
    void retentionEndpointReportsNothingBeforeASweep(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = queue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));

            String body = client.get("/api/retention").body();
            assertTrue(body.contains("\"reported\":false"), body);
            assertTrue(body.contains("\"enabled\":false"), body);
            assertTrue(body.contains("\"namespace\":null"), body);
            assertTrue(body.contains("\"task_logs_ttl_ms\":null"), body);
        }
    }

    @Test
    void retentionEndpointEchoesThePublishedWindows(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = queue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            queue.setSetting(PUBLISHED_KEY, PUBLISHED);

            String body = client.get("/api/retention").body();
            assertTrue(body.contains("\"reported\":true"), body);
            assertTrue(body.contains("\"defaulted\":true"), body);
            assertTrue(body.contains("\"reported_at\":1753200000000"), body);
            assertTrue(body.contains("\"task_logs_ttl_ms\":259200000"), body);
            assertTrue(body.contains("\"dead_letter_ttl_ms\":2592000000"), body);
        }
    }

    @Test
    void dryRunOnAnEmptyQueueCountsNothing(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            // Computed in-process, so it answers without a worker sweep. Empty
            // queue → every count is zero, but the defaults are still on.
            RetentionPreview preview = queue.dryRunRetention();
            assertTrue(preview.enabled);
            assertTrue(preview.defaulted);
            assertEquals("default", preview.namespace);
            assertTrue(preview.referenceTime > 0);
            assertEquals(0, preview.total);
            assertEquals(0, preview.counts.archivedJobs);
            assertEquals(0, preview.counts.deadLetter);
        }
    }

    @Test
    void dryRunFollowsTheReportedPolicy(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            // Retention config lives in the worker here, so the no-candidate
            // preview must follow the reported policy, not assume the defaults.
            queue.setSetting(PUBLISHED_KEY, PUBLISHED);

            RetentionPreview preview = queue.dryRunRetention();
            assertTrue(preview.defaulted, "the published document says defaulted");
            assertEquals(3 * DAY_MS, preview.windows.taskLogsMs);
            // The published document leaves job_errors unset — kept forever.
            assertEquals(null, preview.windows.jobErrorsMs);
        }
    }

    @Test
    void dryRunPreviewsCandidateWindows(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            // Size a window before setting it: pass candidate windows, no worker
            // reconfiguration.
            RetentionPreview preview =
                    queue.dryRunRetention(Retention.builder().archivedJobs(0).build());
            assertTrue(preview.enabled);
            assertFalse(preview.defaulted);
            assertEquals(0L, preview.windows.archivedJobsMs);
            // Unset candidate windows keep forever.
            assertEquals(null, preview.windows.deadLetterMs);
        }
    }

    @Test
    void retentionDryRunEndpointReportsCounts(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = queue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));

            String body = client.get("/api/retention/dry-run").body();
            assertTrue(body.contains("\"enabled\":true"), body);
            assertTrue(body.contains("\"defaulted\":true"), body);
            assertTrue(body.contains("\"total\":0"), body);
            assertTrue(body.contains("\"archived_jobs\":0"), body);
            assertTrue(body.contains("\"task_logs_ttl_ms\":259200000"), body);
        }
    }

    @Test
    void retentionDryRunEndpointEchoesTheReportedWindows(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = queue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            queue.setSetting(PUBLISHED_KEY, PUBLISHED);

            String body = client.get("/api/retention/dry-run").body();
            assertTrue(body.contains("\"task_logs_ttl_ms\":259200000"), body);
            // The published document leaves job_errors unset — kept forever.
            assertTrue(body.contains("\"job_errors_ttl_ms\":null"), body);
        }
    }

    @Test
    void publishedWindowsAreNotAnEditableSetting(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = queue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            queue.setSetting(PUBLISHED_KEY, PUBLISHED);

            // A report of what the worker does, not a knob: never listed as an
            // editable row, never spoofable through the generic KV endpoints.
            assertFalse(client.get("/api/settings").body().contains(PUBLISHED_KEY));
            assertEquals(404, client.get("/api/settings/" + PUBLISHED_KEY).statusCode());
            assertEquals(
                    400,
                    client.put("/api/settings/" + PUBLISHED_KEY, "{\"value\":\"{}\"}")
                            .statusCode());
        }
    }

    @Test
    void anOlderPublishedDocumentReadsAsCode(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            queue.setSetting(PUBLISHED_KEY, PUBLISHED);

            EffectiveRetention snapshot = queue.effectiveRetention().orElseThrow();
            assertEquals(RetentionSource.CODE, snapshot.source);
            assertEquals(null, snapshot.codeWindows);
        }
    }

    @Test
    void theOverrideToggleRoundTripsAndSteersThePreview(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            assertEquals(RetentionSource.CODE, queue.retentionOverride().source);
            assertEquals(null, queue.retentionOverride().updatedAt);

            EffectiveRetention.Windows windows = new EffectiveRetention.Windows(null, null, DAY_MS, null, null);
            RetentionOverride saved = queue.setRetentionOverride(RetentionSource.DASHBOARD, windows);
            assertEquals(RetentionSource.DASHBOARD, saved.source);
            assertTrue(saved.updatedAt != null && saved.updatedAt > 0);
            assertEquals(DAY_MS, queue.dryRunRetention().windows.taskLogsMs);

            // Back to code: the preview drops the windows, the override keeps them.
            queue.setRetentionOverride(RetentionSource.CODE, windows);
            assertEquals(3 * DAY_MS, queue.dryRunRetention().windows.taskLogsMs);
            assertEquals(DAY_MS, queue.retentionOverride().windows.taskLogsMs);

            assertTrue(queue.clearRetentionOverride());
            assertEquals(null, queue.retentionOverride().updatedAt);
        }
    }

    @Test
    void aWindowUnderAnHourIsRefusedBeforeWriting(@TempDir Path dir) {
        try (FlexiQ queue = queue(dir)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> queue.setRetentionOverride(
                            RetentionSource.DASHBOARD, new EffectiveRetention.Windows(null, null, 0L, null, null)));
            assertEquals(null, queue.retentionOverride().updatedAt);
        }
    }

    @Test
    void overrideEndpointsSaveAndRefuse(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = queue(dir);
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));

            String unsaved = client.get("/api/retention/override").body();
            assertTrue(unsaved.contains("\"source\":\"code\""), unsaved);
            assertTrue(unsaved.contains("\"updated_at\":null"), unsaved);

            HttpResponse<String> saved = client.put(
                    "/api/retention/override",
                    "{\"source\":\"dashboard\",\"windows\":{\"task_logs_ttl_ms\":86400000}}");
            assertEquals(200, saved.statusCode(), saved.body());
            assertTrue(saved.body().contains("\"task_logs_ttl_ms\":86400000"), saved.body());

            for (String bad : new String[] {
                "{\"source\":\"dashboard\",\"windows\":{\"task_logs_ttl_ms\":0}}",
                "{\"source\":\"dashboard\",\"windows\":{\"task_log_ttl_ms\":86400000}}",
                "{\"source\":\"always\"}",
            }) {
                assertEquals(400, client.put("/api/retention/override", bad).statusCode(), bad);
            }

            String deleted = client.delete("/api/retention/override").body();
            assertTrue(deleted.contains("\"deleted\":true"), deleted);
        }
    }
}
