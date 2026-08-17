package org.byteveda.flexiq.webhooks;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.FlexiQ;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the cross-SDK storage layout for webhook subscriptions: one JSON array
 * under {@code webhooks:subscriptions}, snake_case fields, timeout in seconds.
 * A shell that drifts from this sees only its own hooks on a shared queue.
 */
@Timeout(30)
class WebhookStoreContractTest {
    private static final String KEY = "webhooks:subscriptions";
    private static final String LEGACY_KEY = "flexiq.webhooks";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static FlexiQ open(Path dir) {
        return FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
    }

    private static List<Map<String, Object>> rows(FlexiQ queue) throws Exception {
        Optional<String> raw = queue.getSetting(KEY);
        assertThat(raw).isPresent();
        return JSON.readValue(raw.get(), new TypeReference<List<Map<String, Object>>>() {});
    }

    @Test
    void writesTheCanonicalKeyAndFieldShape(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            WebhookManager manager = WebhookManager.forQueue(queue);
            manager.create(Webhook.builder("https://example.test/hook")
                    .taskFilters("send", "resize")
                    .timeoutMs(15_000)
                    .retryBackoff(3.0)
                    .maxRetries(5));

            List<Map<String, Object>> rows = rows(queue);
            assertThat(rows).hasSize(1);
            Map<String, Object> row = rows.get(0);
            assertThat(row).containsKeys("id", "url", "events", "task_filter", "headers", "secret");
            assertThat(row.get("task_filter")).isEqualTo(List.of("send", "resize"));
            assertThat(row.get("max_retries")).isEqualTo(5);
            // Seconds on the wire, milliseconds in the runtime shape.
            assertThat(((Number) row.get("timeout_seconds")).doubleValue()).isEqualTo(15.0);
            assertThat(((Number) row.get("retry_backoff")).doubleValue()).isEqualTo(3.0);
            assertThat(row).doesNotContainKeys("timeoutMs", "taskFilter", "maxRetries", "retryBackoff");
        }
    }

    @Test
    void readsARowWrittenByAnotherShell(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            queue.setSetting(
                    KEY,
                    "[{\"id\":\"abc\",\"url\":\"https://other.test/hook\",\"events\":[\"job.completed\"],"
                            + "\"task_filter\":[\"send\"],\"headers\":{\"X-Key\":\"v\"},\"secret\":null,"
                            + "\"max_retries\":7,\"timeout_seconds\":2.5,\"retry_backoff\":1.5,"
                            + "\"enabled\":true,\"description\":\"from elsewhere\","
                            + "\"created_at\":111,\"updated_at\":222}]");

            List<Webhook> hooks = WebhookManager.forQueue(queue).list();
            assertThat(hooks).hasSize(1);
            Webhook hook = hooks.get(0);
            assertThat(hook.id).isEqualTo("abc");
            assertThat(hook.events).containsExactly("job.completed");
            assertThat(hook.taskFilters).containsExactly("send");
            assertThat(hook.headers).containsEntry("X-Key", "v");
            assertThat(hook.maxRetries).isEqualTo(7);
            assertThat(hook.timeoutMs).isEqualTo(2_500);
            assertThat(hook.retryBackoff).isEqualTo(1.5);
            assertThat(hook.description).isEqualTo("from elsewhere");
            assertThat(hook.createdAt).isEqualTo(111);
        }
    }

    @Test
    void carriesUnmodelledFieldsThroughARewrite(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            queue.setSetting(
                    KEY,
                    "[{\"id\":\"abc\",\"url\":\"https://other.test/hook\",\"events\":[],"
                            + "\"timeout_seconds\":10,\"future_field\":{\"kept\":true}}]");

            // Creating a second hook rewrites the whole array — the field this
            // shell does not model must survive that rewrite.
            WebhookManager.forQueue(queue).create(Webhook.builder("https://example.test/second"));

            List<Map<String, Object>> rows = rows(queue);
            assertThat(rows).hasSize(2);
            Map<String, Object> original = rows.stream()
                    .filter(row -> "abc".equals(row.get("id")))
                    .findFirst()
                    .orElseThrow();
            assertThat(original.get("future_field")).isEqualTo(Map.of("kept", true));
        }
    }

    @Test
    void foldsHooksWrittenUnderTheLegacyKey(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            queue.setSetting(
                    LEGACY_KEY,
                    "[{\"id\":\"old\",\"url\":\"https://legacy.test/hook\",\"events\":[\"job.completed\"],"
                            + "\"taskFilter\":\"send\",\"maxRetries\":4,\"timeoutMs\":5000,"
                            + "\"enabled\":true,\"createdAt\":10,\"updatedAt\":20}]");

            List<Webhook> hooks = WebhookManager.forQueue(queue).list();
            assertThat(hooks).hasSize(1);
            assertThat(hooks.get(0).id).isEqualTo("old");
            assertThat(hooks.get(0).taskFilters).containsExactly("send");
            assertThat(hooks.get(0).timeoutMs).isEqualTo(5_000);

            // The hook now lives under the shared key, and the old one is gone so
            // the fold cannot run twice.
            assertThat(rows(queue)).hasSize(1);
            assertThat(queue.getSetting(LEGACY_KEY)).isEmpty();
        }
    }

    @Test
    void theCanonicalRowWinsOnAnIdCollision(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            queue.setSetting(
                    KEY,
                    "[{\"id\":\"dup\",\"url\":\"https://canonical.test/hook\",\"events\":[],"
                            + "\"timeout_seconds\":10}]");
            queue.setSetting(
                    LEGACY_KEY,
                    "[{\"id\":\"dup\",\"url\":\"https://legacy.test/hook\",\"events\":[],\"timeoutMs\":5000}]");

            List<Webhook> hooks = WebhookManager.forQueue(queue).list();
            assertThat(hooks).hasSize(1);
            assertThat(hooks.get(0).url).isEqualTo("https://canonical.test/hook");
        }
    }
}
