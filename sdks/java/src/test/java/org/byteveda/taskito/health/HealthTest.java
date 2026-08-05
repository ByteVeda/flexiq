package org.byteveda.taskito.health;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.byteveda.taskito.Taskito;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class HealthTest {

    private static Taskito open(Path dir) {
        return Taskito.builder().sqlite(dir.resolve("t.db").toString()).open();
    }

    @Test
    void livenessIsAlwaysOk() {
        assertThat(Health.check().status()).isEqualTo("ok");
        assertThat(Health.check().toMap()).containsEntry("status", "ok");
    }

    @Test
    void readyWhenStorageAnswersAndNothingIsUnhealthy(@TempDir Path dir) {
        try (Taskito queue = open(dir)) {
            ReadinessReport report = Health.readiness(queue);
            assertThat(report.ready()).isTrue();
            assertThat(report.storage()).isEqualTo("ok");
            // No worker running is not a failure — nothing has claimed the queue yet.
            assertThat(report.workers().count()).isZero();
            assertThat(report.workers().status()).isEqualTo("none");
            assertThat(report.resources()).isNull();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsTheSharedProbeShape(@TempDir Path dir) {
        try (Taskito queue = open(dir)) {
            Map<String, Object> body = Health.readiness(queue).toMap();
            assertThat(body).containsEntry("status", "ready");

            Map<String, Object> checks = (Map<String, Object>) body.get("checks");
            assertThat(checks).containsEntry("storage", "ok");
            assertThat((Map<String, Object>) checks.get("workers"))
                    .containsEntry("count", 0)
                    .containsEntry("status", "none");
            // Absent, not empty, when no worker advertises a resource.
            assertThat(checks).doesNotContainKey("resources");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void degradesInsteadOfThrowingWhenStorageIsGone(@TempDir Path dir) {
        Taskito queue = open(dir);
        queue.close();

        ReadinessReport report = Health.readiness(queue);
        assertThat(report.ready()).isFalse();
        assertThat(report.status()).isEqualTo("degraded");
        assertThat(report.storage()).startsWith("error: ");

        // A probe endpoint must still be able to answer with a body.
        Map<String, Object> checks = (Map<String, Object>) report.toMap().get("checks");
        assertThat((String) checks.get("storage")).startsWith("error: ");
    }

    @Test
    void reportsNoResourcesWhenNoWorkerAdvertisesAny(@TempDir Path dir) {
        try (Taskito queue = open(dir)) {
            assertThat(Health.resourceStatus(queue)).isEmpty();
        }
    }
}
