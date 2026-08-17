package org.byteveda.flexiq.webhooks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DelivererBackoffTest {

    private static Webhook hook(double retryBackoff) {
        return new Webhook(
                "id",
                "https://example.test/hook",
                List.of(),
                null,
                Map.of(),
                null,
                5,
                10_000,
                retryBackoff,
                true,
                null,
                0L,
                0L);
    }

    @Test
    void followsTheContractCurveFromZero() {
        Webhook hook = hook(2.0);
        assertThat(Deliverer.backoffMs(hook, 0)).isEqualTo(1_000);
        assertThat(Deliverer.backoffMs(hook, 1)).isEqualTo(2_000);
        assertThat(Deliverer.backoffMs(hook, 2)).isEqualTo(4_000);
    }

    @Test
    void honoursACustomBase() {
        Webhook hook = hook(3.0);
        assertThat(Deliverer.backoffMs(hook, 0)).isEqualTo(1_000);
        assertThat(Deliverer.backoffMs(hook, 1)).isEqualTo(3_000);
        assertThat(Deliverer.backoffMs(hook, 2)).isEqualTo(9_000);
    }

    @Test
    void capsAtThirtySeconds() {
        assertThat(Deliverer.backoffMs(hook(2.0), 20)).isEqualTo(30_000);
        assertThat(Deliverer.backoffMs(hook(2.0), Integer.MAX_VALUE)).isEqualTo(30_000);
    }

    @Test
    void defaultsWhenAStoredRowPredatesTheField() {
        // Jackson decodes a missing retryBackoff as 0.0, which would retry with no wait.
        assertThat(hook(0.0).retryBackoff).isEqualTo(Webhook.DEFAULT_RETRY_BACKOFF);
    }
}
