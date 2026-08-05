package org.byteveda.taskito.webhooks;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class WebhookTaskFilterTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static Webhook decode(String json) throws Exception {
        return JSON.readValue(json, Webhook.class);
    }

    @Test
    void decodesTheListShape() throws Exception {
        Webhook hook = decode("{\"id\":\"a\",\"url\":\"https://x.test\",\"taskFilters\":[\"send\",\"resize\"]}");
        assertThat(hook.taskFilters).containsExactly("send", "resize");
    }

    @Test
    void decodesARowWrittenWithTheScalarField() throws Exception {
        Webhook hook = decode("{\"id\":\"a\",\"url\":\"https://x.test\",\"taskFilter\":\"send\"}");
        assertThat(hook.taskFilters).containsExactly("send");
    }

    @Test
    void prefersTheListWhenARowCarriesBoth() throws Exception {
        Webhook hook =
                decode("{\"id\":\"a\",\"url\":\"https://x.test\",\"taskFilter\":\"old\",\"taskFilters\":[\"new\"]}");
        assertThat(hook.taskFilters).containsExactly("new");
    }

    @Test
    void unrestrictedWhenNeitherFieldIsPresent() throws Exception {
        Webhook hook = decode("{\"id\":\"a\",\"url\":\"https://x.test\"}");
        assertThat(hook.taskFilters).isEmpty();
        assertThat(hook.taskFilter).isNull();
    }

    @Test
    void theDeprecatedFieldMirrorsTheFirstEntry() throws Exception {
        Webhook hook = decode("{\"id\":\"a\",\"url\":\"https://x.test\",\"taskFilters\":[\"send\",\"resize\"]}");
        assertThat(hook.taskFilter).isEqualTo("send");
    }

    @Test
    void theBuilderAccumulatesFilters() {
        Webhook.Builder spec =
                Webhook.builder("https://x.test").taskFilters("send").taskFilters("resize", "thumbnail");
        assertThat(spec.taskFilters).containsExactly("send", "resize", "thumbnail");
    }

    @Test
    @SuppressWarnings("deprecation")
    void theDeprecatedBuilderSetterAppendsToTheList() {
        Webhook.Builder spec = Webhook.builder("https://x.test").taskFilter("send");
        assertThat(spec.taskFilters).containsExactly("send");
    }
}
