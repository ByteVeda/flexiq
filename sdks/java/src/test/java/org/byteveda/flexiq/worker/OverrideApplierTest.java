package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.store.OverridesStore;
import org.byteveda.flexiq.dashboard.store.OverridesStore.Scope;
import org.junit.jupiter.api.Test;

class OverrideApplierTest {

    private final InMemorySettings settings = new InMemorySettings();
    private final OverridesStore store = new OverridesStore(settings);

    private static Map<String, Object> config(String name, Object... pairs) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("name", name);
        for (int i = 0; i < pairs.length; i += 2) {
            config.put((String) pairs[i], pairs[i + 1]);
        }
        return config;
    }

    private static Map<String, Object> patch(Object... pairs) {
        Map<String, Object> patch = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            patch.put((String) pairs[i], pairs[i + 1]);
        }
        return patch;
    }

    @Test
    void patchesADeclaredTaskConfig() {
        store.putTask("email", patch("rate_limit", "5/s", "max_concurrent", 3, "retry_backoff", 2));
        List<Map<String, Object>> declared =
                List.of(config("email", "rateLimit", "100/m", "maxDelayMs", 60_000L, "onExcess", "drop"));

        List<Map<String, Object>> out = OverrideApplier.applyTaskOverrides(declared, Set.of("email"), store);

        assertEquals(
                List.of(config(
                        "email",
                        "rateLimit",
                        "5/s",
                        "maxDelayMs",
                        60_000L,
                        "onExcess",
                        "drop",
                        "maxConcurrent",
                        3,
                        "baseDelayMs",
                        2000L)),
                out);
        assertEquals("100/m", declared.get(0).get("rateLimit"), "the declared config is not mutated");
    }

    @Test
    void anOverrideOnlyTaskGainsAFreshEntry() {
        store.putTask("report", patch("max_concurrent", 1));
        List<Map<String, Object>> declared = List.of(config("email", "rateLimit", "1/s"));

        List<Map<String, Object>> out = OverrideApplier.applyTaskOverrides(declared, Set.of("email", "report"), store);

        assertEquals(List.of(config("email", "rateLimit", "1/s"), config("report", "maxConcurrent", 1)), out);
    }

    @Test
    void anUnregisteredTaskIsNotApplied() {
        store.putTask("elsewhere", patch("max_concurrent", 1));

        assertEquals(List.of(), OverrideApplier.applyTaskOverrides(List.of(), Set.of("email"), store));
    }

    @Test
    void retryBackoffRoundsToWholeMilliseconds() {
        store.putTask("a", patch("retry_backoff", 1.2345));
        store.putTask("b", patch("retry_backoff", 0.0006));
        store.putTask("c", patch("retry_backoff", 0));

        List<Map<String, Object>> out = OverrideApplier.applyTaskOverrides(List.of(), Set.of("a", "b", "c"), store);

        assertEquals(
                List.of(
                        config("a", "baseDelayMs", 1235L),
                        config("b", "baseDelayMs", 1L),
                        config("c", "baseDelayMs", 0L)),
                out);
    }

    @Test
    void fieldsWithoutAWorkerConfigSlotAreIgnored() {
        store.putTask("email", patch("max_retries", 7, "timeout", 30, "priority", 9, "paused", true));
        store.putQueue("bulk", patch("paused", true));

        assertEquals(List.of(), OverrideApplier.applyTaskOverrides(List.of(), Set.of("email"), store));
        assertEquals(List.of(), OverrideApplier.applyQueueOverrides(List.of(), store));
    }

    @Test
    void queueOverridesMergeAndAddQueues() {
        store.putQueue("bulk", patch("rate_limit", "10/m"));
        store.putQueue("fresh", patch("max_concurrent", 4, "rate_limit", "2/s"));
        List<Map<String, Object>> declared = List.of(config("bulk", "codelTargetMs", 5L, "codelIntervalMs", 100L));

        List<Map<String, Object>> out = OverrideApplier.applyQueueOverrides(declared, store);

        assertEquals(
                List.of(
                        config("bulk", "codelTargetMs", 5L, "codelIntervalMs", 100L, "rateLimit", "10/m"),
                        config("fresh", "rateLimit", "2/s", "maxConcurrent", 4)),
                out);
    }

    @Test
    void malformedStoredFieldsAreSkippedNotFatal() {
        // Hand-written rows bypass the store's validation, as an operator's direct edit would.
        settings.setSetting(
                OverridesStore.key(Scope.TASK, null, "email"),
                "{\"rate_limit\":\"0/m\",\"max_concurrent\":\"lots\",\"retry_backoff\":-1}");
        settings.setSetting(
                OverridesStore.key(Scope.TASK, null, "report"),
                "{\"rate_limit\":42,\"max_concurrent\":2.5,\"retry_backoff\":3}");
        settings.setSetting(OverridesStore.key(Scope.TASK, null, "broken"), "not json");
        settings.setSetting(
                OverridesStore.key(Scope.QUEUE, null, "bulk"), "{\"rate_limit\":\"5/day\",\"max_concurrent\":-1}");

        assertEquals(
                List.of(config("report", "baseDelayMs", 3000L)),
                OverrideApplier.applyTaskOverrides(List.of(), Set.of("email", "report", "broken"), store));
        assertEquals(List.of(), OverrideApplier.applyQueueOverrides(List.of(), store));
    }

    @Test
    void rateSpecsFollowTheCoreGrammar() {
        for (String ok : List.of("100/m", "1/s", " 2.5 / h ", "1e3/sec", "1./minute", "3/hour", "+4/hr")) {
            assertTrue(OverrideApplier.isValidRate(ok), ok);
        }
        for (String bad : List.of("0/m", "0.5/s", "-1/s", "NaN/s", "Infinity/s", "5d/s", "5/day", "5", "5/m/s", "/s")) {
            assertFalse(OverrideApplier.isValidRate(bad), bad);
        }
    }

    @Test
    void onlyTheWorkersNamespaceApplies() {
        OverridesStore tenant = new OverridesStore(settings, "tenant");
        tenant.putTask("email", patch("max_concurrent", 9));
        tenant.putQueue("bulk", patch("max_concurrent", 9));
        store.putTask("email", patch("max_concurrent", 1));

        assertEquals(
                List.of(config("email", "maxConcurrent", 1)),
                OverrideApplier.applyTaskOverrides(List.of(), Set.of("email"), store));
        assertEquals(List.of(), OverrideApplier.applyQueueOverrides(List.of(), store));
        assertEquals(
                List.of(config("email", "maxConcurrent", 9)),
                OverrideApplier.applyTaskOverrides(List.of(), Set.of("email"), tenant));
        assertEquals(
                List.of(config("bulk", "maxConcurrent", 9)), OverrideApplier.applyQueueOverrides(List.of(), tenant));
        assertTrue(new OverridesStore(settings, "other").taskNames().isEmpty());
    }
}
