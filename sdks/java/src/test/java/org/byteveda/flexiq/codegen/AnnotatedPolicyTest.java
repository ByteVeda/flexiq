package org.byteveda.flexiq.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.byteveda.flexiq.task.CircuitBreakerConfig;
import org.junit.jupiter.api.Test;

/** The {@code @TaskHandler} processor threads per-task policy attributes onto the generated tasks. */
class AnnotatedPolicyTest {

    @Test
    void idempotentAttributeReachesGeneratedTask() {
        assertTrue(AnnotatedPoliciesTasks.IDEM.idempotent());
        assertFalse(AnnotatedPoliciesTasks.CB.idempotent());
    }

    @Test
    void circuitBreakerAttributesReachGeneratedTask() {
        CircuitBreakerConfig cb = AnnotatedPoliciesTasks.CB.circuitBreaker();
        assertNotNull(cb, "the annotated threshold should produce a circuit breaker");
        assertEquals(3, cb.threshold());
        assertEquals(Duration.ofSeconds(30), cb.window());
        assertEquals(Duration.ofSeconds(120), cb.cooldown());
        assertEquals(2, cb.halfOpenProbes());
        assertEquals(0.5, cb.halfOpenSuccessRate());
    }

    @Test
    void noThresholdMeansNoBreaker() {
        assertEquals(null, AnnotatedPoliciesTasks.IDEM.circuitBreaker());
    }
}
