package org.byteveda.flexiq.codegen;

import org.byteveda.flexiq.annotation.TaskHandler;

/** Test fixture exercising the per-task policy attributes of {@code @TaskHandler}. */
class AnnotatedPolicies {

    @TaskHandler(value = "ap.idem", idempotent = true)
    String idem(String s) {
        return s;
    }

    @TaskHandler(
            value = "ap.cb",
            circuitBreakerThreshold = 3,
            circuitBreakerWindowSeconds = 30,
            circuitBreakerCooldownSeconds = 120,
            circuitBreakerHalfOpenProbes = 2,
            circuitBreakerHalfOpenSuccessRate = 0.5)
    String cb(String s) {
        return s;
    }
}
