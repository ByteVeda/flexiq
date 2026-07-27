package org.byteveda.taskito.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.model.CircuitBreakerState;
import org.byteveda.taskito.task.CircuitBreakerConfig;
import org.byteveda.taskito.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class CircuitBreakerTest {

    @Test
    void builderValidatesInputs() {
        assertThrows(IllegalArgumentException.class, () -> CircuitBreakerConfig.of(0));
        assertThrows(IllegalArgumentException.class, () -> CircuitBreakerConfig.builder(1)
                .halfOpenProbes(0));
        assertThrows(IllegalArgumentException.class, () -> CircuitBreakerConfig.builder(1)
                .halfOpenSuccessRate(1.5));
        assertThrows(IllegalArgumentException.class, () -> CircuitBreakerConfig.builder(1)
                .window(Duration.ZERO));
    }

    @Test
    void builderAppliesDefaults() {
        CircuitBreakerConfig cb = CircuitBreakerConfig.of(3);
        assertEquals(3, cb.threshold());
        assertEquals(Duration.ofSeconds(60), cb.window());
        assertEquals(Duration.ofSeconds(300), cb.cooldown());
        assertEquals(5, cb.halfOpenProbes());
        assertEquals(0.8, cb.halfOpenSuccessRate());
    }

    @Test
    @Timeout(60)
    void breakerOpensAfterThresholdFailures(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("cb.db").toString()).open()) {
            Task<String> failing =
                    Task.of("cb.fail", String.class).maxRetries(0).circuitBreaker(CircuitBreakerConfig.of(2));

            // Enqueue more jobs than the threshold; once two fail, the breaker opens and gates the rest.
            for (int i = 0; i < 5; i++) {
                queue.enqueue(failing, "boom");
            }

            Worker worker = queue.worker()
                    .handle(failing, p -> {
                        throw new RuntimeException("boom");
                    })
                    .start();
            try (worker) {
                CircuitBreakerState state = awaitOpenBreaker(queue, "cb.fail", Duration.ofSeconds(40));
                assertTrue(state.isOpen(), "breaker should be open after reaching the failure threshold");
                assertEquals(2, state.threshold);
            }
        }
    }

    private static CircuitBreakerState awaitOpenBreaker(Taskito queue, String taskName, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<CircuitBreakerState> found = queue.listCircuitBreakers().stream()
                    .filter(b -> taskName.equals(b.taskName))
                    .findFirst();
            if (found.isPresent() && found.get().isOpen()) {
                return found.get();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("circuit breaker for '" + taskName + "' did not open within " + timeout);
    }

    @Test
    @Timeout(30)
    void listReturnsEmptyWithoutBreakers(@TempDir Path dir) throws Exception {
        try (Taskito queue =
                Taskito.builder().url(dir.resolve("cb_empty.db").toString()).open()) {
            List<CircuitBreakerState> breakers = queue.listCircuitBreakers();
            assertTrue(breakers.isEmpty());
        }
    }
}
