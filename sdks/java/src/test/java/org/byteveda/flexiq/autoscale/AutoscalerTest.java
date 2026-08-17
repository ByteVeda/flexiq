package org.byteveda.flexiq.autoscale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AutoscalerTest {

    private static final AutoscaleOptions OPTS = new AutoscaleOptions(1, 5, 10, Duration.ofSeconds(60));

    @Test
    void desiredSizeClampsToBounds() {
        assertEquals(1, Autoscaler.desiredSize(0, OPTS)); // empty → floor
        assertEquals(1, Autoscaler.desiredSize(5, OPTS)); // ceil(0.5) → floor
        assertEquals(3, Autoscaler.desiredSize(25, OPTS)); // ceil(2.5)
        assertEquals(5, Autoscaler.desiredSize(1000, OPTS)); // clamp to ceiling
    }

    @Test
    void tickResizesPoolToDepth() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        AtomicLong depth = new AtomicLong(0);
        try (Autoscaler scaler = new Autoscaler(pool, depth::get, OPTS)) {
            depth.set(30);
            scaler.tick();
            assertEquals(3, pool.getCorePoolSize());
            assertEquals(3, pool.getMaximumPoolSize());

            depth.set(0);
            scaler.tick();
            assertEquals(1, pool.getCorePoolSize());
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void rejectsInvalidOptions() {
        assertThrows(IllegalArgumentException.class, () -> new AutoscaleOptions(0, 5, 10, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new AutoscaleOptions(3, 1, 10, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new AutoscaleOptions(1, 5, 0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new AutoscaleOptions(1, 5, 10, Duration.ZERO));
    }
}
