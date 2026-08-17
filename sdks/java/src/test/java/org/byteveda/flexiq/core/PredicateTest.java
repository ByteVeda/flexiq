package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.PredicateRejectedException;
import org.byteveda.flexiq.middleware.EnqueueContext;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.predicates.EnqueueDecision;
import org.byteveda.flexiq.predicates.Predicate;
import org.byteveda.flexiq.predicates.PredicateContext;
import org.byteveda.flexiq.predicates.PredicateStats;
import org.byteveda.flexiq.predicates.Predicates;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PredicateTest {

    private static final Task<Integer> TASK = Task.of("p.task", Integer.class);
    private static final Task<Integer> UNGATED = Task.of("p.ungated", Integer.class);

    @Test
    void allowsWhenPredicatePasses(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("p.db").toString()).open()) {
            queue.predicate("p.task", ctx -> (Integer) ctx.payload() > 0);
            assertNotNull(queue.enqueue(TASK, 5));
        }
    }

    @Test
    void rejectsWhenPredicateFails(@TempDir Path dir) {
        try (FlexiQ queue = FlexiQ.builder().url(dir.resolve("p.db").toString()).open()) {
            queue.predicate("p.task", ctx -> (Integer) ctx.payload() > 0);
            assertThrows(PredicateRejectedException.class, () -> queue.enqueue(TASK, -1));
        }
    }

    @Test
    void batchEnqueueRejectsViaPredicate(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("pb.db").toString()).open()) {
            queue.predicate("p.task", ctx -> (Integer) ctx.payload() > 0);
            // One rejected payload fails the whole batch — the batch API can't bypass the gate.
            assertThrows(PredicateRejectedException.class, () -> queue.enqueueMany(TASK, List.of(1, -1, 2)));
        }
    }

    @Test
    void predicateSeesMiddlewareRewrittenPayload(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("pm.db").toString()).open()) {
            queue.predicate("p.task", ctx -> (Integer) ctx.payload() > 0);
            queue.use(new Middleware() {
                @Override
                public void onEnqueue(EnqueueContext context) {
                    context.payload(5); // rewrite the rejected -1 into an accepted value
                }
            });
            // Gate runs after middleware, so it sees the rewritten 5 and allows the enqueue.
            assertNotNull(queue.enqueue(TASK, -1));
        }
    }

    @Test
    void predicateStatsCountOneDecisionPerGatedEnqueue(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("ps.db").toString()).open()) {
            queue.gate("p.task", ctx -> {
                int value = (Integer) ctx.payload();
                if (value < 0) {
                    return EnqueueDecision.reject("negative");
                }
                if (value == 0) {
                    return EnqueueDecision.skip("zero");
                }
                if (value == 1) {
                    return EnqueueDecision.defer(Duration.ofSeconds(1));
                }
                return EnqueueDecision.allow();
            });

            queue.enqueue(TASK, 5);
            queue.tryEnqueue(TASK, 0);
            queue.enqueue(TASK, 1);
            assertThrows(PredicateRejectedException.class, () -> queue.enqueue(TASK, -1));
            queue.enqueue(UNGATED, 5); // ungated: not a decision, so not counted

            PredicateStats stats = queue.predicateStats();
            assertEquals(1, stats.allowed());
            assertEquals(1, stats.skipped());
            assertEquals(1, stats.deferred());
            assertEquals(1, stats.rejected());
            assertEquals(0, stats.errors());
        }
    }

    @Test
    void predicateStatsCountAThrowingGate(@TempDir Path dir) {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("pe.db").toString()).open()) {
            queue.gate("p.task", ctx -> {
                throw new IllegalStateException("gate blew up");
            });
            // The error still reaches the caller; it is counted on the way out.
            assertThrows(IllegalStateException.class, () -> queue.enqueue(TASK, 1));
            assertEquals(1, queue.predicateStats().errors());
        }
    }

    @Test
    void combinators() {
        Predicate even = ctx -> (Integer) ctx.payload() % 2 == 0;
        Predicate positive = ctx -> (Integer) ctx.payload() > 0;

        assertTrue(Predicates.allOf(even, positive).test(ctx(4)));
        assertFalse(Predicates.allOf(even, positive).test(ctx(3)));
        assertTrue(Predicates.anyOf(even, positive).test(ctx(3))); // positive
        assertFalse(Predicates.anyOf(even, positive).test(ctx(-1)));
        assertTrue(Predicates.not(even).test(ctx(3)));
    }

    private static PredicateContext ctx(int payload) {
        return new PredicateContext("p.task", payload);
    }
}
