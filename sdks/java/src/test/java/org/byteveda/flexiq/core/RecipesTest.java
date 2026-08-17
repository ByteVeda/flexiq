package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.byteveda.flexiq.predicates.EnqueueDecision;
import org.byteveda.flexiq.predicates.EnqueueGate;
import org.byteveda.flexiq.predicates.PredicateContext;
import org.byteveda.flexiq.predicates.Recipes;
import org.junit.jupiter.api.Test;

class RecipesTest {

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final PredicateContext CTX = new PredicateContext("t", "payload");

    @Test
    void payloadMatchesAllowsAndSkips() {
        EnqueueGate gate = Recipes.payloadMatches(payload -> "yes".equals(payload));

        assertInstanceOf(EnqueueDecision.Allow.class, gate.decide(new PredicateContext("t", "yes")));
        assertInstanceOf(EnqueueDecision.Skip.class, gate.decide(new PredicateContext("t", "no")));
    }

    @Test
    void featureFlagAllowsWhenEnabledSkipsOtherwise() {
        assertInstanceOf(
                EnqueueDecision.Allow.class,
                Recipes.featureFlag("beta", flag -> true).decide(CTX));
        assertInstanceOf(
                EnqueueDecision.Skip.class,
                Recipes.featureFlag("beta", flag -> false).decide(CTX));
    }

    @Test
    void dayOfWeekAllowsToday() {
        var today = ZonedDateTime.now(UTC).getDayOfWeek();
        assertInstanceOf(
                EnqueueDecision.Allow.class, Recipes.dayOfWeek(UTC, today).decide(CTX));
    }

    @Test
    void dayOfWeekDefersToOtherDay() {
        var tomorrow = ZonedDateTime.now(UTC).plusDays(1).getDayOfWeek();
        EnqueueDecision decision = Recipes.dayOfWeek(UTC, tomorrow).decide(CTX);
        EnqueueDecision.Defer defer = assertInstanceOf(EnqueueDecision.Defer.class, decision);
        assertTrue(!defer.delay().isNegative() && defer.delay().compareTo(Duration.ofDays(2)) <= 0);
    }

    @Test
    void timeWindowAllowsWhenWindowSpansTheDay() {
        EnqueueGate gate = Recipes.timeWindow(UTC, LocalTime.MIN, LocalTime.MAX);
        assertInstanceOf(EnqueueDecision.Allow.class, gate.decide(CTX));
    }

    @Test
    void businessHoursAllowsOrDefersWithinAWeek() {
        EnqueueDecision decision = Recipes.businessHours(UTC).decide(CTX);
        if (decision instanceof EnqueueDecision.Defer defer) {
            assertTrue(defer.delay().compareTo(Duration.ofDays(7)) <= 0);
        } else {
            assertInstanceOf(EnqueueDecision.Allow.class, decision);
        }
    }
}
