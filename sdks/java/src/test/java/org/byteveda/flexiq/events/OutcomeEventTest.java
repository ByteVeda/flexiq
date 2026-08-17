package org.byteveda.flexiq.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class OutcomeEventTest {

    @Test
    void reportsMeasuredDurationInMilliseconds() {
        OutcomeEvent event = new OutcomeEvent(EventName.SUCCESS, "j", "t", null, -1, false, 42_000_000L);
        assertEquals(42L, event.durationMs());
    }

    @Test
    void subMillisecondRunReportsZeroNotNull() {
        // It ran — 0ms is a measurement, not the absence of one.
        OutcomeEvent event = new OutcomeEvent(EventName.SUCCESS, "j", "t", null, -1, false, 500_000L);
        assertEquals(0L, event.durationMs());
    }

    @Test
    void unmeasuredRunHasNoDuration() {
        OutcomeEvent event = new OutcomeEvent(EventName.DEAD, "j", "t", "boom", -1, false, 0L);
        assertNull(event.durationMs());
        assertNull(new OutcomeEvent(EventName.DEAD, "j", "t", "boom", -1, false).durationMs());
    }

    @Test
    void nameAccessorMirrorsTheField() {
        OutcomeEvent event = new OutcomeEvent(EventName.RETRY, "j", "t", "boom", 1, false);
        assertEquals(EventName.RETRY, event.name());
        assertEquals(event.name, event.name());
    }

    @Test
    void jobFailedShapedEventCarriesTheAttemptError() {
        OutcomeEvent event = new OutcomeEvent(EventName.JOB_FAILED, "j", "t", "boom", -1, false, 0L);
        assertEquals(EventName.JOB_FAILED, event.name());
        assertEquals("boom", event.error);
        assertNull(event.durationMs());
    }
}
