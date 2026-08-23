package org.byteveda.flexiq.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.byteveda.flexiq.errors.SerializationException;
import org.junit.jupiter.api.Test;

class EventNameTest {

    @Test
    void wireNamesAreUniqueAcrossAllValues() {
        Set<String> seen = new HashSet<>();
        for (EventName name : EventName.values()) {
            assertTrue(seen.add(name.wireName()), "duplicate wire name: " + name.wireName());
        }
        assertEquals(30, seen.size());
    }

    @Test
    void everyPredicateWireNameInTheContractHasAConstant() {
        assertEquals("predicate.rejected", EventName.PREDICATE_REJECTED.wireName());
        assertEquals("predicate.deferred", EventName.PREDICATE_DEFERRED.wireName());
        assertEquals("predicate.skipped", EventName.PREDICATE_SKIPPED.wireName());
        assertEquals("predicate.cancelled", EventName.PREDICATE_CANCELLED.wireName());
    }

    @Test
    void fromWireRoundTripsEveryConstant() {
        for (EventName name : EventName.values()) {
            assertEquals(name, EventName.fromWire(name.wireName()));
        }
    }

    @Test
    void fromWireAcceptsLegacyOutcomeAliases() {
        assertEquals(EventName.SUCCESS, EventName.fromWire("success"));
        assertEquals(EventName.RETRY, EventName.fromWire("retry"));
        assertEquals(EventName.DEAD, EventName.fromWire("dead"));
        assertEquals(EventName.CANCELLED, EventName.fromWire("cancelled"));
    }

    @Test
    void fromWireRejectsUnknownNames() {
        assertThrows(SerializationException.class, () -> EventName.fromWire("job.exploded"));
        assertThrows(SerializationException.class, () -> EventName.fromWire("SUCCESS"));
    }

    @Test
    void fromKindStillMapsTheFourNativeOutcomes() {
        assertEquals(EventName.SUCCESS, EventName.fromKind("success"));
        assertEquals(EventName.RETRY, EventName.fromKind("retry"));
        assertEquals(EventName.DEAD, EventName.fromKind("dead"));
        assertEquals(EventName.CANCELLED, EventName.fromKind("cancelled"));
        assertThrows(SerializationException.class, () -> EventName.fromKind("job.completed"));
    }

    @Test
    void firstFourOrdinalsAreStable() {
        assertEquals(0, EventName.SUCCESS.ordinal());
        assertEquals(1, EventName.RETRY.ordinal());
        assertEquals(2, EventName.DEAD.ordinal());
        assertEquals(3, EventName.CANCELLED.ordinal());
    }

    @Test
    void onlyOutcomeNamesReportIsJobOutcome() {
        Set<EventName> outcomes =
                Set.of(EventName.SUCCESS, EventName.RETRY, EventName.DEAD, EventName.CANCELLED, EventName.JOB_FAILED);
        for (EventName name : EventName.values()) {
            assertEquals(outcomes.contains(name), name.isJobOutcome(), name.toString());
        }
    }

    @Test
    void dottedWireNamesFollowTheTaxonomy() {
        assertEquals("job.completed", EventName.SUCCESS.wireName());
        assertEquals("job.retrying", EventName.RETRY.wireName());
        assertEquals("worker.online", EventName.WORKER_ONLINE.wireName());
        assertEquals("workflow.gate_reached", EventName.WORKFLOW_GATE_REACHED.wireName());
        assertEquals("predicate.rejected", EventName.PREDICATE_REJECTED.wireName());
        for (EventName name : EventName.values()) {
            assertFalse(name.wireName().isEmpty());
            assertTrue(name.wireName().contains("."), name + " must use a dotted wire name");
        }
    }
}
