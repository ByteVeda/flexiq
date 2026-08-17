package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class NodeSnapshotTest {

    private static NodeSnapshot snapshot(Long startedAt, Long completedAt, Long compStart, Long compEnd) {
        return new NodeSnapshot(
                "step",
                NodeStatus.COMPLETED,
                "job",
                null,
                null,
                startedAt,
                completedAt,
                null,
                null,
                compStart,
                compEnd,
                null);
    }

    @Test
    void reportsElapsedBetweenTimestamps() {
        NodeSnapshot node = snapshot(1_000L, 1_250L, 2_000L, 2_075L);
        assertEquals(250L, node.durationMs());
        assertEquals(75L, node.compensationDurationMs());
    }

    @Test
    void unfinishedNodeHasNoDuration() {
        assertNull(snapshot(1_000L, null, null, null).durationMs());
        assertNull(snapshot(null, 1_250L, null, null).durationMs());
    }

    @Test
    void nodeOutsideCompensationHasNoCompensationDuration() {
        assertNull(snapshot(1_000L, 1_250L, null, null).compensationDurationMs());
        assertNull(snapshot(1_000L, 1_250L, 2_000L, null).compensationDurationMs());
    }
}
