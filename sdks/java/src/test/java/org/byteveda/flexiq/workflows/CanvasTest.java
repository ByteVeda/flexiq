package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;

class CanvasTest {

    private static final Task<Integer> T = Task.of("cv.t", Integer.class);

    @Test
    void chainIsLinear() {
        Workflow wf = Canvas.chain("c", Canvas.link("a", T, 1), Canvas.link("b", T, 2), Canvas.link("c", T, 3));
        assertEquals(List.of("a", "b", "c"), WorkflowAnalysis.topologicalOrder(wf));
        assertEquals(List.of("a"), WorkflowAnalysis.roots(wf));
        assertEquals(List.of("c"), WorkflowAnalysis.leaves(wf));
    }

    @Test
    void groupIsParallel() {
        Workflow wf = Canvas.group("g", Canvas.link("a", T, 1), Canvas.link("b", T, 2), Canvas.link("c", T, 3));
        assertEquals(Set.of("a", "b", "c"), Set.copyOf(WorkflowAnalysis.roots(wf)));
        assertEquals(Set.of("a", "b", "c"), Set.copyOf(WorkflowAnalysis.leaves(wf)));
    }

    @Test
    void chordCallbackRunsAfterGroup() {
        Workflow wf = Canvas.chord("ch", Canvas.link("cb", T, 0), Canvas.link("a", T, 1), Canvas.link("b", T, 2));
        assertEquals(Set.of("a", "b"), WorkflowAnalysis.ancestors(wf, "cb"));
        assertEquals(List.of("cb"), WorkflowAnalysis.leaves(wf));
        assertTrue(WorkflowAnalysis.roots(wf).containsAll(Set.of("a", "b")));
    }
}
