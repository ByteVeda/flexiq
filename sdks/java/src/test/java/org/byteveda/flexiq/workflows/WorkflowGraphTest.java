package org.byteveda.flexiq.workflows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.byteveda.flexiq.errors.WorkflowException;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;

class WorkflowGraphTest {

    private static final Task<Integer> T = Task.of("v.t", Integer.class);

    // a → {b, c} → d (a diamond).
    private static Workflow diamond() {
        return Workflow.named("graph")
                .step("a", T, 1)
                .step("b", T, 1, "a")
                .step("c", T, 1, "a")
                .step("d", T, 1, "b", "c");
    }

    @Test
    void topologicalOrderRespectsDependencies() {
        List<String> order = WorkflowAnalysis.topologicalOrder(diamond());
        assertTrue(order.indexOf("a") < order.indexOf("b"));
        assertTrue(order.indexOf("a") < order.indexOf("c"));
        assertTrue(order.indexOf("b") < order.indexOf("d"));
        assertTrue(order.indexOf("c") < order.indexOf("d"));
    }

    @Test
    void levelsGroupByDepth() {
        List<List<String>> levels = WorkflowAnalysis.levels(diamond());
        assertEquals(List.of("a"), levels.get(0));
        assertEquals(Set.of("b", "c"), Set.copyOf(levels.get(1)));
        assertEquals(List.of("d"), levels.get(2));
    }

    @Test
    void ancestorsDescendantsRootsLeaves() {
        Workflow wf = diamond();
        assertEquals(Set.of("a", "b", "c"), WorkflowAnalysis.ancestors(wf, "d"));
        assertEquals(Set.of("b", "c", "d"), WorkflowAnalysis.descendants(wf, "a"));
        assertEquals(List.of("a"), WorkflowAnalysis.roots(wf));
        assertEquals(List.of("d"), WorkflowAnalysis.leaves(wf));
    }

    @Test
    void cycleIsRejected() {
        Workflow cyclic = Workflow.named("cyc").step("x", T, 1, "y").step("y", T, 1, "x");
        assertThrows(WorkflowException.class, () -> WorkflowAnalysis.topologicalOrder(cyclic));
    }

    @Test
    void unknownDependencyIsRejectedByEveryQuery() {
        Workflow bad = Workflow.named("bad").step("a", T, 1, "missing");
        assertThrows(WorkflowException.class, () -> WorkflowAnalysis.roots(bad));
        assertThrows(WorkflowException.class, () -> WorkflowAnalysis.leaves(bad));
        assertThrows(WorkflowException.class, () -> WorkflowAnalysis.ancestors(bad, "a"));
        assertThrows(WorkflowException.class, () -> WorkflowAnalysis.descendants(bad, "a"));
        assertThrows(WorkflowException.class, () -> WorkflowVisualization.mermaid(bad));
        assertThrows(WorkflowException.class, () -> WorkflowVisualization.dot(bad));
    }

    @Test
    void mermaidRendersNodesAndEdges() {
        String mermaid = WorkflowVisualization.mermaid(diamond());
        assertTrue(mermaid.startsWith("graph TD"));
        // a,b,c,d → n0,n1,n2,n3; edges a→b and c→d.
        assertTrue(mermaid.contains("n0 --> n1"));
        assertTrue(mermaid.contains("n2 --> n3"));
    }

    @Test
    void mermaidMarksGates() {
        Workflow gated = Workflow.named("g").step("a", T, 1).gate("approve", GateConfig.manual(), "a");
        String mermaid = WorkflowVisualization.mermaid(gated);
        assertTrue(mermaid.contains("n1{"));
        assertTrue(mermaid.contains("(gate)"));
    }

    @Test
    void mermaidIdsDoNotCollideOnLossyNames() {
        // "a-b" and "a b" both normalize to "n_a_b" under lossy mapping — must stay distinct.
        Workflow wf = Workflow.named("collide").step("a-b", T, 1).step("a b", T, 1);
        String mermaid = WorkflowVisualization.mermaid(wf);
        assertTrue(mermaid.contains("n0["));
        assertTrue(mermaid.contains("n1["));
    }

    @Test
    void dotRendersDigraph() {
        String dot = WorkflowVisualization.dot(diamond());
        assertTrue(dot.startsWith("digraph"));
        assertTrue(dot.contains("\"a\" -> \"b\""));
    }
}
