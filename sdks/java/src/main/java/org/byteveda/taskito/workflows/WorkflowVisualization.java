package org.byteveda.taskito.workflows;

import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Renders a {@link Workflow}'s DAG as text for docs or debugging: Mermaid
 * ({@code graph TD}) or Graphviz DOT. Node labels note the step kind (gate,
 * fan-out, fan-in, sub-workflow, conditional).
 */
public final class WorkflowVisualization {
    private WorkflowVisualization() {}

    /** A Mermaid {@code graph TD} diagram of the workflow. */
    public static String mermaid(Workflow workflow) {
        WorkflowAnalysis.validate(workflow);
        Map<String, String> ids = nodeIds(workflow);
        StringBuilder out = new StringBuilder("graph TD\n");
        for (Step step : workflow.steps()) {
            String id = ids.get(step.name);
            String label = quote(label(step));
            if (step.gate != null) {
                out.append("  ").append(id).append("{").append(label).append("}\n");
            } else {
                out.append("  ").append(id).append("[").append(label).append("]\n");
            }
        }
        for (Step step : workflow.steps()) {
            for (String pred : step.after) {
                out.append("  ")
                        .append(ids.get(pred))
                        .append(" --> ")
                        .append(ids.get(step.name))
                        .append("\n");
            }
        }
        return out.toString();
    }

    /** A Graphviz DOT digraph of the workflow. */
    public static String dot(Workflow workflow) {
        WorkflowAnalysis.validate(workflow);
        StringBuilder out =
                new StringBuilder("digraph ").append(quote(workflow.name())).append(" {\n");
        for (Step step : workflow.steps()) {
            out.append("  ")
                    .append(quote(step.name))
                    .append(" [label=")
                    .append(quote(label(step)))
                    .append("];\n");
        }
        for (Step step : workflow.steps()) {
            for (String pred : step.after) {
                out.append("  ")
                        .append(quote(pred))
                        .append(" -> ")
                        .append(quote(step.name))
                        .append(";\n");
            }
        }
        return out.append("}\n").toString();
    }

    private static String label(Step step) {
        String kind = kind(step);
        return kind == null ? step.name : step.name + " (" + kind + ")";
    }

    private static @Nullable String kind(Step step) {
        if (step.gate != null) {
            return "gate";
        }
        if (step.subWorkflow != null) {
            return "sub-workflow";
        }
        if (step.fanOut != null) {
            return "fan-out";
        }
        if (step.fanIn != null) {
            return "fan-in";
        }
        if (step.condition != null) {
            return "conditional";
        }
        return null;
    }

    /**
     * Assign each step a collision-free Mermaid id ({@code n0}, {@code n1}, …) in
     * declaration order. A lossy name-to-id mapping could merge distinct steps
     * (e.g. {@code "a-b"} and {@code "a b"}); sequential ids never collide. The real
     * name stays in the node label.
     */
    private static Map<String, String> nodeIds(Workflow workflow) {
        Map<String, String> ids = new HashMap<>();
        int next = 0;
        for (Step step : workflow.steps()) {
            ids.put(step.name, "n" + next++);
        }
        return ids;
    }

    private static String quote(String text) {
        return '"' + text.replace("\"", "\\\"") + '"';
    }
}
