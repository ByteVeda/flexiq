package org.byteveda.flexiq.workflows;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.byteveda.flexiq.errors.WorkflowException;

/**
 * Pure-graph queries over a {@link Workflow}'s DAG (its steps and their
 * {@code after} edges) — useful for validation and tooling before a run starts.
 * All methods are static and read-only.
 */
public final class WorkflowAnalysis {
    private WorkflowAnalysis() {}

    /**
     * Ensure every {@code after} edge names a declared step. Throws a
     * {@link WorkflowException} on the first dangling dependency. Every query below
     * runs this first, so they all reject the same invalid workflows.
     */
    public static void validate(Workflow workflow) {
        Set<String> names = new HashSet<>();
        for (Step step : workflow.steps()) {
            names.add(step.name);
        }
        for (Step step : workflow.steps()) {
            for (String pred : step.after) {
                if (!names.contains(pred)) {
                    throw new WorkflowException("step '" + step.name + "' depends on unknown step '" + pred + "'");
                }
            }
        }
    }

    /** Nodes in a topological order (predecessors before successors). Throws on a cycle. */
    public static List<String> topologicalOrder(Workflow workflow) {
        validate(workflow);
        Map<String, List<String>> successors = successors(workflow);
        Map<String, Integer> indegree = indegree(workflow);
        Deque<String> ready = new ArrayDeque<>();
        // Seed with roots in declaration order for a stable result.
        for (Step step : workflow.steps()) {
            if (indegree.getOrDefault(step.name, 0) == 0) {
                ready.add(step.name);
            }
        }
        List<String> order = new ArrayList<>(indegree.size());
        while (!ready.isEmpty()) {
            String node = ready.poll();
            order.add(node);
            for (String next : successors.getOrDefault(node, List.of())) {
                indegree.merge(next, -1, (count, delta) -> count + delta);
                if (indegree.getOrDefault(next, 0) == 0) {
                    ready.add(next);
                }
            }
        }
        if (order.size() != indegree.size()) {
            throw new WorkflowException("workflow '" + workflow.name() + "' has a cycle");
        }
        return order;
    }

    /** Nodes grouped into dependency levels: level 0 is the roots, each later level depends only on earlier ones. */
    public static List<List<String>> levels(Workflow workflow) {
        Map<String, List<String>> predecessors = predecessors(workflow);
        Map<String, Integer> level = new HashMap<>();
        List<List<String>> levels = new ArrayList<>();
        for (String node : topologicalOrder(workflow)) {
            int depth = 0;
            for (String pred : predecessors.getOrDefault(node, List.of())) {
                depth = Math.max(depth, level.getOrDefault(pred, 0) + 1);
            }
            level.put(node, depth);
            while (levels.size() <= depth) {
                levels.add(new ArrayList<>());
            }
            levels.get(depth).add(node);
        }
        return levels;
    }

    /** All nodes that must complete before {@code node} (transitive predecessors). */
    public static Set<String> ancestors(Workflow workflow, String node) {
        validate(workflow);
        return reachable(predecessors(workflow), node);
    }

    /** All nodes that run after {@code node} (transitive successors). */
    public static Set<String> descendants(Workflow workflow, String node) {
        validate(workflow);
        return reachable(successors(workflow), node);
    }

    /** Nodes with no predecessors. */
    public static List<String> roots(Workflow workflow) {
        validate(workflow);
        Map<String, Integer> indegree = indegree(workflow);
        List<String> roots = new ArrayList<>();
        for (Step step : workflow.steps()) {
            if (indegree.getOrDefault(step.name, 0) == 0) {
                roots.add(step.name);
            }
        }
        return roots;
    }

    /** Nodes with no successors. */
    public static List<String> leaves(Workflow workflow) {
        validate(workflow);
        Map<String, List<String>> successors = successors(workflow);
        List<String> leaves = new ArrayList<>();
        for (Step step : workflow.steps()) {
            if (successors.getOrDefault(step.name, List.of()).isEmpty()) {
                leaves.add(step.name);
            }
        }
        return leaves;
    }

    private static Set<String> reachable(Map<String, List<String>> edges, String start) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> stack = new ArrayDeque<>(edges.getOrDefault(start, List.of()));
        while (!stack.isEmpty()) {
            String node = stack.pop();
            if (seen.add(node)) {
                stack.addAll(edges.getOrDefault(node, List.of()));
            }
        }
        return seen;
    }

    private static Map<String, List<String>> predecessors(Workflow workflow) {
        Map<String, List<String>> predecessors = new HashMap<>();
        for (Step step : workflow.steps()) {
            predecessors.put(step.name, new ArrayList<>(step.after));
        }
        return predecessors;
    }

    private static Map<String, List<String>> successors(Workflow workflow) {
        Map<String, List<String>> successors = new HashMap<>();
        for (Step step : workflow.steps()) {
            for (String pred : step.after) {
                successors.computeIfAbsent(pred, key -> new ArrayList<>()).add(step.name);
            }
        }
        return successors;
    }

    private static Map<String, Integer> indegree(Workflow workflow) {
        Map<String, Integer> indegree = new HashMap<>();
        for (Step step : workflow.steps()) {
            indegree.put(step.name, step.after.size());
        }
        return indegree;
    }
}
