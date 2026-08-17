package org.byteveda.flexiq.workflows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;

/** A run's DAG: nodes by name plus successor adjacency, built from the native plan. */
final class RunPlan {
    private final Map<String, PlanNode> byName;
    private final Map<String, List<String>> successors;

    private RunPlan(Map<String, PlanNode> byName, Map<String, List<String>> successors) {
        this.byName = byName;
        this.successors = successors;
    }

    /** Build from the plan node list, inverting predecessors into successor adjacency. */
    static RunPlan from(List<PlanNode> nodes) {
        Map<String, PlanNode> byName = new HashMap<>();
        Map<String, List<String>> successors = new HashMap<>();
        for (PlanNode node : nodes) {
            byName.put(node.name, node);
        }
        for (PlanNode node : nodes) {
            for (String predecessor : node.predecessors) {
                successors
                        .computeIfAbsent(predecessor, key -> new ArrayList<>())
                        .add(node.name);
            }
        }
        return new RunPlan(byName, successors);
    }

    /** The plan node by name, or {@code null} if absent. */
    @Nullable
    PlanNode node(String name) {
        return byName.get(name);
    }

    /** Every plan node. */
    java.util.Collection<PlanNode> allNodes() {
        return byName.values();
    }

    /** Every successor of {@code node} whose plan entry matches {@code match}. */
    List<PlanNode> successorsMatching(String node, Predicate<PlanNode> match) {
        List<PlanNode> matches = new ArrayList<>();
        for (String successor : successors.getOrDefault(node, List.of())) {
            PlanNode candidate = byName.get(successor);
            if (candidate != null && match.test(candidate)) {
                matches.add(candidate);
            }
        }
        return matches;
    }
}
