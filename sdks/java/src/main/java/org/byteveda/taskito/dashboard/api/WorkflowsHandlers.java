package org.byteveda.taskito.dashboard.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.dashboard.support.Http;
import org.byteveda.taskito.dashboard.support.Json;
import org.byteveda.taskito.workflows.NodeSnapshot;
import org.jspecify.annotations.Nullable;

/**
 * Workflow read endpoints: run listing, run detail (run + nodes), children, and
 * the DAG. The DAG is enriched — edges rebuilt from each node's {@code deps} and
 * live status/job-id folded in — so the SPA's graph view renders real links.
 */
public final class WorkflowsHandlers {
    private static final long DEFAULT_LIMIT = 50;

    private final Taskito queue;

    public WorkflowsHandlers(Taskito queue) {
        this.queue = queue;
    }

    public Object runs(Map<String, String> query) {
        long limit = Http.longParam(query, "limit", DEFAULT_LIMIT);
        long offset = Http.longParam(query, "offset", 0);
        List<Object> runs =
                queue.listWorkflowRuns(query.get("definition_name"), query.get("state"), limit, offset).stream()
                        .map(Contract::workflowRun)
                        .collect(Collectors.toList());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runs", runs);
        out.put("limit", limit);
        out.put("offset", offset);
        return out;
    }

    public @Nullable Object run(String id) {
        var run = queue.getWorkflowRun(id).orElse(null);
        if (run == null) {
            return null;
        }
        List<Object> nodes = nodesOf(id).stream().map(Contract::workflowNode).collect(Collectors.toList());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("run", Contract.workflowRun(run));
        out.put("nodes", nodes);
        return out;
    }

    public Object children(String id) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put(
                "children",
                queue.getWorkflowChildren(id).stream()
                        .map(Contract::workflowRun)
                        .collect(Collectors.toList()));
        return out;
    }

    public @Nullable Object dag(String id) {
        String dag = queue.getWorkflowDag(id).orElse(null);
        if (dag == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dag", enrichDag(dag, nodesOf(id)));
        return out;
    }

    private List<NodeSnapshot> nodesOf(String id) {
        return queue.workflowStatus(id).map(status -> status.nodes).orElse(List.of());
    }

    /**
     * Rewrite the raw {@code SerializableGraph} into the SPA's DAG shape: edges
     * come from each node's incoming edges, and live status/job-id are folded in.
     * Returns a JSON string (the SPA parses it).
     */
    private static String enrichDag(String dagJson, List<NodeSnapshot> nodes) {
        Map<String, Object> graph = Json.parseMap(dagJson);
        if (graph == null) {
            return dagJson; // not our JSON — pass through
        }
        Map<String, NodeSnapshot> byName = new HashMap<>();
        for (NodeSnapshot node : nodes) {
            byName.put(node.nodeName, node);
        }
        List<Map<String, Object>> edges = asMapList(graph.get("edges"));
        List<Map<String, Object>> enriched = new ArrayList<>();
        for (Map<String, Object> raw : asMapList(graph.get("nodes"))) {
            String name = raw.get("name") == null ? "" : String.valueOf(raw.get("name"));
            NodeSnapshot node = byName.get(name);
            List<Object> deps = new ArrayList<>();
            for (Map<String, Object> edge : edges) {
                if (name.equals(edge.get("to"))) {
                    deps.add(edge.get("from"));
                }
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", name);
            entry.put("node_name", name);
            entry.put("status", node != null ? node.status : "pending");
            entry.put("id", node != null && node.jobId != null ? node.jobId : name);
            entry.put("deps", deps);
            enriched.add(entry);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodes", enriched);
        result.put("edges", edges);
        return Json.toString(result);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> asMapList(@Nullable Object value) {
        if (!(value instanceof List)) {
            return List.of();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object item : (List<Object>) value) {
            if (item instanceof Map) {
                out.add((Map<String, Object>) item);
            }
        }
        return out;
    }
}
