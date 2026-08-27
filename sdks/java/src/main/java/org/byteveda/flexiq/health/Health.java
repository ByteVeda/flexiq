package org.byteveda.flexiq.health;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.WorkerInfo;
import org.jspecify.annotations.Nullable;

/** Liveness and readiness probes that can be wired into any HTTP framework or container probe. */
public final class Health {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Health() {}

    /**
     * Basic liveness check — always ok.
     *
     * @return an {@code ok} report; a process that could not answer would never reach here
     */
    public static HealthReport check() {
        return new HealthReport("ok");
    }

    /**
     * Readiness: storage reachable, workers alive, resources healthy. Never
     * throws — a failing dependency lands in its own check and degrades the
     * status, so a probe endpoint can always answer.
     *
     * @param queue the queue to probe
     * @return the report, {@code degraded} rather than thrown when a dependency fails
     */
    public static ReadinessReport readiness(FlexiQ queue) {
        String storage = "ok";
        boolean allOk = true;
        try {
            queue.stats();
        } catch (RuntimeException e) {
            storage = "error: " + e;
            allOk = false;
        }

        WorkersCheck workers;
        try {
            int count = queue.listWorkers().size();
            workers = new WorkersCheck(count, count > 0 ? "ok" : "none", null);
        } catch (RuntimeException e) {
            workers = new WorkersCheck(0, "error", "error: " + e);
            allOk = false;
        }

        ResourcesCheck resources = null;
        try {
            List<ResourceStatusEntry> entries = resourceStatus(queue);
            // Absent, not empty, when nothing advertises a resource — there is
            // nothing to be ready for.
            if (!entries.isEmpty()) {
                List<String> unhealthy = entries.stream()
                        .filter(entry -> !"healthy".equals(entry.health()))
                        .map(ResourceStatusEntry::name)
                        .toList();
                resources =
                        new ResourcesCheck(entries.size(), unhealthy, unhealthy.isEmpty() ? "ok" : "degraded", null);
                allOk = allOk && unhealthy.isEmpty();
            }
        } catch (RuntimeException e) {
            resources = new ResourcesCheck(0, List.of(), "error", "error: " + e);
            allOk = false;
        }

        return new ReadinessReport(allOk ? "ready" : "degraded", storage, workers, resources);
    }

    /**
     * Per-resource status aggregated across workers, derived from each worker's
     * advertised resources plus the health its heartbeat reported. A resource a
     * worker advertises but never reports on is {@code not_initialized}; when
     * workers disagree, the worst health wins.
     *
     * @param queue the queue whose workers are polled
     * @return one entry per resource any live worker advertises or reports on, by name
     */
    public static List<ResourceStatusEntry> resourceStatus(FlexiQ queue) {
        Map<String, Integer> reported = new TreeMap<>();
        Set<String> advertised = new TreeSet<>();
        for (WorkerInfo worker : queue.listWorkers()) {
            advertised.addAll(parseStringList(worker.resources));
            // BinaryOperator.maxBy keeps the merge on boxed Integers — Math::max
            // would funnel both arguments through an unchecked Integer->int unboxing.
            parseMap(worker.resourceHealth)
                    .forEach((name, value) -> reported.merge(
                            name, severity(String.valueOf(value)), BinaryOperator.maxBy(Comparator.naturalOrder())));
        }

        Set<String> names = new TreeSet<>(advertised);
        names.addAll(reported.keySet());
        List<ResourceStatusEntry> out = new ArrayList<>(names.size());
        for (String name : names) {
            Integer worst = reported.get(name);
            String health = worst == null ? "not_initialized" : healthName(worst);
            out.add(new ResourceStatusEntry(name, "worker", health, 0, 0, List.of()));
        }
        return out;
    }

    private static int severity(String health) {
        return switch (health) {
            case "unhealthy" -> 2;
            case "degraded" -> 1;
            default -> 0;
        };
    }

    private static String healthName(int severity) {
        return switch (severity) {
            case 2 -> "unhealthy";
            case 1 -> "degraded";
            default -> "healthy";
        };
    }

    /** A heartbeat's JSON payloads are advisory: unreadable ones read as absent. */
    private static List<String> parseStringList(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return List.of();
        }
        try {
            return JSON.readValue(json, new TypeReference<List<String>>() {});
        } catch (Exception e) {
            return List.of();
        }
    }

    private static Map<String, Object> parseMap(@Nullable String json) {
        if (json == null || json.isEmpty()) {
            return Map.of();
        }
        try {
            return JSON.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }
}
