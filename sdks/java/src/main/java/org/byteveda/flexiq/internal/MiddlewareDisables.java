package org.byteveda.flexiq.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.spi.QueueBackend;
import org.jspecify.annotations.Nullable;

/**
 * Per-task middleware disable list, persisted under
 * {@code middleware:disabled:<task_name>} as a JSON array of middleware names.
 *
 * <p>Read on every invocation rather than cached, so a toggle takes effect on
 * the next job without restarting the worker. That costs one settings read per
 * job — the same trade the other SDKs accept for a live toggle.
 *
 * <p>Internal: the key format and the name derivation live here so the worker
 * that honours the list and the admin API that writes it cannot drift apart.
 */
public final class MiddlewareDisables {
    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<List<String>> NAMES = new TypeReference<List<String>>() {};

    private static final String KEY_PREFIX = "middleware:disabled:";

    /**
     * Absent for an attached executor, which has no storage to read toggles
     * from; nothing is then disabled, which is the same answer an unconfigured
     * queue gives.
     */
    private final @Nullable QueueBackend backend;

    public MiddlewareDisables(@Nullable QueueBackend backend) {
        this.backend = backend;
    }

    /** The settings key holding {@code taskName}'s disable list. */
    public static String key(String taskName) {
        return KEY_PREFIX + taskName;
    }

    /**
     * The stable name a middleware is keyed on. Java has no name property to
     * prefer, so the class name is the only identity available — the same one
     * already used when logging a middleware failure.
     */
    public static String nameOf(Middleware middleware) {
        return middleware.getClass().getName();
    }

    /**
     * {@code middleware} minus the names in {@code disabledJson}.
     *
     * <p>For an attached executor, whose toggle list arrives on the dispatch
     * frame rather than from a settings read it has no storage to perform. A
     * corrupt list runs everything, which is the same failure-open stance
     * {@link #disabledFor} takes.
     */
    public static List<Middleware> without(List<Middleware> middleware, String disabledJson) {
        if (middleware.isEmpty()) {
            return middleware;
        }
        List<String> disabled;
        try {
            List<String> parsed = JSON.readValue(disabledJson, NAMES);
            disabled = parsed == null ? List.of() : parsed;
        } catch (Exception e) {
            LOG.warn("dispatched middleware disable list is not valid JSON; treating as empty");
            return middleware;
        }
        if (disabled.isEmpty()) {
            return middleware;
        }
        return middleware.stream().filter(m -> !disabled.contains(nameOf(m))).toList();
    }

    /**
     * A stored disable list as a mutable list; empty when unset or unreadable.
     *
     * <p>A corrupt list must not stop the job — run every middleware, which is
     * the same behaviour as having no list at all.
     */
    public static List<String> decode(Optional<String> raw) {
        if (raw.isEmpty()) {
            return new ArrayList<>();
        }
        try {
            List<String> names = JSON.readValue(raw.get(), NAMES);
            return names == null ? new ArrayList<>() : new ArrayList<>(names);
        } catch (Exception e) {
            LOG.warn("middleware disable list is not valid JSON; treating as empty");
            return new ArrayList<>();
        }
    }

    /** Names disabled for {@code taskName}; empty when none are, or the list is unreadable. */
    public List<String> disabledFor(String taskName) {
        if (backend == null) {
            return List.of();
        }
        return List.copyOf(decode(backend.getSetting(key(taskName))));
    }

    /**
     * {@code middleware} minus anything disabled for {@code taskName}.
     *
     * <p>Callers must resolve this <em>once</em> per invocation and reuse it for
     * {@code before}/{@code after}/{@code onError}: re-reading between them would
     * let a mid-job toggle run {@code after} on a middleware whose {@code before}
     * never ran, an unbalanced pair authors are entitled to assume cannot happen.
     */
    public List<Middleware> resolve(String taskName, List<Middleware> middleware) {
        if (middleware.isEmpty()) {
            return middleware;
        }
        List<String> disabled = disabledFor(taskName);
        if (disabled.isEmpty()) {
            return middleware;
        }
        return middleware.stream().filter(m -> !disabled.contains(nameOf(m))).toList();
    }
}
