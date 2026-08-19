package org.byteveda.flexiq.worker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import org.byteveda.flexiq.errors.DuplicateTaskException;
import org.byteveda.flexiq.errors.TaskDiscoveryException;

/**
 * Loads the handlers the {@code @TaskHandler} processor listed in
 * {@code META-INF/services}, shared by {@link Worker.Builder#discover()} and
 * {@link Executor.Builder#discover()}.
 *
 * <p>Nothing here scans packages or reflects over arbitrary classes: the set of
 * handlers was fixed at compile time, which is what keeps discovery working under
 * GraalVM native-image.
 */
final class HandlerDiscovery {

    private HandlerDiscovery() {}

    /**
     * The loader {@code discover()} defaults to — the thread context loader, falling
     * back to this class's own when a host leaves it unset (a bare
     * {@code ServiceLoader.load(service, null)} would search the bootstrap loader
     * and find nothing).
     */
    static ClassLoader contextLoader() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return loader != null ? loader : HandlerDiscovery.class.getClassLoader();
    }

    /**
     * Discovered handlers, refusing any name in {@code taken} — nothing is returned
     * unless every one of them can be registered, so a clash leaves the builder as
     * it was rather than half-discovered.
     *
     * @param target what the caller registers into, for the message ("worker")
     */
    static List<Handler<?, ?>> load(ClassLoader loader, Set<String> taken, String target) {
        List<Handler<?, ?>> handlers = load(loader);
        for (Handler<?, ?> handler : handlers) {
            String name = handler.task().name();
            if (taken.contains(name)) {
                throw new DuplicateTaskException("a handler for \"" + name + "\" is already registered on this "
                        + target + ", and discovery does not replace one"
                        + " — register it after discover() to override it deliberately");
            }
        }
        return handlers;
    }

    /**
     * Every handler contributed by a {@link HandlerRegistryProvider} visible to
     * {@code loader}, in classpath order.
     *
     * @throws TaskDiscoveryException a provider could not be loaded, or threw while
     *     building its handlers — never skipped, because a worker missing a handler
     *     dead-letters that task's jobs instead of failing visibly
     * @throws DuplicateTaskException two providers claim one task name
     */
    static List<Handler<?, ?>> load(ClassLoader loader) {
        List<Handler<?, ?>> handlers = new ArrayList<>();
        // Which provider claimed each name, so a clash can name both sides.
        Map<String, String> claimedBy = new LinkedHashMap<>();
        // ServiceLoader raises a ServiceConfigurationError from the iteration itself
        // for an entry it cannot resolve or instantiate, so the whole walk is covered
        // rather than each provider call.
        try {
            for (HandlerRegistryProvider provider : ServiceLoader.load(HandlerRegistryProvider.class, loader)) {
                collect(provider, handlers, claimedBy);
            }
        } catch (ServiceConfigurationError e) {
            throw new TaskDiscoveryException("failed to load a handler provider from the classpath: " + e, e);
        }
        return handlers;
    }

    private static void collect(
            HandlerRegistryProvider provider, List<Handler<?, ?>> handlers, Map<String, String> claimedBy) {
        String origin = provider.getClass().getName();
        HandlerRegistry registry;
        try {
            registry = provider.registry();
        } catch (RuntimeException e) {
            throw new TaskDiscoveryException("handler provider " + origin + " failed to build its handlers", e);
        }
        for (Handler<?, ?> handler : registry.handlers()) {
            String name = handler.task().name();
            String previous = claimedBy.putIfAbsent(name, origin);
            if (previous != null) {
                throw new DuplicateTaskException("two handler providers claim the task \"" + name + "\": " + previous
                        + " and " + origin + " — rename one with @TaskHandler(\"...\"),"
                        + " or keep one of them off the classpath");
            }
            handlers.add(handler);
        }
    }
}
