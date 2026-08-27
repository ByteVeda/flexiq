package org.byteveda.flexiq.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.byteveda.flexiq.events.Emitter;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.FlexiQEvent;
import org.byteveda.flexiq.events.WorkerEvent;
import org.byteveda.flexiq.internal.JniExecutorControl;
import org.byteveda.flexiq.internal.NativeExecutor;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.resources.ResourceRuntime;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.serialization.PayloadCodec;
import org.byteveda.flexiq.serialization.Serializer;
import org.jspecify.annotations.Nullable;

/**
 * Runs tasks for a detached scheduler instead of polling storage.
 *
 * <p>The inverse of {@link Worker}: the scheduler holds the database connection
 * and dispatches jobs over a socket, so this process runs task bodies without
 * any database credentials of its own. Handlers, middleware, codecs and the
 * cancel signal behave exactly as they do in a worker — only the transport
 * differs.
 *
 * <pre>{@code
 * try (Executor executor = Executor.builder()
 *         .discover()                 // handlers from META-INF/services
 *         .attach("scheduler:7777")
 *         .slots(4)
 *         .start()) {
 *     executor.awaitSession();        // until the scheduler shuts down
 * }
 * }</pre>
 */
public final class Executor implements AutoCloseable {
    private static final FlexiQLogger LOG = FlexiQLogger.create("executor");
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final JniExecutorControl control;
    private final ExecutorService handlerPool;
    private final ResourceRuntime resources;
    private final Emitter emitter;
    private boolean closed;

    private Executor(
            JniExecutorControl control, ExecutorService handlerPool, ResourceRuntime resources, Emitter emitter) {
        this.control = control;
        this.handlerPool = handlerPool;
        this.resources = resources;
        this.emitter = emitter;
    }

    /**
     * A builder for an executor. Register handlers, point it at a scheduler, start.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Identity the scheduler announced when it accepted this attach.
     *
     * @return the scheduler's id
     */
    public String schedulerId() {
        return control.schedulerId();
    }

    /**
     * Identity this executor attached under.
     *
     * @return the id the scheduler knows this executor by
     */
    public String executorId() {
        return control.executorId();
    }

    /**
     * Peer label of the scheduler connection.
     *
     * @return the label, for logs and diagnostics
     */
    public String peer() {
        return control.peer();
    }

    /**
     * Whether this executor is still accepting work.
     *
     * @return {@code false} once the session ended or {@code stop()} was called
     */
    public boolean isRunning() {
        return control.isRunning();
    }

    /**
     * Block until this executor stops accepting work — the scheduler ending the
     * session, or a local {@link #stop()}. Does not drain; {@link #close()} does.
     */
    public void awaitSession() {
        control.awaitSession();
    }

    /**
     * Ask the scheduler to stop sending work and finish what is in flight.
     * Returns at once, so it is safe from a shutdown hook.
     */
    public void stop() {
        control.stop();
    }

    /**
     * Drain in-flight work, disconnect, and release handler threads.
     *
     * <p>The handler pool is drained before the native handle is freed: a handler
     * may still be completing its job through {@code control}, and the handle has
     * to outlive every such call. Idempotent.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        control.stop();
        handlerPool.shutdown(); // stop accepting; let running handlers finish
        try {
            if (!handlerPool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                handlerPool.shutdownNow();
                if (!handlerPool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("handler threads still running after " + (2 * SHUTDOWN_TIMEOUT_SECONDS)
                            + "s; closing the executor handle anyway");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            control.close();
        } finally {
            resources.teardownWorker();
            emitter.emit(new WorkerEvent(EventName.WORKER_STOPPED, List.of()));
        }
    }

    /** Registers handlers and attach options, then starts the executor. */
    public static final class Builder {
        private static final ObjectMapper JSON = new ObjectMapper();

        private final Map<String, RegisteredTask> handlers = new HashMap<>();
        private final Map<EventName, List<Consumer<FlexiQEvent>>> listeners = new LinkedHashMap<>();
        private Serializer serializer = new JsonSerializer();
        private List<Middleware> middleware = List.of();
        private Map<String, PayloadCodec> codecs = Map.of();
        private ResourceRuntime resources = new ResourceRuntime();
        private @Nullable String address;
        private @Nullable String token;
        private @Nullable String executorId;
        private int slots = 1;
        private @Nullable Long connectTimeoutMs;
        private @Nullable Long heartbeatIntervalMs;
        private @Nullable Long shutdownDrainMs;

        private Builder() {}

        /**
         * Register every handler discoverable on the classpath.
         *
         * <p>The {@code @TaskHandler} processor lists a provider per annotated
         * class in {@code META-INF/services}, so this needs no application code
         * at all. A class the executor cannot construct is skipped at build time
         * with a compiler note — register those explicitly.
         *
         * <p>Discovery never replaces a handler already registered on this builder;
         * call {@code register(...)} <em>after</em> it to override one deliberately.
         *
         * @return {@code this}, for chaining
         */
        public Builder discover() {
            return discover(HandlerDiscovery.contextLoader());
        }

        /**
         * {@link #discover()} against a specific class loader.
         *
         * @param loader where to look for {@code META-INF/services} entries
         * @return {@code this}, for chaining
         */
        public Builder discover(ClassLoader loader) {
            HandlerDiscovery.load(loader, handlers.keySet(), "executor").forEach(this::register);
            return this;
        }

        /**
         * Register a generated {@code <Class>Tasks.handlers(impl)} bundle.
         *
         * @param registry the bundle whose handlers to take on
         * @return {@code this}, for chaining
         */
        public Builder register(HandlerRegistry registry) {
            registry.handlers().forEach(this::register);
            return this;
        }

        /**
         * Register one handler.
         *
         * @param handler the task descriptor paired with the code that runs it
         * @return {@code this}, for chaining
         */
        public Builder register(Handler<?, ?> handler) {
            handlers.put(
                    handler.task().name(),
                    new RegisteredTask(
                            handler.task().payloadType(),
                            cast(handler.function()),
                            handler.task().codecNames(),
                            handler.task().retryOn()));
            return this;
        }

        /**
         * Scheduler address: {@code host:port}, {@code :port}, or {@code unix:/path}.
         *
         * @param address where the scheduler is listening
         * @return {@code this}, for chaining
         */
        public Builder attach(String address) {
            this.address = address;
            return this;
        }

        /**
         * Jobs to run at once (default 1).
         *
         * @param slots the concurrency; values below 1 are raised to 1
         * @return {@code this}, for chaining
         */
        public Builder slots(int slots) {
            this.slots = Math.max(1, slots);
            return this;
        }

        /**
         * Shared secret, when the scheduler requires one.
         *
         * @param token the secret, or {@code null} when the scheduler needs none
         * @return {@code this}, for chaining
         */
        public Builder token(@Nullable String token) {
            this.token = token;
            return this;
        }

        /**
         * Identity announced to the scheduler (default: generated per process).
         *
         * @param executorId the id, or {@code null} to generate one
         * @return {@code this}, for chaining
         */
        public Builder executorId(@Nullable String executorId) {
            this.executorId = executorId;
            return this;
        }

        /**
         * How long to wait for the connection (default 10000ms).
         *
         * @param millis the connect timeout in milliseconds
         * @return {@code this}, for chaining
         */
        public Builder connectTimeoutMs(long millis) {
            this.connectTimeoutMs = millis;
            return this;
        }

        /**
         * How often to send a liveness heartbeat (default 5000ms).
         *
         * @param millis the interval in milliseconds
         * @return {@code this}, for chaining
         */
        public Builder heartbeatIntervalMs(long millis) {
            this.heartbeatIntervalMs = millis;
            return this;
        }

        /**
         * How long a drain waits for in-flight jobs (default 30000ms).
         *
         * @param millis the drain window in milliseconds
         * @return {@code this}, for chaining
         */
        public Builder shutdownDrainMs(long millis) {
            this.shutdownDrainMs = millis;
            return this;
        }

        /**
         * Payload serializer (default JSON). Must match the enqueuing side.
         *
         * @param serializer what decodes payloads and encodes results
         * @return {@code this}, for chaining
         */
        public Builder serializer(Serializer serializer) {
            this.serializer = serializer;
            return this;
        }

        /**
         * Middleware applied around every handler.
         *
         * @param middleware the chain, in the order it wraps the handler
         * @return {@code this}, for chaining
         */
        public Builder middleware(List<Middleware> middleware) {
            this.middleware = List.copyOf(middleware);
            return this;
        }

        /**
         * Named payload codecs, for tasks declaring {@code codecs}.
         *
         * @param codecs the codecs by name, resolved against each task's declared list
         * @return {@code this}, for chaining
         */
        public Builder codecs(Map<String, PayloadCodec> codecs) {
            this.codecs = Map.copyOf(codecs);
            return this;
        }

        /**
         * Injectable resources available to handlers.
         *
         * @param resources the runtime handlers resolve {@code Resources.use(...)} against
         * @return {@code this}, for chaining
         */
        public Builder resources(ResourceRuntime resources) {
            this.resources = resources;
            return this;
        }

        /**
         * Subscribe to a worker lifecycle or job event.
         *
         * @param name the event to subscribe to
         * @param listener called on the emitting thread
         * @return {@code this}, for chaining
         */
        public Builder on(EventName name, Consumer<FlexiQEvent> listener) {
            listeners.computeIfAbsent(name, key -> new ArrayList<>()).add(listener);
            return this;
        }

        /**
         * Task names this executor will advertise.
         *
         * @return the registered names; the scheduler dispatches nothing outside them
         */
        public List<String> tasks() {
            return List.copyOf(handlers.keySet());
        }

        /**
         * Dial the scheduler and start running jobs.
         *
         * <p>The handshake happens here, so a bad token or an unreachable
         * scheduler throws before any handler thread exists.
         *
         * @return the running executor, to be closed when the process stops
         */
        public Executor start() {
            if (address == null || address.isBlank()) {
                throw new IllegalStateException(
                        "no scheduler address: call attach(...) or set FLEXIQ_ATTACH (e.g. scheduler:7777)");
            }
            if (handlers.isEmpty()) {
                // The scheduler only dispatches task names an executor
                // advertises, so this would attach and then sit idle forever.
                throw new IllegalStateException("no handlers registered: call discover() or register(...)");
            }

            ExecutorService pool = Executors.newFixedThreadPool(slots);
            Emitter emitter = new Emitter();
            listeners.forEach((name, bound) -> bound.forEach(listener -> emitter.onEvent(name, listener)));

            // No QueueBackend: an executor reads no storage, which is the point
            // of the split. Job metadata and dashboard middleware toggles are
            // storage-backed and so unavailable here.
            WorkerDispatchBridge bridge =
                    new WorkerDispatchBridge(null, handlers, serializer, pool, emitter, middleware, resources, codecs);

            long handle;
            try {
                handle = NativeExecutor.attach(bridge, encodeOptions());
            } catch (RuntimeException e) {
                pool.shutdownNow();
                throw e;
            }
            JniExecutorControl control = new JniExecutorControl(handle);
            try {
                bridge.bind(control);
                emitter.emit(new WorkerEvent(EventName.WORKER_STARTED, List.of()));
                // Lease worker resources only after the attach succeeded, so a
                // refused handshake leaks nothing.
                resources.acquireWorker();
            } catch (RuntimeException e) {
                // The attach already succeeded, so the socket, the reader thread
                // and the native handle are live; a throwing listener or
                // resource factory must not strand them for the process's life.
                control.close();
                pool.shutdownNow();
                throw e;
            }
            return new Executor(control, pool, resources, emitter);
        }

        /** The attach options, as the JSON the native side parses. */
        private String encodeOptions() {
            Map<String, Object> options = new LinkedHashMap<>();
            options.put("address", address);
            options.put("tasks", tasks());
            options.put("slots", slots);
            if (token != null) {
                options.put("token", token);
            }
            if (executorId != null) {
                options.put("executorId", executorId);
            }
            if (connectTimeoutMs != null) {
                options.put("connectTimeoutMs", connectTimeoutMs);
            }
            if (heartbeatIntervalMs != null) {
                options.put("heartbeatIntervalMs", heartbeatIntervalMs);
            }
            if (shutdownDrainMs != null) {
                options.put("shutdownDrainMs", shutdownDrainMs);
            }
            try {
                return JSON.writeValueAsString(options);
            } catch (Exception e) {
                throw new IllegalStateException("failed to encode executor options", e);
            }
        }

        @SuppressWarnings("unchecked")
        private static org.byteveda.flexiq.task.TaskFunction<Object, Object> cast(Object function) {
            return (org.byteveda.flexiq.task.TaskFunction<Object, Object>) function;
        }
    }
}
