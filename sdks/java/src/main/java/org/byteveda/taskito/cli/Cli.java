package org.byteveda.taskito.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.dashboard.DashboardServer;
import org.byteveda.taskito.model.DeadJob;
import org.byteveda.taskito.model.Job;
import org.byteveda.taskito.model.JobFilter;
import org.byteveda.taskito.model.JobStatus;
import org.jspecify.annotations.Nullable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/** Command-line interface over a Taskito queue. */
@Command(
        name = "taskito",
        mixinStandardHelpOptions = true,
        subcommands = {
            Cli.Stats.class,
            Cli.Enqueue.class,
            Cli.Jobs.class,
            Cli.Migrate.class,
            Cli.Cancel.class,
            Cli.Pause.class,
            Cli.Resume.class,
            Cli.Dlq.class,
            Cli.Dashboard.class,
            Cli.Executor.class
        })
public final class Cli {
    static final ObjectMapper JSON = new ObjectMapper();

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Option(names = "--backend", description = "Storage backend (default sqlite).", defaultValue = "sqlite")
    String backend;

    @Option(names = "--url", description = "Connection string (SQLite path or URL); defaults to .flexiq/flexiq.db.")
    @Nullable
    String url;

    Taskito open() {
        return open(true);
    }

    Taskito open(boolean autoMigrate) {
        // Only SQLite has a sensible default store; every other backend needs a URL.
        if (url == null && !"sqlite".equalsIgnoreCase(backend)) {
            throw new CommandLine.ParameterException(
                    spec.commandLine(), "--url is required for the '" + backend + "' backend");
        }
        Taskito.Builder builder = Taskito.builder().backend(backend).autoMigrate(autoMigrate);
        if (url != null) {
            builder.url(url);
        }
        return builder.open();
    }

    static String json(Object value) {
        try {
            return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));
    }

    @Command(name = "stats", description = "Show job counts by status.")
    static final class Stats implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Override
        public Integer call() {
            try (Taskito queue = parent.open()) {
                System.out.println(json(queue.stats()));
            }
            return 0;
        }
    }

    @Command(name = "migrate", description = "Apply pending schema changes (for a deployment that gates DDL).")
    static final class Migrate implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Override
        public Integer call() {
            // Opened unmigrated on purpose: this command is the one path
            // allowed to apply DDL, so opening must not do it first.
            try (Taskito queue = parent.open(false)) {
                System.out.println(json(queue.migrate()));
            }
            return 0;
        }
    }

    @Command(name = "enqueue", description = "Enqueue a task with a JSON payload.")
    static final class Enqueue implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Parameters(index = "0", description = "Task name.")
        String task;

        @Parameters(index = "1", arity = "0..1", description = "JSON payload (default null).")
        @Nullable
        String payload;

        @Override
        public Integer call() throws Exception {
            Object value = payload == null ? null : JSON.readValue(payload, Object.class);
            try (Taskito queue = parent.open()) {
                System.out.println(queue.enqueue(task, value));
            }
            return 0;
        }
    }

    @Command(name = "jobs", description = "List jobs.")
    static final class Jobs implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Option(names = "--status", description = "Filter by status.")
        @Nullable
        String status;

        @Option(names = "--queue", description = "Filter by queue.")
        @Nullable
        String queue;

        @Option(names = "--limit", defaultValue = "50")
        int limit;

        @Override
        public Integer call() {
            JobFilter.Builder filter = JobFilter.builder().limit(limit);
            if (status != null) {
                filter.status(JobStatus.fromWire(status));
            }
            if (queue != null) {
                filter.queue(queue);
            }
            try (Taskito q = parent.open()) {
                List<Job> jobs = q.listJobs(filter.build());
                System.out.println(json(jobs));
            }
            return 0;
        }
    }

    @Command(name = "cancel", description = "Cancel a pending job.")
    static final class Cancel implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Parameters(index = "0", description = "Job id.")
        String id;

        @Override
        public Integer call() {
            try (Taskito queue = parent.open()) {
                return queue.cancel(id) ? 0 : 1;
            }
        }
    }

    @Command(name = "pause", description = "Pause a queue.")
    static final class Pause implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Parameters(index = "0", description = "Queue name.")
        String queue;

        @Override
        public Integer call() {
            try (Taskito q = parent.open()) {
                q.queue(queue).pause();
            }
            return 0;
        }
    }

    @Command(name = "resume", description = "Resume a queue.")
    static final class Resume implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Parameters(index = "0", description = "Queue name.")
        String queue;

        @Override
        public Integer call() {
            try (Taskito q = parent.open()) {
                q.queue(queue).resume();
            }
            return 0;
        }
    }

    @Command(
            name = "dlq",
            description = "Dead-letter operations.",
            subcommands = {Dlq.ListDead.class, Dlq.Retry.class, Dlq.Delete.class})
    static final class Dlq {
        @ParentCommand
        Cli parent;

        Taskito open() {
            return parent.open();
        }

        @Command(name = "list", description = "List dead-letter entries.")
        static final class ListDead implements Callable<Integer> {
            @ParentCommand
            Dlq dlq;

            @Option(names = "--limit", defaultValue = "50")
            long limit;

            @Override
            public Integer call() {
                try (Taskito queue = dlq.open()) {
                    List<DeadJob> dead = queue.listDead(limit, 0);
                    System.out.println(json(dead));
                }
                return 0;
            }
        }

        @Command(name = "retry", description = "Re-enqueue a dead-letter entry.")
        static final class Retry implements Callable<Integer> {
            @ParentCommand
            Dlq dlq;

            @Parameters(index = "0", description = "Dead-letter id.")
            String id;

            @Override
            public Integer call() {
                try (Taskito queue = dlq.open()) {
                    System.out.println(queue.retryDead(id));
                }
                return 0;
            }
        }

        @Command(name = "delete", description = "Delete a dead-letter entry.")
        static final class Delete implements Callable<Integer> {
            @ParentCommand
            Dlq dlq;

            @Parameters(index = "0", description = "Dead-letter id.")
            String id;

            @Override
            public Integer call() {
                try (Taskito queue = dlq.open()) {
                    return queue.deleteDead(id) ? 0 : 1;
                }
            }
        }
    }

    @Command(name = "executor", description = "Run tasks for a detached scheduler instead of polling storage.")
    static final class Executor implements Callable<Integer> {
        /** How long a shutdown hook waits for the drain before letting the JVM go. */
        private static final int DRAIN_WAIT_SECONDS = 40;

        @CommandLine.Spec
        CommandLine.Model.CommandSpec spec;

        @Option(
                names = "--attach",
                description = "Scheduler address: host:port, :port, or unix:/path (env: FLEXIQ_ATTACH).")
        @Nullable
        String attach;

        @Option(names = "--slots", description = "Jobs to run concurrently (env: FLEXIQ_SLOTS).")
        @Nullable
        Integer slots;

        @Option(names = "--executor-id", description = "Identity announced to the scheduler.")
        @Nullable
        String executorId;

        @Override
        public Integer call() throws Exception {
            String address = attach != null ? attach : System.getenv("FLEXIQ_ATTACH");
            if (address == null || address.isBlank()) {
                System.err.println("--attach is required (or set FLEXIQ_ATTACH), e.g. --attach scheduler:7777");
                return 1;
            }

            int slotCount = resolveSlots();
            org.byteveda.taskito.worker.Executor.Builder builder = org.byteveda.taskito.worker.Executor.builder()
                    // Handlers come from META-INF/services, so no application
                    // code has to run to register them.
                    .discover()
                    .attach(address)
                    .slots(slotCount)
                    // Env only, never a flag: a token in argv is visible in `ps`
                    // output and lands in shell history.
                    .token(envOrNull("FLEXIQ_ATTACH_TOKEN"))
                    .executorId(executorId);

            if (builder.tasks().isEmpty()) {
                System.err.println("no handlers found on the classpath. Annotate methods with @TaskHandler and "
                        + "make sure the annotation processor ran, or register them from your own main.");
                return 1;
            }

            try (org.byteveda.taskito.worker.Executor executor = builder.start()) {
                System.out.printf(
                        "taskito executor %s attached to %s at %s (%d slot(s), %d task(s)) — Ctrl-C to stop%n",
                        executor.executorId(),
                        executor.schedulerId(),
                        executor.peer(),
                        slotCount,
                        builder.tasks().size());
                // A shutdown hook does not hold the JVM open, so stopping alone
                // would let the process exit while the drain was still running
                // and strand in-flight jobs for the reaper. Signal, then wait
                // for the main thread to finish close().
                Thread main = Thread.currentThread();
                Thread hook = new Thread(() -> {
                    executor.stop();
                    try {
                        main.join(TimeUnit.SECONDS.toMillis(DRAIN_WAIT_SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                Runtime.getRuntime().addShutdownHook(hook);
                try {
                    executor.awaitSession();
                } finally {
                    // Dropped before the normal exit path reaches it. `main`
                    // calls `System.exit`, which runs hooks while the main
                    // thread is still inside `Runtime.exit` — so a hook left
                    // registered would wait out the whole drain budget for a
                    // join that cannot complete, then call `stop()` on an
                    // already-closed control.
                    dropHook(hook);
                }
            }
            System.out.println("taskito executor detached");
            return 0;
        }

        /**
         * Unregister the drain hook, tolerating the one case that cannot: the
         * JVM is already shutting down, which means the hook is running and has
         * the drain in hand.
         */
        private static void dropHook(Thread hook) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException alreadyShuttingDown) {
                // Nothing to undo — the hook is doing its job right now.
            }
        }

        /**
         * An environment value, or null when unset <em>or blank</em>. Compose
         * files and container runtimes set empty strings freely, and an empty
         * token must read as "no token" rather than be presented as one.
         */
        private static @Nullable String envOrNull(String name) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? null : value;
        }

        /** Slots from the flag, then the env, then one. */
        private int resolveSlots() {
            if (slots != null) {
                return atLeastOne(slots, "--slots");
            }
            String raw = System.getenv("FLEXIQ_SLOTS");
            if (raw == null || raw.isBlank()) {
                return 1;
            }
            int parsed;
            try {
                parsed = Integer.parseInt(raw.trim());
            } catch (NumberFormatException e) {
                throw new CommandLine.ParameterException(
                        spec.commandLine(), "FLEXIQ_SLOTS must be an integer, got '" + raw + "'");
            }
            return atLeastOne(parsed, "FLEXIQ_SLOTS");
        }

        /**
         * Reject a count the executor would silently clamp, so the banner cannot
         * announce a concurrency the executor does not run with.
         */
        private int atLeastOne(int value, String source) {
            if (value < 1) {
                throw new CommandLine.ParameterException(
                        spec.commandLine(), source + " must be at least 1, got " + value);
            }
            return value;
        }
    }

    @Command(name = "dashboard", description = "Serve the dashboard until interrupted.")
    static final class Dashboard implements Callable<Integer> {
        @ParentCommand
        Cli parent;

        @Option(names = "--port", defaultValue = "8080")
        int port;

        @Option(names = "--auth", description = "Enable session authentication (off by default).")
        boolean auth;

        @Option(names = "--token", description = "Require this token for API access.")
        @Nullable
        String token;

        @Option(names = "--static", description = "Directory of the prebuilt SPA.")
        @Nullable
        String staticDir;

        @Option(
                names = "--insecure-cookies",
                description = "Drop the Secure cookie attribute (for local HTTP development).")
        boolean insecureCookies;

        @Override
        public Integer call() throws Exception {
            try (Taskito queue = parent.open();
                    DashboardServer server =
                            DashboardServer.start(queue, port, token, staticDir, !insecureCookies, auth)) {
                System.out.println("dashboard on http://localhost:" + server.port());
                new CountDownLatch(1).await();
            }
            return 0;
        }
    }
}
