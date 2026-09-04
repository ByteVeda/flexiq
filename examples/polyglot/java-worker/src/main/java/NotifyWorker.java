import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.annotation.TaskHandler;
import org.byteveda.flexiq.serialization.CborSerializer;
import org.byteveda.flexiq.worker.Worker;

/**
 * Runs `orders.notify` jobs enqueued by a worker in another language.
 *
 * <p>Nothing here is aware of the producer or the upstream worker. The task name
 * and the CBOR wire format are the entire contract between the three runtimes.
 *
 * <p>Two deployments share this one class. {@code ./gradlew run} polls the
 * database itself, via {@code main()} below. {@code flexiq executor} discovers
 * {@link #notifyCustomer} through {@code META-INF/services} instead — the
 * {@code @TaskHandler} processor generates that registration, so nothing here
 * has to run for it to be found.
 */
public final class NotifyWorker {

    @TaskHandler("orders.notify")
    Map<String, Object> notifyCustomer(Map<String, Object> notification) {
        System.out.printf(
                "[java] notifying %s about %s — %s %s (processed by %s)%n",
                notification.get("customer"),
                notification.get("order_id"),
                notification.get("total"),
                notification.get("currency"),
                notification.get("processed_by"));
        return Map.of("order_id", String.valueOf(notification.get("order_id")), "notified", true);
    }

    public static void main(String[] args) throws Exception {
        String db = System.getenv().getOrDefault("FLEXIQ_DB", "../flexiq.db");
        // Set by the gRPC variant only: a job enqueued through the producer door
        // always carries its token's namespace, so a worker with no namespace
        // would never see it. Unset here, same as every process in the original
        // example.
        String namespace = System.getenv("FLEXIQ_NAMESPACE");

        CountDownLatch stop = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        // The JVM runs shutdown hooks concurrently with the main thread and does
        // not wait for it, so a hook that only signals `stop` would let the
        // process halt mid-teardown. Blocking on `closed` keeps the JVM alive
        // until try-with-resources has released the worker and the queue.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.countDown();
            try {
                closed.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }));

        // Each SDK's own default serializer is same-language-only. CBOR is the
        // cross-SDK format, and every runtime here opts into it explicitly.
        FlexiQ.Builder builder = FlexiQ.builder().sqlite(db).serializer(new CborSerializer());
        if (namespace != null) {
            builder = builder.namespace(namespace);
        }
        try (FlexiQ flexiq = builder.open();
                Worker worker = flexiq.worker()
                        .apply(b -> NotifyWorkerTasks.bind(b, new NotifyWorker()))
                        // Poll only this stage's queue. A worker claims whatever is in
                        // the queues it polls, so sharing one queue would let this
                        // process pick up jobs it has no handler for.
                        .queues("notify")
                        .concurrency(2)
                        .start()) {

            System.out.println("[java] worker running against " + db + "; ctrl-c to stop");
            stop.await();
            System.out.println("\n[java] stopping");
        } finally {
            closed.countDown();
        }
    }
}
