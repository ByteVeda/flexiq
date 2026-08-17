import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.serialization.CborSerializer;
import org.byteveda.flexiq.worker.Worker;

/**
 * Runs `orders.notify` jobs enqueued by a worker in another language.
 *
 * <p>Nothing here is aware of the producer or the upstream worker. The task name
 * and the CBOR wire format are the entire contract between the three runtimes.
 */
public final class NotifyWorker {

    public static void main(String[] args) throws Exception {
        String db = System.getenv().getOrDefault("FLEXIQ_DB", "../flexiq.db");

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
        try (FlexiQ flexiq = FlexiQ.builder()
                        .sqlite(db)
                        .serializer(new CborSerializer())
                        .open();
                Worker worker = flexiq.worker()
                        .handle("orders.notify", Map.class, NotifyWorker::notifyCustomer)
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

    private static Map<String, Object> notifyCustomer(Map<?, ?> notification) {
        System.out.printf(
                "[java] notifying %s about %s — %s %s (processed by %s)%n",
                notification.get("customer"),
                notification.get("order_id"),
                notification.get("total"),
                notification.get("currency"),
                notification.get("processed_by"));
        return Map.of("order_id", String.valueOf(notification.get("order_id")), "notified", true);
    }
}
