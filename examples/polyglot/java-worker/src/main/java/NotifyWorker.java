import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.serialization.CborSerializer;
import org.byteveda.taskito.worker.Worker;

/**
 * Runs `orders.notify` jobs enqueued by a worker in another language.
 *
 * <p>Nothing here is aware of the producer or the upstream worker. The task name
 * and the CBOR wire format are the entire contract between the three runtimes.
 */
public final class NotifyWorker {

    public static void main(String[] args) throws Exception {
        String db = System.getenv().getOrDefault("TASKITO_DB", "../taskito.db");

        // Each SDK's own default serializer is same-language-only. CBOR is the
        // cross-SDK format, and every runtime here opts into it explicitly.
        try (Taskito taskito = Taskito.builder()
                        .sqlite(db)
                        .serializer(new CborSerializer())
                        .open();
                Worker worker = taskito.worker()
                        .handle("orders.notify", Map.class, NotifyWorker::notifyCustomer)
                        // Poll only this stage's queue. A worker claims whatever is in
                        // the queues it polls, so sharing one queue would let this
                        // process pick up jobs it has no handler for.
                        .queues("notify")
                        .concurrency(2)
                        .start()) {

            System.out.println("[java] worker running against " + db + "; ctrl-c to stop");
            CountDownLatch stop = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));
            stop.await();
            System.out.println("\n[java] stopping");
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
