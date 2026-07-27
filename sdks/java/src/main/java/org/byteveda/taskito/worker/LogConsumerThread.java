package org.byteveda.taskito.worker;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.byteveda.taskito.logging.TaskitoLogger;
import org.byteveda.taskito.model.TopicMessage;
import org.byteveda.taskito.pubsub.LogConsumerConfig;
import org.byteveda.taskito.serialization.Serializer;
import org.jspecify.annotations.Nullable;

/**
 * Daemon thread driving one managed log consumer. Each loop reads a batch after the
 * cursor, decodes and invokes the handler per message, then advances the cursor to
 * the last successfully handled id — the {@code readTopic}/{@code ackTopic} loop a
 * caller would otherwise hand-write. A non-empty batch loops immediately to drain a
 * backlog; an empty one waits {@code pollIntervalMs} (interruptibly) before re-reading.
 */
final class LogConsumerThread extends Thread {
    private static final TaskitoLogger LOG = TaskitoLogger.create("worker");

    private final LogTopicReader reader;
    private final Serializer serializer;
    private final LogConsumerConfig config;
    // Doubles as the stop flag (count 0 = stop) and the interruptible inter-poll wait.
    private final CountDownLatch stop = new CountDownLatch(1);

    LogConsumerThread(LogTopicReader reader, Serializer serializer, LogConsumerConfig config) {
        super("taskito-log-consumer-" + config.name());
        setDaemon(true);
        this.reader = reader;
        this.serializer = serializer;
        this.config = config;
    }

    /** Signal the loop to stop after its in-flight batch; wakes an idle poll wait. */
    void shutdown() {
        stop.countDown();
    }

    @Override
    public void run() {
        while (stop.getCount() > 0) {
            List<TopicMessage> messages;
            try {
                messages = reader.readTopic(config.topic(), config.name(), config.batchSize());
            } catch (RuntimeException e) {
                LOG.warn(label() + ": read failed", e);
                if (waitPoll()) {
                    return;
                }
                continue;
            }
            if (messages.isEmpty()) {
                if (waitPoll()) {
                    return;
                }
                continue;
            }
            DrainResult result = drainBatch(messages);
            ackDrained(result.lastAcked());
            // A retry-mode failure made no progress past the poison message, so
            // wait one interval before re-reading rather than hot-looping on it.
            if (result.retryFailure() && waitPoll()) {
                return;
            }
        }
    }

    /** What a batch drain achieved: the id to ack up to, and whether a retry-mode
     *  handler failure blocked the cursor (so the caller backs off). */
    private record DrainResult(@Nullable String lastAcked, boolean retryFailure) {}

    /**
     * Invoke the handler per message. {@code retry} stops at the first failure and
     * acks only the run of successes before it (and reports {@code retryFailure}), so
     * the failed message re-reads next poll; {@code skip} acks past a failure too and
     * continues. {@code lastAcked} is null when nothing succeeded.
     */
    private DrainResult drainBatch(List<TopicMessage> messages) {
        @Nullable String lastAcked = null;
        for (TopicMessage message : messages) {
            if (stop.getCount() == 0) {
                break;
            }
            try {
                config.handler().accept(serializer.deserializeCall(message.payload, config.payloadType()));
            } catch (RuntimeException e) {
                LOG.warn(label() + ": handler failed on message " + message.id, e);
                if ("retry".equals(config.onError())) {
                    return new DrainResult(lastAcked, true);
                }
            }
            lastAcked = message.id;
        }
        return new DrainResult(lastAcked, false);
    }

    /** Advance the cursor to {@code lastAcked}; a null id (nothing handled) is a no-op. */
    private void ackDrained(@Nullable String lastAcked) {
        if (lastAcked == null) {
            return;
        }
        try {
            reader.ackTopic(config.topic(), config.name(), lastAcked);
        } catch (RuntimeException e) {
            LOG.warn(label() + ": ack failed", e);
        }
    }

    /** Wait one poll interval; return true when a stop was signaled meanwhile. */
    private boolean waitPoll() {
        try {
            return stop.await(config.pollIntervalMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    private String label() {
        return "log consumer " + config.topic() + "/" + config.name();
    }
}
