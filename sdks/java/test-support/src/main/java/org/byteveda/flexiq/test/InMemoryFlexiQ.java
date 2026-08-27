package org.byteveda.flexiq.test;

import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.serialization.Serializer;

/**
 * Opens a {@link FlexiQ} backed by an {@link InMemoryQueueBackend} — no JNI, no
 * disk. Intended for fast unit tests of producers, handlers, retries, and
 * dead-lettering. Workflows are not supported in-memory.
 *
 * <h2>Durable steps</h2>
 *
 * {@code JobContext.current().step()} works here, and works as a <b>sequence</b>
 * rather than as a bypass: committed steps are recorded per job, a replay is
 * answered from that record, a changed sequence diverges with the same permanent
 * verdict a worker gives it, and {@code step.sleep} ends the attempt and
 * reschedules the job. A task that passes here is a task a worker runs the same
 * way — which is the only reason step support is worth having in a harness at
 * all.
 *
 * <p>The rules behind that — how a step is named, when an occurrence is spent,
 * what counts as a divergence, the size caps — live in the Rust core, and this
 * backend cannot ask it: being free of the native library is what makes it fast.
 * It restates them in Java instead, and a parity suite runs one task body over
 * this backend and over a real worker to keep the restatement honest.
 *
 * <p>What that buys is behavioural fidelity, not a database. Specifically, this
 * backend:
 *
 * <ul>
 *   <li>is one process, so the {@code (owner, attempt)} fence is a field check
 *       rather than a row condition — it refuses a write from a worker that does
 *       not hold the claim, but nothing here is a transaction;
 *   <li>drops a job's steps when the job finishes, rather than retaining them
 *       for inspection;
 *   <li>enforces the core's <i>default</i> caps (256 KiB per step, 4 MiB per
 *       job, 1000 steps) and does not read a queue's configuration for them.
 * </ul>
 *
 * <p>For anything that turns on real storage — retention, a reaper reclaiming a
 * dead worker's job, two processes racing — run the task against a file-backed
 * queue instead:
 *
 * <pre>{@code
 * FlexiQ queue = FlexiQ.builder().sqlite(tempDir + "/steps.db").open();
 * }</pre>
 */
public final class InMemoryFlexiQ {
    private InMemoryFlexiQ() {}

    /**
     * A queue over a fresh in-memory backend using the default JSON serializer.
     *
     * @return an open queue; close it to stop its workers
     */
    public static FlexiQ open() {
        return FlexiQ.builder().open(new InMemoryQueueBackend());
    }

    /**
     * A queue over a fresh in-memory backend with a custom serializer.
     *
     * @param serializer the serializer payloads and results are encoded with
     * @return an open queue; close it to stop its workers
     */
    public static FlexiQ open(Serializer serializer) {
        return FlexiQ.builder().serializer(serializer).open(new InMemoryQueueBackend());
    }
}
