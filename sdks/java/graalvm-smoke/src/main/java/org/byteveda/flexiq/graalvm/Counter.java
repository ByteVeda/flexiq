package org.byteveda.flexiq.graalvm;

import java.util.List;
import org.byteveda.flexiq.annotation.TaskHandler;

/**
 * A handler the smoke never names when it builds the worker.
 *
 * <p>The processor turns this into a {@code CounterTasks} companion and lists its
 * provider in {@code META-INF/services}, so {@code discover()} finds it. That is
 * the whole reason the JVM SDK registers tasks at compile time rather than by
 * scanning the classpath — a scan cannot survive native-image, and this class
 * existing in the compiled binary is the proof that the alternative does.
 *
 * <p>The payload is generic on purpose: the generated companion describes it with
 * an anonymous {@code TypeReference}, whose type argument is only recoverable
 * through the generic signature, so this exercises the path a plain
 * {@code Task.of(name, String.class)} would skip.
 */
public class Counter {

    /** Constructs a handler instance; the generated provider calls this reflectively. */
    public Counter() {}

    /**
     * Sums the payload, the handler the generated companion registers.
     *
     * @param numbers the task payload
     * @return the sum of the payload
     */
    @TaskHandler("discovered")
    public Integer total(List<Integer> numbers) {
        return numbers.stream().mapToInt(Integer::intValue).sum();
    }
}
