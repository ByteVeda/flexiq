package org.byteveda.flexiq.steps;

import org.jspecify.annotations.Nullable;

/**
 * Identity overrides for one step.
 *
 * <pre>{@code
 * ctx.step().run("charge", Charge.class, () -> charge(item), StepOptions.key(item.id()));
 * ctx.step().sleep(Duration.ofHours(1), StepOptions.named("cooldown"));
 * }</pre>
 */
public final class StepOptions {
    private static final StepOptions NONE = new StepOptions(null, null);

    private final @Nullable String name;
    private final @Nullable String key;

    private StepOptions(@Nullable String name, @Nullable String key) {
        this.name = name;
        this.key = key;
    }

    /**
     * No overrides: the step is identified by its name and its position.
     *
     * @return the shared empty instance
     */
    public static StepOptions none() {
        return NONE;
    }

    /**
     * Identify this step explicitly, rather than by where it sits in the
     * sequence.
     *
     * <p>Reach for it when the position can move — a loop over an unordered
     * collection. A keyed step is matched by key wherever it sits; an unkeyed
     * one is matched at its position, and keyed steps do not spend a position.
     *
     * @param key the step's identity, stable wherever the step moves to
     * @return options carrying that identity
     */
    public static StepOptions key(String key) {
        return new StepOptions(null, key);
    }

    /**
     * Name a sleep. Strongly recommended: a sequence that reads
     * {@code sleep#0, sleep#1, sleep#2} tells nobody which one diverged.
     *
     * <p>Only sleeps take a name here — {@code run}'s name is its first
     * argument, and passing one both ways is refused rather than silently
     * resolved.
     *
     * @param name what this sleep is waiting for, as it should read in the sequence
     * @return options carrying that name
     */
    public static StepOptions named(String name) {
        return new StepOptions(name, null);
    }

    /**
     * This, with {@code key} as its explicit identity.
     *
     * @param key the step's identity, stable wherever the step moves to
     * @return a copy carrying that key
     */
    public StepOptions withKey(String key) {
        return new StepOptions(name, key);
    }

    /**
     * This, with {@code name} as the sleep's name.
     *
     * @param name what this sleep is waiting for, as it should read in the sequence
     * @return a copy carrying that name
     */
    public StepOptions withName(String name) {
        return new StepOptions(name, key);
    }

    /**
     * The sleep name.
     *
     * @return the name, or {@code null} to fall back to {@code sleep#<position>}
     */
    public @Nullable String name() {
        return name;
    }

    /**
     * The explicit identity.
     *
     * @return the key, or {@code null} to identify the step by its position
     */
    public @Nullable String key() {
        return key;
    }
}
