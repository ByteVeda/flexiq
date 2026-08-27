package org.byteveda.flexiq.predicates;

/** A feature-flag lookup seam, plugged into {@link Recipes#featureFlag}. */
@FunctionalInterface
public interface BooleanFlag {
    /**
     * Look one flag up.
     *
     * @param flag the flag's name
     * @return whether it is on
     */
    boolean enabled(String flag);
}
