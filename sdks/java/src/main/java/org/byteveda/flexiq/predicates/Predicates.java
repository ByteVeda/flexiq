package org.byteveda.flexiq.predicates;

import java.util.List;

/** Combinators for building compound {@link Predicate}s. */
public final class Predicates {
    private Predicates() {}

    /** Passes only when every {@code predicate} passes (vacuously true when none). */
    public static Predicate allOf(Predicate... predicates) {
        List<Predicate> all = List.of(predicates);
        return context -> {
            for (Predicate predicate : all) {
                if (!predicate.test(context)) {
                    return false;
                }
            }
            return true;
        };
    }

    /** Passes when at least one {@code predicate} passes (false when none). */
    public static Predicate anyOf(Predicate... predicates) {
        List<Predicate> any = List.of(predicates);
        return context -> {
            for (Predicate predicate : any) {
                if (predicate.test(context)) {
                    return true;
                }
            }
            return false;
        };
    }

    /** Inverts {@code predicate}. */
    public static Predicate not(Predicate predicate) {
        return context -> !predicate.test(context);
    }
}
