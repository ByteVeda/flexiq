/**
 * Enqueue gates: register a {@link org.byteveda.flexiq.predicates.Predicate}
 * for a task and it is evaluated when that task is enqueued. If any predicate
 * rejects, the enqueue fails with a
 * {@link org.byteveda.flexiq.errors.PredicateRejectedException} and no job is
 * created. Combine predicates with
 * {@link org.byteveda.flexiq.predicates.Predicates#allOf},
 * {@code anyOf}, and {@code not}.
 */
@NullMarked
package org.byteveda.flexiq.predicates;

import org.jspecify.annotations.NullMarked;
