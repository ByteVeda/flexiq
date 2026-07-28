/**
 * Enqueue gates: register a {@link org.byteveda.taskito.predicates.Predicate}
 * for a task and it is evaluated when that task is enqueued. If any predicate
 * rejects, the enqueue fails with a
 * {@link org.byteveda.taskito.errors.PredicateRejectedException} and no job is
 * created. Combine predicates with
 * {@link org.byteveda.taskito.predicates.Predicates#allOf},
 * {@code anyOf}, and {@code not}.
 */
@NullMarked
package org.byteveda.taskito.predicates;

import org.jspecify.annotations.NullMarked;
