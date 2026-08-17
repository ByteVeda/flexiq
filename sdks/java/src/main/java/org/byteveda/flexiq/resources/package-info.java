/**
 * Worker-side dependency injection: register a resource once, resolve it inside
 * task handlers.
 *
 * <p>Five scopes: {@link org.byteveda.flexiq.resources.ResourceScope#WORKER}
 * resources are built lazily once and shared across every task on the worker;
 * {@link org.byteveda.flexiq.resources.ResourceScope#THREAD} resources are
 * built lazily once per worker thread and disposed at worker shutdown;
 * {@link org.byteveda.flexiq.resources.ResourceScope#TASK} resources are built
 * lazily per task invocation and disposed (LIFO) when it ends;
 * {@link org.byteveda.flexiq.resources.ResourceScope#REQUEST} resources are
 * built fresh on every use and disposed with the task; and
 * {@link org.byteveda.flexiq.resources.ResourceScope#POOLED} resources live in
 * a bounded pool (sized by {@link org.byteveda.flexiq.resources.PoolConfig})
 * that each task checks one instance out of for its duration. Handlers resolve
 * them with {@link org.byteveda.flexiq.resources.Resources#use(String)}.
 */
@NullMarked
package org.byteveda.flexiq.resources;

import org.jspecify.annotations.NullMarked;
