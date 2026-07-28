/**
 * Topic pub/sub: N independent subscribers, each delivered its own job. A
 * subscriber is a normal task plus a registry row mapping {@code (topic, name)}
 * to {@code (taskName, queue)}; {@code Taskito.publish(...)} fans a message out
 * as one ordinary job per active subscription, so every task feature — retries,
 * dead-lettering, middleware — applies per subscriber, and a failing subscriber
 * never affects its siblings.
 */
@NullMarked
package org.byteveda.taskito.pubsub;

import org.jspecify.annotations.NullMarked;
