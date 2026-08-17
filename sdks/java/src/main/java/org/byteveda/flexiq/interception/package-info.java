/**
 * Producer-side argument interception: an {@link org.byteveda.flexiq.interception.Interceptor}
 * inspects each enqueue and returns an {@link org.byteveda.flexiq.interception.Interception}
 * — pass it through, convert the payload (e.g. to a
 * {@link org.byteveda.flexiq.proxies.ProxyRef}), redirect it to another task,
 * or reject it. Register with {@code FlexiQ.intercept(...)}. Unlike Python's
 * implicit arg-walking, interception here is an explicit transform over the
 * typed payload.
 */
@NullMarked
package org.byteveda.flexiq.interception;

import org.jspecify.annotations.NullMarked;
