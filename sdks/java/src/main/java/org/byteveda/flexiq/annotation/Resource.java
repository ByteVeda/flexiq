package org.byteveda.flexiq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Injects a worker resource into a {@code @TaskHandler} method. Place it on
 * parameters <em>after</em> the payload; the generated companion resolves each by
 * name from the worker's resource runtime (equivalent to {@code Resources.use(name)})
 * and passes it positionally. Source-retention — read at compile time, never at
 * runtime.
 *
 * <pre>{@code
 * @TaskHandler("send_email")
 * void send(EmailPayload payload, @Resource("db") Database db) { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface Resource {
    /** The registered resource name to resolve. */
    String value();
}
