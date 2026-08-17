package org.byteveda.flexiq.worker;

/**
 * A {@link HandlerRegistry} discoverable on the classpath.
 *
 * <p>The {@code @TaskHandler} processor generates one implementation per
 * annotated class and lists it in {@code META-INF/services}, which is what lets
 * {@code flexiq executor} find handlers with no user {@code main} calling
 * {@code register(...)}.
 *
 * <p>The indirection is not incidental. {@link java.util.ServiceLoader} can only
 * instantiate a listed class when it is a subtype of the service with a public
 * no-arg constructor — the static {@code provider()} form works for modules on
 * the module path, not for a classpath {@code java -cp app.jar}. {@link
 * HandlerRegistry} is a final value type built from a user instance, so it can
 * be neither; this interface is the thing that can.
 *
 * <p>Implement it by hand when a handler class needs constructor arguments the
 * executor cannot supply — the processor skips those, since only your own code
 * knows how to build them.
 */
public interface HandlerRegistryProvider {
    /** The handlers this provider contributes. Called once per executor start. */
    HandlerRegistry registry();
}
