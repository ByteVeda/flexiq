package org.byteveda.flexiq.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An immutable bundle of {@link Handler}s registered together via
 * {@code Worker.Builder.register}. Generated {@code <Class>Tasks.handlers(impl)}
 * returns one of these.
 */
public final class HandlerRegistry {
    private final List<Handler<?, ?>> handlers;

    private HandlerRegistry(List<Handler<?, ?>> handlers) {
        this.handlers = handlers;
    }

    public static HandlerRegistry of(Handler<?, ?>... handlers) {
        // Copy the varargs array so later caller mutations can't leak in.
        return new HandlerRegistry(Collections.unmodifiableList(new ArrayList<>(Arrays.asList(handlers))));
    }

    public List<Handler<?, ?>> handlers() {
        return handlers;
    }
}
