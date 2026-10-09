package org.byteveda.flexiq.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

/** A backend written against the pre-trace {@code submitWorkflow} must keep receiving runs. */
class QueueBackendWorkflowCompatTest {

    private static final int LEGACY_ARITY = 10;

    /** A backend overriding only the methods {@code overrides} answers; every default runs as written. */
    private static QueueBackend backend(InvocationHandler overrides) {
        return (QueueBackend) Proxy.newProxyInstance(
                QueueBackend.class.getClassLoader(), new Class<?>[] {QueueBackend.class}, (proxy, method, args) -> {
                    Object answer = overrides.invoke(proxy, method, args);
                    if (answer != null) {
                        return answer;
                    }
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    throw new AssertionError("unexpected call: " + method);
                });
    }

    private static boolean isLegacySubmit(Method method) {
        return method.getName().equals("submitWorkflow") && method.getParameterCount() == LEGACY_ARITY;
    }

    private static String submitTraced(QueueBackend backend) {
        return backend.submitWorkflow(
                "wf", 1, "[]", new String[0], new byte[0][], null, null, new String[0], null, null, "tp", "ts");
    }

    @Test
    void tracedSubmitFallsBackToALegacyOverride() {
        QueueBackend legacy = backend((proxy, method, args) -> isLegacySubmit(method) ? "run-1" : null);

        assertEquals("run-1", submitTraced(legacy));
    }

    @Test
    void tracedSubmitStillRefusesWhenNeitherOverloadIsOverridden() {
        QueueBackend bare = backend((proxy, method, args) -> null);

        UnsupportedOperationException refused =
                assertThrows(UnsupportedOperationException.class, () -> submitTraced(bare));
        assertEquals(QueueBackend.WORKFLOWS_UNSUPPORTED, refused.getMessage());
    }
}
