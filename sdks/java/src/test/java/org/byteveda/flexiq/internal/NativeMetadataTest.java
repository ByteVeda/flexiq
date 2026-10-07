package org.byteveda.flexiq.internal;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The shipped native-image reflection metadata names constructors by exact signature. A JVM
 * run never reads it, so a parameter added to a {@code @JsonCreator} constructor only failed
 * in the GraalVM job; this pins the two together on every test run.
 */
class NativeMetadataTest {

    private static final String REFLECT_CONFIG = "/META-INF/native-image/org.byteveda/flexiq/reflect-config.json";

    /** The JDK the Multi-Release overlay ({@code src/main/java22}) targets. */
    private static final int FFM_FEATURE_VERSION = 22;

    private static final Map<String, Class<?>> PRIMITIVES = Map.of(
            "int", int.class,
            "long", long.class,
            "boolean", boolean.class,
            "double", double.class);

    @Test
    void everyListedConstructorExistsAndEveryJsonCreatorIsListed() throws Exception {
        JsonNode entries;
        try (InputStream in = NativeMetadataTest.class.getResourceAsStream(REFLECT_CONFIG)) {
            assertNotNull(in, "reflect-config.json ships in the jar");
            entries = new ObjectMapper().readTree(in);
        }

        for (JsonNode entry : entries) {
            String name = entry.get("name").asText();
            if (!name.startsWith("org.byteveda.")) {
                continue;
            }
            Class<?> type;
            try {
                type = Class.forName(name);
            } catch (ClassNotFoundException e) {
                // The FFM transport ships in the jar's META-INF/versions/22 overlay, so an
                // older JVM legitimately cannot load it; on 22+ a missing class is drift.
                assertTrue(Runtime.version().feature() < FFM_FEATURE_VERSION, name + " is listed but does not exist");
                continue;
            }
            List<List<Class<?>>> listed = new ArrayList<>();
            for (JsonNode method : entry.path("methods")) {
                if (!"<init>".equals(method.get("name").asText())) {
                    continue;
                }
                List<Class<?>> params = new ArrayList<>();
                for (JsonNode param : method.get("parameterTypes")) {
                    params.add(resolve(param.asText()));
                }
                // Throws NoSuchMethodException when the metadata names a constructor that is gone.
                type.getDeclaredConstructor(params.toArray(new Class<?>[0]));
                listed.add(params);
            }
            for (Constructor<?> ctor : type.getDeclaredConstructors()) {
                if (ctor.isAnnotationPresent(JsonCreator.class)) {
                    assertTrue(
                            listed.contains(List.of(ctor.getParameterTypes())),
                            name + "'s @JsonCreator constructor is missing from reflect-config.json");
                }
            }
        }
    }

    private static Class<?> resolve(String name) throws ClassNotFoundException {
        Class<?> primitive = PRIMITIVES.get(name);
        return primitive != null ? primitive : Class.forName(name);
    }
}
