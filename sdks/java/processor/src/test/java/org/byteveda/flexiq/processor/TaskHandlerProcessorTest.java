package org.byteveda.flexiq.processor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Compile-time checks the processor performs, driven through a real javac run.
 *
 * <p>Task names are global to a queue, so two handlers claiming one means jobs for
 * that name silently run whichever registration landed last. In a single
 * compilation that is knowable before the code ever runs, so it is an error here
 * rather than a surprise at {@code discover()}.
 */
class TaskHandlerProcessorTest {

    /**
     * The annotation, declared in the compilation under test.
     *
     * <p>The processor reads annotations structurally by fully-qualified name and
     * deliberately does not depend on the runtime module, so its tests must not
     * either — depending on the SDK here would reintroduce exactly the cycle this
     * module exists to avoid.
     */
    private static final JavaFileObject ANNOTATION = source(
            "org.byteveda.flexiq.annotation.TaskHandler",
            """
            package org.byteveda.flexiq.annotation;

            public @interface TaskHandler {
                String value() default "";
            }
            """);

    @Test
    void twoHandlersClaimingOneNameFailTheBuild(@TempDir Path out) throws IOException {
        JavaFileObject mailer = source(
                "com.acme.Mailer",
                """
                package com.acme;
                import org.byteveda.flexiq.annotation.TaskHandler;

                public class Mailer {
                    @TaskHandler("send")
                    public String send(String to) { return to; }
                }
                """);
        JavaFileObject courier = source(
                "com.acme.Courier",
                """
                package com.acme;
                import org.byteveda.flexiq.annotation.TaskHandler;

                public class Courier {
                    @TaskHandler("send")
                    public String deliver(String to) { return to; }
                }
                """);

        String errors = errorsOf(compile(out, ANNOTATION, mailer, courier));

        assertTrue(errors.contains("duplicate task name \"send\""), errors);
        // Both sides named: the diagnostic points at one, and says which other one it clashed with.
        assertTrue(errors.contains("com.acme.Mailer#send") || errors.contains("com.acme.Courier#deliver"), errors);
    }

    @Test
    void handlersFallingBackToTheSameMethodNameFailTheBuild(@TempDir Path out) throws IOException {
        // Neither declares a name, so both default to the method's own — the
        // collision nobody writes down, and the one an explicit value never shows.
        JavaFileObject invoices = source(
                "com.acme.Invoices",
                """
                package com.acme;
                import org.byteveda.flexiq.annotation.TaskHandler;

                public class Invoices {
                    @TaskHandler
                    public String send(String to) { return to; }
                }
                """);
        JavaFileObject reminders = source(
                "com.acme.Reminders",
                """
                package com.acme;
                import org.byteveda.flexiq.annotation.TaskHandler;

                public class Reminders {
                    @TaskHandler
                    public String send(String to) { return to; }
                }
                """);

        String errors = errorsOf(compile(out, ANNOTATION, invoices, reminders));

        assertTrue(errors.contains("duplicate task name \"send\""), errors);
    }

    @Test
    void distinctNamesRaiseNothing(@TempDir Path out) throws IOException {
        JavaFileObject mailer = source(
                "com.acme.Mailer",
                """
                package com.acme;
                import org.byteveda.flexiq.annotation.TaskHandler;

                public class Mailer {
                    @TaskHandler("mail.send")
                    public String send(String to) { return to; }

                    @TaskHandler("mail.retry")
                    public String retry(String to) { return to; }
                }
                """);

        // Not "no diagnostics at all": the generated companion references the runtime
        // module, which this compilation deliberately does not have, so javac reports
        // the unresolved symbols. That the generated code compiles for real is what
        // the SDK module's AnnotationProcessorTest proves.
        assertFalse(errorsOf(compile(out, ANNOTATION, mailer)).contains("duplicate task name"));
    }

    /** Run annotation processing only — the generated code references the runtime module, which is not here. */
    private static List<Diagnostic<? extends JavaFileObject>> compile(Path out, JavaFileObject... sources)
            throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager files = compiler.getStandardFileManager(diagnostics, null, UTF_8)) {
            files.setLocation(StandardLocation.CLASS_OUTPUT, List.of(out.toFile()));
            files.setLocation(StandardLocation.SOURCE_OUTPUT, List.of(out.toFile()));
            JavaCompiler.CompilationTask task =
                    compiler.getTask(null, files, diagnostics, List.of("-proc:only"), null, List.of(sources));
            task.setProcessors(List.of(new TaskHandlerProcessor()));
            task.call();
        }
        return diagnostics.getDiagnostics();
    }

    private static String errorsOf(List<Diagnostic<? extends JavaFileObject>> diagnostics) {
        return diagnostics.stream()
                .filter(diagnostic -> diagnostic.getKind() == Diagnostic.Kind.ERROR)
                .map(diagnostic -> diagnostic.getMessage(null))
                .collect(Collectors.joining("\n"));
    }

    private static JavaFileObject source(String qualifiedName, String body) {
        return new SimpleJavaFileObject(
                URI.create("string:///" + qualifiedName.replace('.', '/') + JavaFileObject.Kind.SOURCE.extension),
                JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return body;
            }
        };
    }
}
