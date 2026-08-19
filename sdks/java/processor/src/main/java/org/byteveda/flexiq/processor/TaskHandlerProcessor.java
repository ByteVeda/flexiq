package org.byteveda.flexiq.processor;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import org.jspecify.annotations.Nullable;

/**
 * Generates, for each class with {@code @TaskHandler} methods, a {@code <Class>Tasks}
 * companion holding a typed {@code Task} constant per handler plus a static
 * {@code bind(Worker.Builder, <Class>)}. Reads the annotation structurally, so it
 * needs no dependency on the runtime module.
 *
 * <p>When the annotated class is top-level with an accessible no-arg constructor,
 * the companion also gets a nested {@code Provider} class and is listed in
 * {@code META-INF/services}, which is what lets {@code flexiq executor} find
 * handlers on the classpath with no user {@code main}.
 */
@SupportedAnnotationTypes(TaskHandlerProcessor.ANNOTATION)
public final class TaskHandlerProcessor extends AbstractProcessor {
    static final String ANNOTATION = "org.byteveda.flexiq.annotation.TaskHandler";

    /** Service the generated providers are registered under, for {@link java.util.ServiceLoader}. */
    static final String SERVICE = "org.byteveda.flexiq.worker.HandlerRegistryProvider";

    static final String RESOURCE = "org.byteveda.flexiq.annotation.Resource";
    static final String COMPRESSED = "org.byteveda.flexiq.annotation.Compressed";
    static final String ENCRYPTED = "org.byteveda.flexiq.annotation.Encrypted";

    // Accept whatever the compiler runs at, so building at -source 17+ doesn't warn
    // that the processor caps out below it (structural read, version-agnostic).
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    /**
     * Companions that can be service-loaded, accumulated across rounds.
     *
     * <p>The service file has to be written once, in the final round: the Filer
     * rejects reopening a resource it has already created, so a per-round write
     * would fail as soon as generated code triggered a second round.
     */
    private final Set<String> providers = new LinkedHashSet<>();

    /**
     * Task name -> the handler that claimed it, accumulated across rounds.
     *
     * <p>Task names are global to a queue, so two handlers sharing one means every
     * job for that name runs whichever handler registration happened to land last.
     * Only this compilation is visible here — an incremental build recompiles a
     * subset — so {@code discover()} repeats the check across the whole classpath
     * at runtime; the two cover different halves of the same mistake.
     */
    private final Map<String, String> claimedBy = new LinkedHashMap<>();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        TypeElement marker = processingEnv.getElementUtils().getTypeElement(ANNOTATION);
        if (marker == null) {
            return false;
        }
        // Group handler methods by their enclosing class.
        Map<TypeElement, List<ExecutableElement>> byClass = new LinkedHashMap<>();
        for (Element element : roundEnv.getElementsAnnotatedWith(marker)) {
            if (element.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) element;
            if (!validate(method) || !claim(method)) {
                continue;
            }
            TypeElement owner = (TypeElement) method.getEnclosingElement();
            byClass.computeIfAbsent(owner, key -> new java.util.ArrayList<>()).add(method);
        }
        byClass.forEach(this::generate);
        if (roundEnv.processingOver()) {
            writeServiceFile();
        }
        return true;
    }

    /**
     * List the generated companions in {@code META-INF/services} so
     * {@code ServiceLoader.load(HandlerRegistryProvider.class)} finds them.
     */
    private void writeServiceFile() {
        if (providers.isEmpty()) {
            return;
        }
        try {
            FileObject file = processingEnv
                    .getFiler()
                    .createResource(StandardLocation.CLASS_OUTPUT, "", "META-INF/services/" + SERVICE);
            try (Writer writer = file.openWriter()) {
                for (String provider : providers) {
                    writer.write(provider);
                    writer.write("\n");
                }
            }
        } catch (IOException e) {
            processingEnv
                    .getMessager()
                    .printMessage(
                            Diagnostic.Kind.ERROR,
                            "failed to write META-INF/services/" + SERVICE + ": " + e.getMessage());
        }
    }

    /**
     * Whether {@code owner} can be built by {@link java.util.ServiceLoader} — a
     * top-level, non-abstract type with an accessible no-arg constructor.
     *
     * <p>A class with only injected dependencies cannot be, and only the user's
     * own code knows how to build it; that stays the explicit
     * {@code register(...)} path.
     *
     * <p>Every nested owner is excluded, static ones included: the companion is
     * written at package level and names the handler by its simple name, which
     * does not resolve for an {@code Outer.Inner}.
     */
    private boolean isServiceLoadable(TypeElement owner) {
        if (owner.getModifiers().contains(Modifier.ABSTRACT)
                || owner.getNestingKind().isNested()) {
            return false;
        }
        boolean declaresAny = false;
        for (Element enclosed : owner.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.CONSTRUCTOR) {
                continue;
            }
            declaresAny = true;
            ExecutableElement constructor = (ExecutableElement) enclosed;
            if (constructor.getParameters().isEmpty()
                    && !constructor.getModifiers().contains(Modifier.PRIVATE)) {
                return true;
            }
        }
        // No declared constructor at all means the implicit public no-arg one.
        return !declaresAny;
    }

    /**
     * Record this handler's task name, failing the build when another handler in
     * this compilation already claimed it.
     *
     * <p>Reported on the second one seen; which of the two that is depends on the
     * order the compiler hands back annotated elements, so the message names both.
     */
    private boolean claim(ExecutableElement method) {
        String name = taskName(method);
        String origin = origin(method);
        String previous = claimedBy.putIfAbsent(name, origin);
        if (previous != null) {
            error(
                    method,
                    "duplicate task name \"" + name + "\": already declared by " + previous
                            + ". Task names are global, so one handler would shadow the other"
                            + " — give one an explicit @TaskHandler(\"...\")");
            return false;
        }
        return true;
    }

    /** {@code com.acme.Mailer#send}, for a diagnostic naming the other side of a clash. */
    private String origin(ExecutableElement method) {
        TypeElement owner =
                (TypeElement) Objects.requireNonNull(method.getEnclosingElement(), "a method has an enclosing type");
        return owner.getQualifiedName() + "#" + method.getSimpleName();
    }

    private boolean validate(ExecutableElement method) {
        List<? extends VariableElement> params = method.getParameters();
        if (params.isEmpty()) {
            error(method, "@TaskHandler method must take the payload as its first parameter");
            return false;
        }
        if (resourceName(params.get(0)) != null) {
            error(method, "the first @TaskHandler parameter is the payload and must not be annotated @Resource");
            return false;
        }
        if (params.get(0).asType().getKind().isPrimitive()) {
            // A primitive payload would generate `new TypeReference<int>() {}` —
            // invalid Java that only surfaces as a cryptic error in the generated file.
            error(method, "@TaskHandler payload must be a reference type (use the boxed type, e.g. Integer)");
            return false;
        }
        for (int i = 1; i < params.size(); i++) {
            if (resourceName(params.get(i)) == null) {
                error(method, "@TaskHandler parameters after the payload must be annotated @Resource");
                return false;
            }
        }
        if (method.getModifiers().contains(Modifier.PRIVATE)) {
            error(method, "@TaskHandler method must not be private");
            return false;
        }
        if (method.getModifiers().contains(Modifier.STATIC)) {
            error(method, "@TaskHandler method must not be static");
            return false;
        }
        return true;
    }

    private void generate(TypeElement owner, List<ExecutableElement> methods) {
        String pkg = packageOf(owner);
        String ownerSimple = owner.getSimpleName().toString();
        String companion = ownerSimple + "Tasks";
        String qualified = pkg.isEmpty() ? companion : pkg + "." + companion;
        boolean anyResources = methods.stream().anyMatch(m -> m.getParameters().size() > 1);

        StringBuilder out = new StringBuilder();
        if (!pkg.isEmpty()) {
            out.append("package ").append(pkg).append(";\n\n");
        }
        out.append("import com.fasterxml.jackson.core.type.TypeReference;\n")
                .append("import org.byteveda.flexiq.task.Task;\n")
                .append("import org.byteveda.flexiq.worker.Handler;\n")
                .append("import org.byteveda.flexiq.worker.HandlerRegistry;\n")
                .append("import org.byteveda.flexiq.worker.HandlerRegistryProvider;\n")
                .append("import org.byteveda.flexiq.worker.Worker;\n")
                .append(anyResources ? "import org.byteveda.flexiq.resources.Resources;\n" : "")
                .append("\n")
                .append("/** Generated task descriptors + worker binding for {@link ")
                .append(ownerSimple)
                .append("}. */\n")
                .append("public final class ")
                .append(companion)
                .append(" {\n")
                .append("    private ")
                .append(companion)
                .append("() {}\n\n");

        for (ExecutableElement method : methods) {
            String constant = upperSnake(method.getSimpleName().toString());
            String payloadType = method.getParameters().get(0).asType().toString();
            String name = taskName(method);
            out.append("    public static final Task<")
                    .append(payloadType)
                    .append("> ")
                    .append(constant)
                    .append(" = Task.of(\"")
                    .append(escape(name))
                    .append("\", new TypeReference<")
                    .append(payloadType)
                    .append(">() {})")
                    .append(options(method))
                    .append(codecsChain(method))
                    .append(";\n\n");
        }

        out.append("    public static Worker.Builder bind(Worker.Builder builder, ")
                .append(ownerSimple)
                .append(" impl) {\n");
        for (ExecutableElement method : methods) {
            String constant = upperSnake(method.getSimpleName().toString());
            out.append("        builder.handle(")
                    .append(constant)
                    .append(", ")
                    .append(handlerExpr(method))
                    .append(");\n");
        }
        out.append("        return builder;\n    }\n\n");

        // handlers() — returns a HandlerRegistry for Worker.Builder.register(...).
        out.append("    public static HandlerRegistry handlers(")
                .append(ownerSimple)
                .append(" impl) {\n        return HandlerRegistry.of(\n");
        for (int i = 0; i < methods.size(); i++) {
            ExecutableElement method = methods.get(i);
            String constant = upperSnake(method.getSimpleName().toString());
            out.append("                Handler.of(")
                    .append(constant)
                    .append(", ")
                    .append(handlerExpr(method))
                    .append(")");
            out.append(i + 1 < methods.size() ? ",\n" : ");\n");
        }
        out.append("    }\n");

        // The ServiceLoader entry point. On the classpath a listed class must be
        // a subtype of the service with a public no-arg constructor — the static
        // `provider()` form only applies to modules — so this is a real nested
        // implementation rather than a factory method on the companion.
        if (isServiceLoadable(owner)) {
            out.append("\n    /** Discovered by {@code flexiq executor} via ServiceLoader. */\n")
                    .append("    public static final class Provider implements HandlerRegistryProvider {\n")
                    .append("        public Provider() {}\n\n")
                    .append("        @Override\n")
                    .append("        public HandlerRegistry registry() {\n            return handlers(new ")
                    .append(ownerSimple)
                    .append("());\n        }\n    }\n");
            // Binary name: ServiceLoader resolves the nested class by `$` form.
            providers.add(qualified + "$Provider");
        } else {
            processingEnv
                    .getMessager()
                    .printMessage(
                            Diagnostic.Kind.NOTE,
                            ownerSimple
                                    + " is not a top-level type with an accessible no-arg constructor, so it"
                                    + " cannot be discovered by `flexiq executor`; register its handlers"
                                    + " explicitly with " + companion + ".handlers(impl)",
                            owner);
        }
        out.append("}\n");

        write(owner, qualified, out.toString());
    }

    /** Build the chained option calls (.queue/.maxRetries/.timeoutMs/.priority) from the annotation. */
    private String options(ExecutableElement method) {
        AnnotationMirror mirror = mirror(method);
        StringBuilder chain = new StringBuilder();
        String queue = stringValue(mirror, "queue", "");
        if (!queue.isEmpty()) {
            chain.append(".queue(\"").append(escape(queue)).append("\")");
        }
        long maxRetries = longValue(mirror, "maxRetries", -1);
        if (maxRetries >= 0) {
            chain.append(".maxRetries(").append(maxRetries).append(")");
        }
        long timeoutMs = longValue(mirror, "timeoutMs", -1);
        if (timeoutMs >= 0) {
            chain.append(".timeoutMs(").append(timeoutMs).append("L)");
        }
        long priority = longValue(mirror, "priority", 0);
        if (priority != 0) {
            chain.append(".priority(").append(priority).append(")");
        }
        if (boolValue(mirror, "idempotent", false)) {
            chain.append(".idempotent(true)");
        }
        String rateLimit = stringValue(mirror, "rateLimit", "");
        if (!rateLimit.isEmpty()) {
            chain.append(".rateLimit(\"").append(rateLimit).append("\")");
        }
        String onExcess = stringValue(mirror, "onExcess", "");
        if (!onExcess.isEmpty()) {
            // Resolved by the runtime, like rateLimit: this processor is
            // deliberately dependency-free, so it cannot name the enum constants.
            chain.append(".onExcess(org.byteveda.flexiq.task.OnExcess.fromWireName(\"")
                    .append(escape(onExcess))
                    .append("\"))");
        }
        String retryBudget = stringValue(mirror, "retryBudget", "");
        if (!retryBudget.isEmpty()) {
            chain.append(".retryBudget(\"").append(retryBudget).append("\")");
        }
        long maxConcurrent = longValue(mirror, "maxConcurrent", 0);
        if (maxConcurrent > 0) {
            chain.append(".maxConcurrent(").append(maxConcurrent).append(")");
        }
        long maxInFlightPerTask = longValue(mirror, "maxInFlightPerTask", 0);
        if (maxInFlightPerTask > 0) {
            chain.append(".maxInFlightPerTask(").append(maxInFlightPerTask).append(")");
        }
        appendCircuitBreaker(chain, mirror);
        return chain.toString();
    }

    /** Emit a {@code .circuitBreaker(...)} call when the annotation configures a threshold. */
    private void appendCircuitBreaker(StringBuilder chain, @Nullable AnnotationMirror mirror) {
        long threshold = longValue(mirror, "circuitBreakerThreshold", 0);
        if (threshold <= 0) {
            return;
        }
        // Fully qualified so the generated companion needs no extra import.
        chain.append(".circuitBreaker(org.byteveda.flexiq.task.CircuitBreakerConfig.builder(")
                .append(threshold)
                .append(")")
                .append(".windowSeconds(")
                .append(longValue(mirror, "circuitBreakerWindowSeconds", 60))
                .append("L).cooldownSeconds(")
                .append(longValue(mirror, "circuitBreakerCooldownSeconds", 300))
                .append("L).halfOpenProbes(")
                .append(longValue(mirror, "circuitBreakerHalfOpenProbes", 5))
                .append(").halfOpenSuccessRate(")
                .append(doubleValue(mirror, "circuitBreakerHalfOpenSuccessRate", 0.8))
                .append(").build())");
    }

    /**
     * The {@code TaskFunction} expression for a handler: a method reference when it
     * takes only the payload, otherwise a lambda that resolves each {@code @Resource}
     * parameter from the worker resource runtime.
     */
    private String handlerExpr(ExecutableElement method) {
        String methodName = method.getSimpleName().toString();
        List<? extends VariableElement> params = method.getParameters();
        StringBuilder args = new StringBuilder("payload");
        for (int i = 1; i < params.size(); i++) {
            args.append(", Resources.use(\"")
                    .append(escape(requireResourceName(params.get(i))))
                    .append("\")");
        }
        if (isVoid(method)) {
            return "payload -> { impl." + methodName + "(" + args + "); return null; }";
        }
        return params.size() > 1 ? "payload -> impl." + methodName + "(" + args + ")" : "impl::" + methodName;
    }

    /** A {@code .codecs(...)} call for the task's @Compressed/@Encrypted markers (compress before encrypt). */
    private String codecsChain(ExecutableElement method) {
        List<String> names = new java.util.ArrayList<>();
        if (hasAnnotation(method, COMPRESSED)) {
            names.add("compressed");
        }
        if (hasAnnotation(method, ENCRYPTED)) {
            names.add("encrypted");
        }
        if (names.isEmpty()) {
            return "";
        }
        StringBuilder chain = new StringBuilder(".codecs(");
        for (int i = 0; i < names.size(); i++) {
            chain.append("\"").append(names.get(i)).append("\"");
            if (i + 1 < names.size()) {
                chain.append(", ");
            }
        }
        return chain.append(")").toString();
    }

    /** Whether the method or its enclosing class carries the annotation {@code fqn}. */
    private boolean hasAnnotation(ExecutableElement method, String fqn) {
        Element enclosing = Objects.requireNonNull(method.getEnclosingElement(), "a method has an enclosing type");
        return hasAnnotation(method.getAnnotationMirrors(), fqn)
                || hasAnnotation(enclosing.getAnnotationMirrors(), fqn);
    }

    private boolean hasAnnotation(List<? extends AnnotationMirror> mirrors, String fqn) {
        for (AnnotationMirror mirror : mirrors) {
            if (mirror.getAnnotationType().toString().equals(fqn)) {
                return true;
            }
        }
        return false;
    }

    /** The {@code @Resource} value on a parameter, or null if it is not annotated. */
    /** The injected resource name of a non-payload parameter, which validation has already required. */
    private String requireResourceName(VariableElement param) {
        return Objects.requireNonNull(resourceName(param), "a non-payload parameter must carry a resource name");
    }

    private @Nullable String resourceName(VariableElement param) {
        for (AnnotationMirror annotation : param.getAnnotationMirrors()) {
            if (annotation.getAnnotationType().toString().equals(RESOURCE)) {
                Object value = rawValue(annotation, "value");
                return value == null ? "" : value.toString();
            }
        }
        return null;
    }

    private String taskName(ExecutableElement method) {
        String value = stringValue(mirror(method), "value", "");
        return value.isEmpty() ? method.getSimpleName().toString() : value;
    }

    private @Nullable AnnotationMirror mirror(ExecutableElement method) {
        for (AnnotationMirror mirror : method.getAnnotationMirrors()) {
            if (mirror.getAnnotationType().toString().equals(ANNOTATION)) {
                return mirror;
            }
        }
        return null;
    }

    private String stringValue(@Nullable AnnotationMirror mirror, String key, String fallback) {
        Object value = rawValue(mirror, key);
        return value == null ? fallback : value.toString();
    }

    private long longValue(@Nullable AnnotationMirror mirror, String key, long fallback) {
        Object value = rawValue(mirror, key);
        return value instanceof Number ? ((Number) value).longValue() : fallback;
    }

    private boolean boolValue(@Nullable AnnotationMirror mirror, String key, boolean fallback) {
        Object value = rawValue(mirror, key);
        return value instanceof Boolean ? (Boolean) value : fallback;
    }

    private double doubleValue(@Nullable AnnotationMirror mirror, String key, double fallback) {
        Object value = rawValue(mirror, key);
        return value instanceof Number ? ((Number) value).doubleValue() : fallback;
    }

    private @Nullable Object rawValue(@Nullable AnnotationMirror mirror, String key) {
        if (mirror == null) {
            return null;
        }
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
                mirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(key)) {
                return entry.getValue().getValue();
            }
        }
        return null;
    }

    private boolean isVoid(ExecutableElement method) {
        return method.getReturnType().getKind() == javax.lang.model.type.TypeKind.VOID;
    }

    private String packageOf(TypeElement type) {
        return processingEnv
                .getElementUtils()
                .getPackageOf(type)
                .getQualifiedName()
                .toString();
    }

    private void write(Element origin, String qualifiedName, String source) {
        try {
            JavaFileObject file = processingEnv.getFiler().createSourceFile(qualifiedName, origin);
            try (Writer writer = file.openWriter()) {
                writer.write(source);
            }
        } catch (IOException e) {
            error(origin, "failed to write " + qualifiedName + ": " + e.getMessage());
        }
    }

    private void error(Element element, String message) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, element);
    }

    /** {@code sendEmail} -> {@code SEND_EMAIL}. */
    private static String upperSnake(String camel) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                out.append('_');
            }
            out.append(Character.toUpperCase(c));
        }
        return out.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
