// A throwaway consumer that drives the SDK's JNI + Jackson paths end to end.
// CI builds it into a GraalVM native image (no tracing agent) to verify the
// reachability metadata the runtime jar ships under
// META-INF/native-image/org.byteveda/flexiq/. Not published; not part of the
// SDK artifact.
plugins {
    application
    checkstyle
    id("com.diffplug.spotless") version "7.2.1"
    id("org.graalvm.buildtools.native") version "0.10.6"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":"))
    // The runtime jar is native-free (natives publish as classifier artifacts);
    // consume the staged host-platform library directly from the root build.
    runtimeOnly(project(mapOf("path" to ":", "configuration" to "nativeRuntime")))
}

application {
    mainClass.set("org.byteveda.flexiq.graalvm.Smoke")
}

graalvmNative {
    binaries.named("main") {
        // Fail the build on missing metadata rather than emitting a fallback image.
        buildArgs.add("--no-fallback")
    }
    // Regeneration helper, not part of CI: `-Pagent :graalvm-smoke:run` then
    // `:graalvm-smoke:metadataCopy` rewrites the SDK's shipped metadata from a
    // fresh trace. Review the diff before committing — the agent records only
    // what the smoke exercised, and the resource-includes pattern for the
    // native library must stay platform-generic (see the shipped
    // resource-config.json).
    agent {
        metadataCopy {
            inputTaskNames.add("run")
            outputDirectories.add("../src/main/resources/META-INF/native-image/org.byteveda/flexiq")
            mergeWithExisting.set(true)
        }
    }
}

spotless {
    java {
        target("src/**/*.java")
        palantirJavaFormat("2.50.0")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

checkstyle {
    toolVersion = "10.21.4"
    configFile = file("../config/checkstyle/checkstyle.xml")
    isIgnoreFailures = false
}
