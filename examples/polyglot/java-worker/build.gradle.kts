plugins {
    application
}

repositories {
    // First so a locally published SDK wins: from sdks/java run
    // `./gradlew publishToMavenLocal` and this resolves that build instead of
    // the release. Remove it to pin the example to Maven Central.
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.byteveda:flexiq:2.0.0")
    // The native library ships as a per-platform classifier artifact. Swap for
    // your platform: linux-x86_64, linux-aarch64, osx-x86_64, osx-aarch64, windows-x86_64.
    runtimeOnly("org.byteveda:flexiq:2.0.0:linux-x86_64")
    // CborSerializer needs Jackson's CBOR dataformat, which the SDK leaves optional.
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.20.0")
    // Compile-time @TaskHandler bindings — what makes notifyCustomer discoverable
    // by `flexiq executor` via META-INF/services.
    annotationProcessor("org.byteveda:flexiq-processor:2.0.0")
}

// No toolchain pin on purpose: the SDK's baseline is Java 17, so whatever JDK
// runs Gradle works as long as it is 17 or newer. Pinning an exact version would
// fail on a machine that simply has a newer one installed.

application {
    mainClass = "NotifyWorker"
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}

// A second entry point beside `run`: the SDK's own CLI, discovering
// notifyCustomer via META-INF/services instead of running this module's
// main(). Configuration (--attach/--slots/--serializer, the token) comes
// entirely from the FLEXIQ_* env vars org.byteveda.flexiq.cli.Cli already
// reads, so no Gradle --args plumbing is needed.
tasks.register<JavaExec>("runExecutor") {
    group = "application"
    description = "Run as an attached executor (flexiq executor) instead of polling storage."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.byteveda.flexiq.cli.Cli")
    args("executor")
    standardInput = System.`in`
}
