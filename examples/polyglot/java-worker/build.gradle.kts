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
    implementation("org.byteveda:taskito:0.23.0")
    // The native library ships as a per-platform classifier artifact. Swap for
    // your platform: linux-aarch64, osx-x86_64, osx-aarch64, windows-x86_64.
    runtimeOnly("org.byteveda:taskito:0.23.0:linux-x86_64")
    // CborSerializer needs Jackson's CBOR dataformat, which the SDK leaves optional.
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.20.0")
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
