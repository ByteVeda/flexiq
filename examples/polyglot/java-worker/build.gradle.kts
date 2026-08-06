plugins {
    application
}

repositories {
    mavenCentral()
    // Lets the example run against a locally built SDK: from sdks/java run
    // `./gradlew publishToMavenLocal` and this picks it up ahead of Central.
    mavenLocal()
}

dependencies {
    implementation("org.byteveda:taskito:0.22.0")
    // The native library ships as a per-platform classifier artifact. Swap for
    // your platform: linux-aarch64, osx-x86_64, osx-aarch64, windows-x86_64.
    runtimeOnly("org.byteveda:taskito:0.22.0:linux-x86_64")
    // CborSerializer needs Jackson's CBOR dataformat, which the SDK leaves optional.
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.20.0")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

application {
    mainClass = "NotifyWorker"
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
