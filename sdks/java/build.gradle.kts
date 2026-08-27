
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

plugins {
    `java-library`
    checkstyle
    id("com.diffplug.spotless") version "7.2.1"
    id("com.vanniktech.maven.publish") version "0.37.0"
    id("net.ltgt.errorprone") version "5.1.0"
}

java {
    // Sources + javadoc jars are added by the maven-publish plugin below.
    // Compile to Java 17 bytecode with whatever JDK (>= 17) runs Gradle, rather
    // than pinning a toolchain — `--release 17` also rejects post-17 stdlib APIs.
    // Floor is 17 so every Spring Boot 3 app can adopt the SDK.
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    // Error Prone is carried only for NullAway; every other check stays off so the
    // build fails on real nullness regressions and nothing else.
    //
    // `enabled.set(...)`, never `isEnabled = ...`: `ErrorProneOptions` has no
    // `isEnabled` member, so that assignment resolves against the *outer*
    // receiver — the JavaCompile task — and silently disables the compile
    // task itself rather than Error Prone.
    options.errorprone {
        enabled.set(false)
    }
}

// NullAway makes the JSpecify annotations load-bearing: in a @NullMarked package
// an unannotated reference is non-null, and dereferencing a @Nullable one fails
// the build. OnlyNullMarked keeps the `package-info.java` annotations the single
// source of truth — no package list duplicated here. Tests deliberately feed
// nulls to assert the error paths, so they stay out.
tasks.named<JavaCompile>("compileJava") {
    options.errorprone {
        enabled.set(true)
        disableAllChecks.set(true)
        check("NullAway", CheckSeverity.ERROR)
        option("NullAway:OnlyNullMarked", "true")
        option("NullAway:JSpecifyMode", "true")
        // picocli assigns command fields reflectively after construction, so the
        // constructor-initialization check cannot see them being set.
        option(
            "NullAway:ExcludedFieldAnnotations",
            "picocli.CommandLine.Option,picocli.CommandLine.Parameters,picocli.CommandLine.ParentCommand,picocli.CommandLine.Spec",
        )
    }
}

repositories {
    mavenCentral()
}

// --- Publishing: Maven Central via the Central Publisher Portal ------------

mavenPublishing {
    publishToMavenCentral()
    // Signature artifacts are attached to the publication, so asking for them
    // without a key makes even `publishToMavenLocal` fail on an unsigned
    // artifact that can never be produced. Only CI holds the key, and the
    // Central Portal rejects an unsigned upload, so the release path is still
    // covered.
    if (providers.gradleProperty("signingInMemoryKey").isPresent ||
        providers.gradleProperty("signing.keyId").isPresent
    ) {
        signAllPublications()
    }
    coordinates(group.toString(), "flexiq", version.toString())
    pom {
        name.set("FlexiQ")
        description.set("Rust-powered task queue for the JVM, via a JNI binding over the FlexiQ core.")
        url.set("https://github.com/ByteVeda/flexiq")
        licenses {
            license {
                name.set("MIT")
                url.set("https://opensource.org/licenses/MIT")
            }
        }
        developers {
            developer {
                id.set("byteveda")
                name.set("ByteVeda")
            }
        }
        scm {
            url.set("https://github.com/ByteVeda/flexiq")
            connection.set("scm:git:https://github.com/ByteVeda/flexiq.git")
            developerConnection.set("scm:git:ssh://git@github.com/ByteVeda/flexiq.git")
        }
    }
}

// Javadoc caps its output at 100 warnings by default, which is how a backlog of
// a few thousand read as "104" for as long as anyone had looked. Raise the cap so
// the number the build prints is the real one.
//
// -Xwerror holds the runtime at zero, the way `flexiq-test` is held: the backlog
// that cap was hiding is cleared, and an undocumented parameter is now a build
// failure rather than a line nobody reads.
tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xmaxwarns", "100000")
        addStringOption("Xwerror", "-quiet")
    }
}

// --- Code integrity: formatting + static analysis -------------------------

spotless {
    java {
        target("src/**/*.java")
        palantirJavaFormat("2.50.0") // modern 4-space formatter; `spotlessApply` to fix
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

checkstyle {
    toolVersion = "10.21.4"
    configFile = file("config/checkstyle/checkstyle.xml")
    isIgnoreFailures = false
}
// Native staging copies binaries under build/resources; never lint those.
tasks.withType<Checkstyle>().configureEach {
    source = fileTree("src") { include("**/*.java") }
}

sourceSets["test"].java.srcDir(
    layout.buildDirectory.dir("generated/sources/annotationProcessor/java/test")
)

dependencies {
    // Nullness contract of the public API. `api` so consumers' own null checkers
    // (and Kotlin's platform-type inference) resolve the annotations; the jar is
    // a few KB with no transitive dependencies.
    api("org.jspecify:jspecify:${property("jspecifyVersion")}")
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    errorprone("com.google.errorprone:error_prone_core:${property("errorProneVersion")}")
    errorprone("com.uber.nullaway:nullaway:${property("nullAwayVersion")}")
    implementation("info.picocli:picocli:4.7.6")

    // Optional: the MessagePack serializer. Compiled against, not bundled — a
    // consumer that uses MsgpackSerializer adds this dependency themselves.
    compileOnly("org.msgpack:jackson-dataformat-msgpack:0.9.8")
    testImplementation("org.msgpack:jackson-dataformat-msgpack:0.9.8")

    // Optional: the CBOR wire serializer (cross-SDK payloads). Same model —
    // consumers that use CborSerializer add this dependency themselves.
    compileOnly("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.17.2")
    testImplementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.17.2")

    // Optional: observability contrib middleware. Consumers that use them add the
    // matching runtime dependency themselves.
    compileOnly("io.micrometer:micrometer-observation:1.13.6")
    testImplementation("io.micrometer:micrometer-observation:1.13.6")
    testImplementation("io.micrometer:micrometer-observation-test:1.13.6")
    compileOnly("io.sentry:sentry:7.14.0")
    testImplementation("io.sentry:sentry:7.14.0")

    // Optional: OIDC id_token validation for dashboard OAuth (Google / generic
    // OIDC). Zero transitive deps. The dashboard degrades to password-only auth
    // when it is absent, so consumers who enable OAuth add it themselves.
    compileOnly("com.nimbusds:nimbus-jose-jwt:10.9.1")
    testImplementation("com.nimbusds:nimbus-jose-jwt:10.9.1")

    // Run the @TaskHandler processor over the tests so the generated companions
    // are exercised end-to-end. Consumers wire it the same way.
    testAnnotationProcessor(project(":processor"))

    // Test-only, and only in this direction: the harness in :test-support is
    // a Java restatement of rules that live in the core, so it is pinned by a
    // parity test that runs one task body over it and over a real worker. That
    // test needs both, and the real-worker half is here.
    testImplementation(project(":test-support"))

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// --- Native (Rust cdylib) -------------------------------------------------
// The main jar is native-free: each platform's cdylib ships as a classifier
// artifact of the same coordinate (e.g. `flexiq-<v>-linux-x86_64.jar`), and
// NativeLoader resolves the right one from the classpath at runtime. The
// classifier-free jar stays usable for consumers that supply their own build
// via `-Dflexiq.native.lib`.

val crateDir = layout.projectDirectory.dir("../../crates/flexiq-java")
val cargoTargetDir = layout.projectDirectory.dir("../../target")
val nativeStaging = layout.buildDirectory.dir("native")

/**
 * Every platform published as a native classifier artifact. Mirrored by
 * `NativeLoader.PUBLISHED_PLATFORMS`, which fails with a clear message on anything
 * else (Windows on ARM included) instead of reaching for a binary that isn't there.
 */
val nativePlatforms = listOf("linux-x86_64", "linux-aarch64", "osx-x86_64", "osx-aarch64", "windows-x86_64")

// Build the native library for the local platform.
val cargoBuild = tasks.register<Exec>("cargoBuild") {
    workingDir = crateDir.asFile
    commandLine("cargo", "build", "--release", "--features", "postgres,redis,workflows,mesh")
}

// Stage the built library under its platform-classifier resource path.
val copyNative = tasks.register<Copy>("copyNative") {
    dependsOn(cargoBuild)
    from(cargoTargetDir.dir("release")) {
        include("libflexiq_java.so", "libflexiq_java.dylib", "flexiq_java.dll")
    }
    into(nativeStaging.map { it.dir("org/byteveda/flexiq/native/${platformClassifier()}") })
}

// Tests load the native through the same classpath lookup consumers use, so
// put the staged dir (not the jar) on the test runtime classpath.
tasks.named<Test>("test") {
    dependsOn(copyNative)
    classpath += files(nativeStaging)
}

// Sibling modules that exercise the native at runtime (graalvm-smoke) consume
// the staged dir through this configuration instead of the runtime jar.
val nativeRuntime by configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
}
artifacts.add(nativeRuntime.name, nativeStaging) { builtBy(copyNative) }

fun stagedNativeDir(platform: String) =
    nativeStaging.get().dir("org/byteveda/flexiq/native/$platform").asFile

/**
 * Platforms this invocation can actually package. CI stages every platform's
 * binary under build/native before calling Gradle, so it gets all five; a
 * developer machine only ever has the host's, and building a jar for a binary
 * that cannot exist locally would block `publishToMavenLocal` outright.
 * `verifyNativeJars` is what keeps a real publish complete.
 */
val publishablePlatforms = nativePlatforms.filter { platform ->
    platform == platformClassifier() || !stagedNativeDir(platform).listFiles().isNullOrEmpty()
}

// One classifier jar per platform, packaging exactly that platform's library.
val nativeJars = publishablePlatforms.map { platform ->
    val camel = platform.split("-").joinToString("") { part -> part.replaceFirstChar(Char::uppercase) }
    tasks.register<Jar>("nativeJar$camel") {
        archiveClassifier.set(platform)
        from(nativeStaging) { include("org/byteveda/flexiq/native/$platform/**") }
        if (platform == platformClassifier()) {
            dependsOn(copyNative)
        }
        doFirst {
            val staged = nativeStaging.get().dir("org/byteveda/flexiq/native/$platform").asFile
            if (staged.listFiles().isNullOrEmpty()) {
                throw GradleException(
                    "no native library staged for $platform under build/native — " +
                        "CI stages prebuilt binaries for all platforms; locally only " +
                        "the host platform is available (:copyNative)"
                )
            }
        }
    }
}

tasks.register("nativeJars") {
    description = "Builds every per-platform native classifier jar."
    dependsOn(nativeJars)
}

/**
 * A release must carry every platform. Local builds legitimately package only
 * the host, so this is asserted on the way out to a real repository rather than
 * when the jars are built — otherwise no one could publish to Maven local.
 */
val verifyNativeJars = tasks.register("verifyNativeJars") {
    description = "Fails unless a native library is staged for every published platform."
    doLast {
        val missing = nativePlatforms.filter { stagedNativeDir(it).listFiles().isNullOrEmpty() }
        if (missing.isNotEmpty()) {
            throw GradleException(
                "no native library staged for ${missing.joinToString(", ")} under build/native — " +
                    "a release must package all of $nativePlatforms; stage the prebuilt " +
                    "binaries before publishing"
            )
        }
    }
}

// Remote publishes only — `publishToMavenLocal` is how a developer consumes a
// local build and has no business demanding five platforms.
tasks.matching { it.name.startsWith("publishAllPublicationsTo") || it.name.contains("MavenCentral") }
    .configureEach { dependsOn(verifyNativeJars) }

publishing {
    publications.withType<MavenPublication>().configureEach {
        nativeJars.forEach { artifact(it) }
    }
}

// Sources jar ships sources only — the dashboard SPA ships in the main jar.
tasks.withType<Jar>().matching { it.name == "sourcesJar" }.configureEach {
    exclude("org/byteveda/flexiq/dashboard/**")
}

// --- FFM fast-path overlay (Multi-Release JAR) ----------------------------
// Base classes target 17 (JNI transport + the fallback). On a build JDK >= 22 we
// also compile the Project Panama (FFM) transport at --release 22 and package it
// under META-INF/versions/22; the runtime selects it on JDK 22+ (see
// NativeTransport.create), else stays on JNI. Older build JDKs simply omit the
// overlay — same public API, faster impl where available (not feature divergence).
val ffmCapable = JavaVersion.current() >= JavaVersion.VERSION_22

if (ffmCapable) {
    val java22 by sourceSets.creating {
        java.srcDir("src/main/java22")
        compileClasspath += sourceSets["main"].output
        runtimeClasspath += sourceSets["main"].output
    }

    tasks.named<JavaCompile>("compileJava22Java") {
        options.release.set(22)
    }

    tasks.named<Jar>("jar") {
        manifest {
            attributes(
                "Multi-Release" to "true",
                // Only takes effect when the jar is run directly (java -jar); it does
                // NOT cover consumers that depend on the SDK on their classpath — they
                // must pass --enable-native-access=ALL-UNNAMED themselves (see README).
                // Restricted FFM methods only warn today but a future JDK denies them.
                "Enable-Native-Access" to "ALL-UNNAMED",
            )
        }
        into("META-INF/versions/22") { from(java22.output) }
    }

    // Exercise the FFM transport in the test suite on this JDK 22+ build. Set on
    // the test task directly: mutating the source set's runtimeClasspath here is
    // too late (the java plugin has already captured the test task's classpath).
    tasks.named<Test>("test") {
        classpath += java22.output
        // Silence (and forward-proof against) the restricted-native-access warning.
        jvmArgs("--enable-native-access=ALL-UNNAMED")
    }
}

tasks.test {
    useJUnitPlatform()
}

/** Resource classifier for the local platform, e.g. "linux-x86_64". */
fun platformClassifier(): String {
    val os = System.getProperty("os.name").lowercase()
    val arch = System.getProperty("os.arch").lowercase()
    val osDir = when {
        os.contains("win") -> "windows"
        os.contains("mac") || os.contains("darwin") -> "osx"
        else -> "linux"
    }
    val archDir = when (arch) {
        "amd64", "x86_64" -> "x86_64"
        "aarch64", "arm64" -> "aarch64"
        else -> arch
    }
    return "$osDir-$archDir"
}
