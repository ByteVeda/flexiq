// Standalone compile-time annotation processor for @TaskHandler. Dependency-free
// (reads annotations structurally via javax.lang.model), so it never forms a
// cycle with the runtime it serves. Consumers add it via `annotationProcessor`.
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

plugins {
    `java-library`
    checkstyle
    id("com.diffplug.spotless") version "7.2.1"
    id("com.vanniktech.maven.publish") version "0.37.0"
    id("net.ltgt.errorprone") version "5.1.0"
}

mavenPublishing {
    publishToMavenCentral()
    // Only CI holds the signing key; see the root build for why asking for
    // signatures without one breaks `publishToMavenLocal`.
    if (providers.gradleProperty("signingInMemoryKey").isPresent ||
        providers.gradleProperty("signing.keyId").isPresent
    ) {
        signAllPublications()
    }
    coordinates(group.toString(), "flexiq-processor", version.toString())
    pom {
        name.set("FlexiQ Processor")
        description.set("Compile-time annotation processor generating task-handler bindings for the FlexiQ JVM SDK.")
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

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

repositories {
    mavenCentral()
}

dependencies {
    errorprone("com.google.errorprone:error_prone_core:${property("errorProneVersion")}")
    errorprone("com.uber.nullaway:nullaway:${property("nullAwayVersion")}")
    compileOnly("org.jspecify:jspecify:${property("jspecifyVersion")}")

    // The tests drive the processor through a real javac run (javax.tools), so they
    // need nothing but JUnit — and deliberately not the runtime module, which would
    // reintroduce the cycle this module avoids.
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
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

// Null-safety: @NullMarked sources checked by NullAway, matching the SDK module.
tasks.withType<JavaCompile>().configureEach {
    // `enabled.set(...)`, never `isEnabled = ...`: that assignment resolves
    // against the outer JavaCompile receiver and disables the compile task.
    options.errorprone { enabled.set(false) }
}

tasks.named<JavaCompile>("compileJava") {
    options.errorprone {
        enabled.set(true)
        disableAllChecks.set(true)
        check("NullAway", CheckSeverity.ERROR)
        option("NullAway:OnlyNullMarked", "true")
        option("NullAway:JSpecifyMode", "true")
    }
}
