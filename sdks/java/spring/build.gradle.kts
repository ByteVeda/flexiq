// taskito-spring: a Spring Boot 3 starter that auto-configures a Taskito bean.
// Spring Boot 3 requires Java 17, which matches the SDK baseline.
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
    coordinates(group.toString(), "taskito-spring", version.toString())
    pom {
        name.set("Taskito Spring")
        description.set("Spring Boot 3 starter that auto-configures a Taskito bean.")
        url.set("https://github.com/ByteVeda/taskito")
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
            url.set("https://github.com/ByteVeda/taskito")
            connection.set("scm:git:https://github.com/ByteVeda/taskito.git")
            developerConnection.set("scm:git:ssh://git@github.com/ByteVeda/taskito.git")
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile>().configureEach { options.release.set(17) }

repositories { mavenCentral() }

val springBoot = "3.3.5"

dependencies {
    errorprone("com.google.errorprone:error_prone_core:${property("errorProneVersion")}")
    errorprone("com.uber.nullaway:nullaway:${property("nullAwayVersion")}")
    api(project(":"))
    // The runtime jar is native-free; tests auto-configure a real Taskito bean,
    // so stage the host-platform library from the root build.
    testRuntimeOnly(project(mapOf("path" to ":", "configuration" to "nativeRuntime")))
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:$springBoot")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:$springBoot")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.springframework.boot:spring-boot-autoconfigure:$springBoot")
    testImplementation("org.springframework.boot:spring-boot-test:$springBoot")
    testImplementation("org.springframework:spring-test:6.1.14")
    // AssertableApplicationContext implements AssertJ's AssertProvider, so the
    // type must be on the test classpath even though we assert with JUnit.
    testImplementation("org.assertj:assertj-core:3.25.3")
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

tasks.test { useJUnitPlatform() }

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
