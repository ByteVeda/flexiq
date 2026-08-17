// flexiq-test: a pure-Java in-memory QueueBackend for fast unit tests (no JNI,
// no disk). Depends on the runtime for the SPI + models; the runtime does NOT
// depend on it (its own tests live here), so there is no cycle.
plugins {
    `java-library`
    checkstyle
    id("com.diffplug.spotless") version "7.2.1"
    id("com.vanniktech.maven.publish") version "0.37.0"
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
    coordinates(group.toString(), "flexiq-test", version.toString())
    pom {
        name.set("FlexiQ Test Support")
        description.set("In-memory queue backend for testing FlexiQ JVM applications without the native runtime.")
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
    api(project(":"))

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
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

tasks.test {
    useJUnitPlatform()
}
