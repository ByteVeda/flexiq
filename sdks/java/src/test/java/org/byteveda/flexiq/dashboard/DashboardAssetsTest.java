package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DashboardAssetsTest {

    @Test
    void extractsSpaTreeAndIgnoresUnrelatedEntries(@TempDir Path tmp) throws IOException {
        Path jar = buildSpaJar(tmp, "spa.jar", "<!doctype html>v1");
        Path workdir = Files.createDirectories(tmp.resolve("work"));

        Path target = DashboardAssets.extractFromJar(jar, workdir);

        assertEquals("<!doctype html>v1", Files.readString(target.resolve("index.html")));
        assertEquals("console.log(1)", Files.readString(target.resolve("assets/app.js")));
        // Entries outside the dashboard prefix must not leak into the output.
        assertFalse(Files.exists(target.resolve("other.txt")));
        assertFalse(Files.exists(target.resolve("../other.txt")));
    }

    @Test
    void reusesTheSameDirectoryOnReExtract(@TempDir Path tmp) throws IOException {
        Path jar = buildSpaJar(tmp, "spa.jar", "<!doctype html>v1");
        Path workdir = Files.createDirectories(tmp.resolve("work"));

        Path first = DashboardAssets.extractFromJar(jar, workdir);
        Path second = DashboardAssets.extractFromJar(jar, workdir);

        assertEquals(first, second);
        assertTrue(Files.isRegularFile(second.resolve("index.html")));
    }

    @Test
    void differentSpaContentGetsADistinctDirectory(@TempDir Path tmp) throws IOException {
        Path workdir = Files.createDirectories(tmp.resolve("work"));

        Path v1 = DashboardAssets.extractFromJar(buildSpaJar(tmp, "v1.jar", "<!doctype html>v1"), workdir);
        Path v2 = DashboardAssets.extractFromJar(buildSpaJar(tmp, "v2.jar", "<!doctype html>v2"), workdir);

        assertNotEquals(v1, v2);
    }

    @Test
    void dirOverridePropertyShortCircuitsDiscovery(@TempDir Path tmp) {
        String key = "flexiq.dashboard.dir";
        System.setProperty(key, tmp.toString());
        try {
            assertEquals(Paths.get(tmp.toString()).normalize(), DashboardAssets.resolveOrNull());
        } finally {
            System.clearProperty(key);
        }
    }

    /** A jar with a fake SPA under the dashboard prefix plus one unrelated entry. */
    private static Path buildSpaJar(Path dir, String name, String indexBody) throws IOException {
        Path jar = dir.resolve(name);
        try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jar))) {
            write(jos, "org/byteveda/flexiq/dashboard/index.html", indexBody);
            write(jos, "org/byteveda/flexiq/dashboard/assets/app.js", "console.log(1)");
            write(jos, "org/byteveda/flexiq/other.txt", "unrelated");
        }
        return jar;
    }

    private static void write(JarOutputStream jos, String name, String body) throws IOException {
        jos.putNextEntry(new JarEntry(name));
        jos.write(body.getBytes(StandardCharsets.UTF_8));
        jos.closeEntry();
    }
}
