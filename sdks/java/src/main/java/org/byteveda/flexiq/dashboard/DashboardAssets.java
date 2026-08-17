package org.byteveda.flexiq.dashboard;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.byteveda.flexiq.FlexiQException;
import org.jspecify.annotations.Nullable;

/**
 * Locates the dashboard SPA that ships inside the flexiq jar and hands back a
 * filesystem directory the {@link DashboardServer} can serve — so callers never
 * pass a path, mirroring how the Python/Node SDKs auto-discover their bundled
 * assets.
 *
 * <p>When the classpath is a real jar the assets are extracted to a per-user,
 * content-addressed directory (the name embeds a hash of {@code index.html}, so
 * a rebuilt SPA gets a fresh directory and a stale one is never served). When
 * the classpath is exploded (dev/tests) the resource directory is served in
 * place. {@code -Dflexiq.dashboard.dir=/path} overrides discovery.
 */
final class DashboardAssets {
    private static final String ROOT = "org/byteveda/flexiq/dashboard";
    private static final String SENTINEL = "/" + ROOT + "/index.html";
    private static final String DIR_OVERRIDE = "flexiq.dashboard.dir";

    private static boolean attempted;
    private static @Nullable Path resolved;

    private DashboardAssets() {}

    /** The SPA root, or {@code null} when no SPA is bundled (then only /api/* responds). */
    static synchronized @Nullable Path resolveOrNull() {
        String override = System.getProperty(DIR_OVERRIDE);
        if (override != null) {
            return Paths.get(override).normalize();
        }
        if (!attempted) {
            attempted = true;
            resolved = locate();
        }
        return resolved;
    }

    private static @Nullable Path locate() {
        URL index = DashboardAssets.class.getResource(SENTINEL);
        if (index == null) {
            return null;
        }
        try {
            URI uri = index.toURI();
            if ("file".equals(uri.getScheme())) {
                return Paths.get(uri).getParent(); // exploded classpath (dev/tests)
            }
            return extractFromJar(jarPathOf(index), secureWorkdir());
        } catch (URISyntaxException | IOException e) {
            throw new FlexiQException("failed to load bundled dashboard assets", e);
        }
    }

    /**
     * Extract the SPA tree from {@code jarPath} into a content-addressed directory
     * under {@code workdir}, returning that directory. Pure in its inputs (no
     * classloader/global state) so it can be exercised against a synthetic jar.
     */
    static Path extractFromJar(Path jarPath, Path workdir) throws IOException {
        String prefix = ROOT + "/";
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry indexEntry = jar.getJarEntry(prefix + "index.html");
            if (indexEntry == null) {
                throw new IOException("no " + prefix + "index.html in " + jarPath);
            }
            // index.html embeds Vite's hashed asset names, so its bytes change
            // whenever any asset does — a cheap, stable content key for the tree.
            byte[] indexBytes;
            try (InputStream in = jar.getInputStream(indexEntry)) {
                indexBytes = in.readAllBytes();
            }
            Path target =
                    workdir.resolve("flexiq-dashboard-" + sha256Hex(indexBytes).substring(0, 16));
            if (Files.isRegularFile(target.resolve("index.html"))) {
                return target; // extracted already, by us or a prior run
            }
            materialize(jar, prefix, workdir, target);
            return target;
        }
    }

    /**
     * Extract into a private staging dir, then publish the whole tree to {@code target}
     * with a single atomic directory move — so a reader never sees a half-written tree,
     * and concurrent first-runs converge (the loser reuses the winner's identical copy).
     */
    private static void materialize(JarFile jar, String prefix, Path workdir, Path target) throws IOException {
        Path staging = Files.createTempDirectory(workdir, ".dash-");
        try {
            extractTree(jar, prefix, staging);
            publish(staging, target);
        } finally {
            deleteRecursive(staging);
        }
        // index.html appears only via the all-or-nothing move, so its presence is a
        // complete-extraction signal — fail loudly if the publish silently no-op'd.
        if (!Files.isRegularFile(target.resolve("index.html"))) {
            throw new IOException("dashboard assets failed to materialize at " + target);
        }
    }

    private static void publish(Path staging, Path target) throws IOException {
        try {
            Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException crossStore) {
            // Rare filesystem that can't rename atomically; copy in best-effort.
            if (!Files.isDirectory(target)) {
                copyTree(staging, target);
            }
        } catch (FileSystemException raced) {
            // A concurrent run published first (target exists, non-empty). Reuse it;
            // anything else is a genuine failure and must propagate.
            if (!Files.isDirectory(target)) {
                throw raced;
            }
        }
    }

    private static void extractTree(JarFile jar, String prefix, Path dest) throws IOException {
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            // Jar entry names always use '/', so this is platform-independent.
            if (entry.isDirectory() || !entry.getName().startsWith(prefix)) {
                continue;
            }
            Path out = dest.resolve(entry.getName().substring(prefix.length())).normalize();
            if (!out.startsWith(dest)) {
                continue; // zip-slip guard
            }
            Files.createDirectories(out.getParent());
            try (InputStream in = jar.getInputStream(entry)) {
                Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static void copyTree(Path src, Path dst) throws IOException {
        try (Stream<Path> walk = Files.walk(src)) {
            for (Path path : (Iterable<Path>) walk::iterator) {
                Path out = dst.resolve(src.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(out);
                } else {
                    Files.createDirectories(out.getParent());
                    Files.copy(path, out, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static Path jarPathOf(URL index) throws IOException {
        JarURLConnection conn = (JarURLConnection) index.openConnection();
        try {
            return Paths.get(conn.getJarFileURL().toURI());
        } catch (URISyntaxException e) {
            throw new IOException("bad jar url: " + index, e);
        }
    }

    /** Per-user extraction dir with owner-only perms, like the native loader's. */
    private static Path secureWorkdir() throws IOException {
        Path dir = Paths.get(System.getProperty("java.io.tmpdir"))
                .resolve("flexiq-dashboard-" + System.getProperty("user.name", "anon"));
        Files.createDirectories(dir);
        try {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException nonPosix) {
            // Windows etc.: per-user profile dirs are already ACL-restricted.
        }
        return dir;
    }

    private static void deleteRecursive(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                    // best-effort cleanup of a staging dir
                }
            });
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16));
                hex.append(Character.forDigit(b & 0xf, 16));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new FlexiQException("hashing dashboard assets failed", e);
        }
    }
}
