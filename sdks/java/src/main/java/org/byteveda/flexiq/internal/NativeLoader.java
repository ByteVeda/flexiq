package org.byteveda.flexiq.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Set;

/**
 * Loads the native FlexiQ library.
 *
 * <p>Order: an explicit {@code -Dflexiq.native.lib=/path} override, otherwise
 * the platform binary on the classpath (shipped in the matching per-platform
 * classifier jar) is extracted and loaded. Extraction is
 * content-addressed — the target file name embeds a hash of the bytes — so it is
 * safe under concurrent processes (atomic move), avoids re-extraction across
 * runs, and never loads a stale binary from a previous build. Honor
 * {@code -Dflexiq.native.workdir} for hardened/noexec {@code /tmp} environments.
 *
 * <p>A platform with no published classifier artifact — Windows on ARM, or any OS
 * beyond Linux/macOS/Windows — fails with an {@link UnsatisfiedLinkError} naming
 * the detected platform. Such platforms are supported by building the crate
 * locally and pointing {@code -Dflexiq.native.lib} at the result.
 */
public final class NativeLoader {
    private static final String LIB = "flexiq_java";
    private static final String WORKDIR_PROPERTY = "flexiq.native.workdir";

    /**
     * Platforms a classifier artifact is published for. Mirrors
     * {@code nativePlatforms} in {@code sdks/java/build.gradle.kts} — a platform
     * added there must be added here for the loader to look for it.
     */
    private static final Set<String> PUBLISHED_PLATFORMS =
            Set.of("linux-x86_64", "linux-aarch64", "osx-x86_64", "osx-aarch64", "windows-x86_64");

    private static boolean loaded;

    private NativeLoader() {}

    /** Load the library once per process. */
    public static synchronized void load() {
        if (loaded) {
            return;
        }
        String override = System.getProperty("flexiq.native.lib");
        System.load(override != null ? override : extractBundled());
        loaded = true;
    }

    private static String extractBundled() {
        byte[] bytes = readResource();
        String expected = sha256Hex(bytes);
        try {
            Path dir = secureWorkdir();
            Path target =
                    dir.resolve(platformDir() + "-" + expected.substring(0, 16) + "-" + System.mapLibraryName(LIB));
            if (!isTrusted(target, bytes.length, expected)) {
                materialize(dir, target, bytes);
                // Fail closed: never System.load a file we haven't verified
                // byte-for-byte. A racing writer could have left an untrusted file
                // at the target after we dropped ours.
                if (!isTrusted(target, bytes.length, expected)) {
                    throw new IOException("native library failed integrity check at " + target);
                }
            }
            return target.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("failed to extract native library: " + e.getMessage());
        }
    }

    /**
     * A pre-existing file is reused only if it is a real (non-symlink) regular
     * file of the expected length whose bytes hash to {@code expected}. This
     * rejects a swapped or symlinked file planted in a shared workdir.
     */
    private static boolean isTrusted(Path target, long expectedLength, String expected) throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.size(target) != expectedLength) {
            return false;
        }
        return sha256Hex(Files.readAllBytes(target)).equals(expected);
    }

    /** Write to a unique temp file, then atomically move it into place. */
    private static void materialize(Path dir, Path target, byte[] bytes) throws IOException {
        Path temp = Files.createTempFile(dir, ".tmp-", null);
        boolean moved = false;
        try {
            Files.write(temp, bytes);
            // We only reach here after verification failed, so drop any untrusted
            // or symlinked file squatting the target. NOFOLLOW deletes the link
            // itself, not whatever it points at.
            Files.deleteIfExists(target);
            try {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
                moved = true;
            } catch (FileAlreadyExistsException raced) {
                moved = false; // another process won; the caller re-verifies the target
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
                moved = true;
            }
        } finally {
            if (!moved) {
                Files.deleteIfExists(temp); // don't leave .tmp-* behind on failure or loss
            }
        }
    }

    private static byte[] readResource() {
        String platform = requirePublishedPlatform();
        String resource = "/org/byteveda/flexiq/native/" + platform + "/" + System.mapLibraryName(LIB);
        try (InputStream in = NativeLoader.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new UnsatisfiedLinkError("no native library for platform '" + platform + "' on the"
                        + " classpath (" + resource + "); add the classifier artifact"
                        + " org.byteveda:flexiq:<version>:" + platform
                        + " as a runtime dependency, or set -Dflexiq.native.lib=/path/to/library");
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("failed to read native library: " + e.getMessage());
        }
    }

    /**
     * Fail with the detected platform rather than reaching for a binary that was
     * never published for it — a classifier jar is only ever built for
     * {@link #PUBLISHED_PLATFORMS}. Building the crate locally and pointing
     * {@code -Dflexiq.native.lib} at the result stays supported everywhere.
     */
    private static String requirePublishedPlatform() {
        String platform = platformDir();
        if (!isPublished(platform)) {
            throw new UnsatisfiedLinkError("no native library is published for platform '" + platform + "' (os.name="
                    + System.getProperty("os.name", "") + ", os.arch=" + System.getProperty("os.arch", "")
                    + "); published platforms are " + PUBLISHED_PLATFORMS
                    + ". Build the flexiq-java crate for this platform and set"
                    + " -Dflexiq.native.lib=/path/to/library");
        }
        return platform;
    }

    /**
     * Per-user extraction directory with owner-only permissions, so other users
     * on a shared {@code /tmp} can't pre-create or swap the file we load.
     */
    private static Path secureWorkdir() throws IOException {
        String configured = System.getProperty(WORKDIR_PROPERTY);
        String base = configured != null ? configured : System.getProperty("java.io.tmpdir");
        Path dir = Paths.get(base).resolve("flexiq-native-" + System.getProperty("user.name", "anon"));
        Files.createDirectories(dir);
        try {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException nonPosix) {
            // Windows etc.: per-user profile dirs are already ACL-restricted.
        }
        return dir;
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
            throw new UnsatisfiedLinkError("hashing failed: " + e.getMessage());
        }
    }

    /**
     * Resource classifier directory for the running platform, e.g.
     * {@code linux-x86_64}. An OS or architecture FlexiQ does not publish for
     * still gets its own honest name (e.g. {@code freebsd-x86_64}) so the failure
     * says which platform it is instead of silently trying a Linux binary.
     */
    static String platformDir() {
        return platformDir(System.getProperty("os.name", ""), System.getProperty("os.arch", ""));
    }

    /** {@link #platformDir()} over explicit values, so the mapping is testable. */
    static String platformDir(String osName, String osArch) {
        return osToken(osName.toLowerCase(Locale.ROOT)) + "-" + archToken(osArch.toLowerCase(Locale.ROOT));
    }

    /** Whether a classifier artifact exists for {@code platform}. */
    static boolean isPublished(String platform) {
        return PUBLISHED_PLATFORMS.contains(platform);
    }

    private static String osToken(String os) {
        if (os.contains("win")) {
            return "windows";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return "osx";
        }
        if (os.contains("linux")) {
            return "linux";
        }
        return sanitize(os);
    }

    private static String archToken(String arch) {
        if (arch.equals("amd64") || arch.equals("x86_64")) {
            return "x86_64";
        }
        if (arch.equals("aarch64") || arch.equals("arm64")) {
            return "aarch64";
        }
        return sanitize(arch);
    }

    /** Keep the token usable as a resource path and file-name segment. */
    private static String sanitize(String raw) {
        String token = raw.replaceAll("[^a-z0-9]+", "");
        return token.isEmpty() ? "unknown" : token;
    }
}
