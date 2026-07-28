package org.byteveda.taskito.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NativeLoaderTest {

    @Test
    void platformDirIsClassifierShaped() {
        String dir = NativeLoader.platformDir();
        assertTrue(dir.matches("(linux|osx|windows)-\\S+"), "unexpected classifier: " + dir);
        assertTrue(NativeLoader.isPublished(dir), "the host platform must be published: " + dir);
    }

    @ParameterizedTest
    @CsvSource({
        "Linux, amd64, linux-x86_64",
        "Linux, aarch64, linux-aarch64",
        "Mac OS X, x86_64, osx-x86_64",
        "Mac OS X, arm64, osx-aarch64",
        "Windows 11, amd64, windows-x86_64",
        "Windows 11, aarch64, windows-aarch64",
    })
    void mapsKnownPlatformsToTheirClassifier(String osName, String osArch, String expected) {
        assertEquals(expected, NativeLoader.platformDir(osName, osArch));
    }

    /** A non-Windows, non-Mac OS must not be mistaken for Linux (issue #534). */
    @ParameterizedTest
    @CsvSource({
        "FreeBSD, amd64, freebsd-x86_64",
        "SunOS, sparcv9, sunos-sparcv9",
        "AIX, ppc64, aix-ppc64",
    })
    void namesOtherOperatingSystemsInsteadOfAssumingLinux(String osName, String osArch, String expected) {
        String platform = NativeLoader.platformDir(osName, osArch);
        assertEquals(expected, platform);
        assertFalse(NativeLoader.isPublished(platform), platform + " must not be treated as published");
    }

    /** No binary ships for Windows on ARM, so the loader must not claim one does (issue #533). */
    @Test
    void windowsArm64IsNotPublished() {
        assertFalse(NativeLoader.isPublished("windows-aarch64"));
    }

    @Test
    void unparseablePlatformStillYieldsAUsableToken() {
        assertEquals("unknown-unknown", NativeLoader.platformDir("", ""));
    }
}
