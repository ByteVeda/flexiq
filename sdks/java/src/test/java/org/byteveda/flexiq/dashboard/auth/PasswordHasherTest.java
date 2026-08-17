package org.byteveda.flexiq.dashboard.auth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Cross-language PBKDF2 compatibility and format correctness. */
class PasswordHasherTest {

    // RFC-style PBKDF2-HMAC-SHA256 vector: password="password", salt="salt"(=73616c74), 1 iter, 32 bytes.
    private static final String VECTOR_1_ITER =
            "pbkdf2_sha256$1$73616c74$120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b";

    // A real hash produced by Python's hashlib.pbkdf2_hmac at the production 600k iterations.
    private static final String PYTHON_600K = "pbkdf2_sha256$600000$000102030405060708090a0b0c0d0e0f"
            + "$96a5904c2e08c8da42305dbcc5d7cf18ead2636d49f59526b606f26696281473";

    @Test
    void verifiesKnownVector() {
        assertTrue(PasswordHasher.verify("password", VECTOR_1_ITER));
        assertFalse(PasswordHasher.verify("wrong", VECTOR_1_ITER));
    }

    @Test
    void verifiesPythonProducedHash() {
        assertTrue(PasswordHasher.verify("correct horse", PYTHON_600K));
        assertFalse(PasswordHasher.verify("correct horse ", PYTHON_600K));
    }

    @Test
    void hashRoundTrips() {
        String encoded = PasswordHasher.hash("hunter2!!");
        assertTrue(PasswordHasher.verify("hunter2!!", encoded));
        assertFalse(PasswordHasher.verify("hunter2!", encoded));
    }

    @Test
    void hashUsesReferenceFormat() {
        String[] parts = PasswordHasher.hash("some-password").split("\\$", -1);
        assertTrue(parts.length == 4);
        assertTrue("pbkdf2_sha256".equals(parts[0]));
        assertTrue("600000".equals(parts[1]));
        assertTrue(parts[2].length() == 32); // 16-byte salt, hex
        assertTrue(parts[3].length() == 64); // 32-byte hash, hex
    }

    @Test
    void handlesNonAsciiPasswords() {
        String encoded = PasswordHasher.hash("pä$$wörd–😀");
        assertTrue(PasswordHasher.verify("pä$$wörd–😀", encoded));
    }

    @Test
    void rejectsOauthSentinel() {
        assertFalse(PasswordHasher.verify("anything", "oauth:google"));
        assertTrue(PasswordHasher.isOauth("oauth:google"));
        assertFalse(PasswordHasher.isOauth(PasswordHasher.hash("x-password")));
    }

    @Test
    void rejectsMalformedEncodings() {
        assertFalse(PasswordHasher.verify("x", null));
        assertFalse(PasswordHasher.verify("x", ""));
        assertFalse(PasswordHasher.verify("x", "pbkdf2_sha256$1$zz$zz"));
        assertFalse(PasswordHasher.verify("x", "bcrypt$1$aa$bb"));
    }

    @Test
    void dummyVerifyDoesNotThrow() {
        PasswordHasher.dummyVerify("whatever");
    }
}
