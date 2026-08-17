package org.byteveda.flexiq.dashboard.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.auth.AuthStore;
import org.byteveda.flexiq.dashboard.auth.Role;
import org.byteveda.flexiq.dashboard.support.Json;
import org.byteveda.flexiq.errors.SettingConflictException;
import org.byteveda.flexiq.internal.SettingsDocument;
import org.junit.jupiter.api.Test;

/**
 * Concurrent edits to the settings-backed feature stores must not be lost.
 *
 * <p>Every store keeps a whole JSON document under one settings key. A
 * read-then-write drops a concurrent edit wholesale, and more than one dashboard
 * replica against one backend is a supported deployment — so each store writes
 * conditionally on the value it read and retries when it loses the race.
 *
 * <p>The races here are deterministic: {@link RacingSettings} runs a supplied
 * writer immediately after a read, which is exactly the window a read-then-write
 * loses.
 */
class SettingsDocumentTest {

    /** A settings view that lets another writer in right after each read. */
    private static final class RacingSettings implements SettingsAccess {
        private final SettingsAccess delegate;
        private final List<Supplier<?>> interlopers;
        private int reads;

        RacingSettings(SettingsAccess delegate, List<Supplier<?>> interlopers) {
            this.delegate = delegate;
            this.interlopers = new ArrayList<>(interlopers);
        }

        @Override
        public Optional<String> getSetting(String key) {
            Optional<String> value = delegate.getSetting(key);
            reads++;
            if (!interlopers.isEmpty()) {
                interlopers.remove(0).get();
            }
            return value;
        }

        @Override
        public void setSetting(String key, String value) {
            delegate.setSetting(key, value);
        }

        @Override
        public boolean setSettingIf(String key, Optional<String> expected, String value) {
            return delegate.setSettingIf(key, expected, value);
        }

        @Override
        public boolean deleteSetting(String key) {
            return delegate.deleteSetting(key);
        }

        @Override
        public Map<String, String> listSettings() {
            return delegate.listSettings();
        }

        @Override
        public List<String> reservedPrefixes() {
            return delegate.reservedPrefixes();
        }
    }

    private static final SettingsDocument.Codec<List<String>> LIST_CODEC = SettingsDocument.codec(
            raw -> raw.map(json -> new ArrayList<>(Json.parseStringList(json))).orElseGet(ArrayList::new),
            Json::toString);

    // ---- the storage primitive --------------------------------------------

    @Test
    void aStaleExpectationLoses() {
        InMemorySettings settings = new InMemorySettings();
        settings.setSetting("k", "v1");

        assertFalse(settings.setSettingIf("k", Optional.of("stale"), "v2"));
        assertEquals(Optional.of("v1"), settings.getSetting("k"));

        assertTrue(settings.setSettingIf("k", Optional.of("v1"), "v2"));
        assertEquals(Optional.of("v2"), settings.getSetting("k"));
    }

    @Test
    void expectingUnsetInsertsExactlyOnce() {
        InMemorySettings settings = new InMemorySettings();

        assertTrue(settings.setSettingIf("k", Optional.empty(), "first"));
        assertFalse(settings.setSettingIf("k", Optional.empty(), "second"));
        assertEquals(Optional.of("first"), settings.getSetting("k"));
    }

    // ---- the retry helper --------------------------------------------------

    @Test
    void aNoOpMutationOnAMissingKeyWritesNothing() {
        // The skip compares the new encoding against the *document as decoded*,
        // not the raw stored value: on a missing key the raw is empty while the
        // encoding is `[]`, so comparing to the raw would write a row for it.
        InMemorySettings settings = new InMemorySettings();

        boolean changed = SettingsDocument.update(settings, "missing", LIST_CODEC, names -> names.remove("absent"));

        assertFalse(changed);
        assertEquals(Optional.empty(), settings.getSetting("missing"));
    }

    @Test
    void updateRetriesUntilItWins() {
        InMemorySettings settings = new InMemorySettings();
        RacingSettings racing = new RacingSettings(settings, List.of(() -> {
            settings.setSetting("k", "[\"interloper\"]");
            return null;
        }));

        SettingsDocument.update(racing, "k", LIST_CODEC, names -> names.add("mine"));

        assertEquals(2, racing.reads, "the first attempt must lose and re-read");
        assertEquals(Optional.of("[\"interloper\",\"mine\"]"), settings.getSetting("k"));
    }

    @Test
    void updateGivesUpAfterTheAttemptBound() {
        // A *different* value on every read, so no attempt can ever win.
        InMemorySettings settings = new InMemorySettings();
        int[] tick = {0};
        List<Supplier<?>> interlopers = new ArrayList<>();
        for (int i = 0; i < SettingsDocument.MAX_ATTEMPTS + 5; i++) {
            interlopers.add(() -> {
                settings.setSetting("k", "[" + tick[0]++ + "]");
                return null;
            });
        }
        RacingSettings racing = new RacingSettings(settings, interlopers);

        SettingConflictException raised = assertThrows(
                SettingConflictException.class,
                () -> SettingsDocument.update(racing, "k", LIST_CODEC, names -> names.add("mine")));

        assertEquals("k", raised.key());
        assertEquals(SettingsDocument.MAX_ATTEMPTS, racing.reads);
    }

    // ---- the stores --------------------------------------------------------

    @Test
    void concurrentUserCreationKeepsBoth() {
        InMemorySettings settings = new InMemorySettings();
        AuthStore quiet = new AuthStore(settings);
        AuthStore contender = new AuthStore(
                new RacingSettings(settings, List.of(() -> quiet.createUser("first", "password123", Role.ADMIN))));

        contender.createUser("second", "password123", Role.ADMIN);

        Map<String, Object> users =
                Json.parseMap(settings.getSetting(AuthStore.USERS_KEY).orElseThrow());
        assertTrue(users.containsKey("first"));
        assertTrue(users.containsKey("second"));
    }

    @Test
    void aUserDeletedMidAuthenticateIsNotResurrected() {
        InMemorySettings settings = new InMemorySettings();
        AuthStore quiet = new AuthStore(settings);
        quiet.createUser("alice", "password123", Role.ADMIN);
        AuthStore contender = new AuthStore(new RacingSettings(settings, List.of(() -> {
            quiet.deleteUser("alice");
            return null;
        })));

        // The read that fed the password check saw the row, so the login stands
        // — but stamping last_login_at must not write the whole document back
        // and bring the deleted account with it.
        assertTrue(contender.authenticate("alice", "password123") != null);
        assertTrue(quiet.getUser("alice").isEmpty());
    }

    @Test
    void concurrentOverrideEditsBothSurvive() {
        InMemorySettings settings = new InMemorySettings();
        OverridesStore quiet = new OverridesStore(settings);
        OverridesStore contender = new OverridesStore(
                new RacingSettings(settings, List.of(() -> quiet.putTask("send_email", patch("max_retries", 7)))));

        contender.putTask("send_email", patch("timeout", 30));

        Map<String, Object> merged = quiet.getTask("send_email");
        assertEquals(7, ((Number) merged.get("max_retries")).intValue());
        assertEquals(30, ((Number) merged.get("timeout")).intValue());
    }

    private static Map<String, Object> patch(String key, Object value) {
        Map<String, Object> single = new LinkedHashMap<>();
        single.put(key, value);
        return single;
    }
}
