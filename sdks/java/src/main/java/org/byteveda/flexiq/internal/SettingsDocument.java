package org.byteveda.flexiq.internal;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.byteveda.flexiq.errors.SettingConflictException;
import org.byteveda.flexiq.spi.ConditionalSettings;

/**
 * Read-modify-write over the settings key/value store, without losing edits.
 *
 * <p>Every dashboard feature store keeps a whole JSON document under one
 * settings key. A plain read-then-write drops a concurrent edit wholesale — the
 * later writer wins with a document that never saw the earlier one — and more
 * than one dashboard replica against one backend is a supported deployment.
 *
 * <p>{@link #update} closes that: it writes conditionally on the value it read
 * and re-reads on a lost race. Writes here are admin-frequency, so contention is
 * rare and a retry is cheap.
 */
public final class SettingsDocument {

    /**
     * How many times {@link #update} re-reads and retries before giving up.
     *
     * <p>A losing writer only ever loses to a writer that won, so the bound has
     * to clear the number of dashboards that could be editing one document at
     * once. Losing this many in a row is a fault, not contention worth waiting
     * out.
     */
    public static final int MAX_ATTEMPTS = 25;

    private SettingsDocument() {}

    /**
     * Decodes a stored value into a document, and encodes one back.
     *
     * @param <T> the decoded document type
     */
    public interface Codec<T> {

        /**
         * Decode the raw stored value; empty means the key is unset.
         *
         * @param raw the stored row, or empty when the key is unset
         * @return the decoded document; an unset key decodes to the empty document
         */
        T decode(Optional<String> raw);

        /**
         * Encode a document for storage.
         *
         * @param document the document to store
         * @return its serialized form, which the change detection compares on
         */
        String encode(T document);
    }

    /**
     * Applies a change to a document and reports what the caller wanted back.
     *
     * @param <T> the document type
     * @param <R> what the calling operation returns
     */
    @FunctionalInterface
    public interface Mutation<T, R> {

        /**
         * Mutate {@code document} in place and return the call's result.
         *
         * @param document the decoded document, to change in place
         * @return what the calling operation returns; the winning attempt's value is
         *     the one that reaches the caller
         */
        R apply(T document);
    }

    /**
     * Load, mutate and store a document, retrying if someone else wrote first.
     *
     * <p>{@code mutate} must change the document in place and do nothing else:
     * it runs once per attempt. Its return value comes back from the winning
     * attempt, and a mutation that changed nothing writes nothing.
     *
     * @param settings where the document is read and conditionally written
     * @param key the document's key
     * @param codec how the stored value maps to a document and back
     * @param mutate the change to apply; runs once per attempt
     * @param <T> the document type
     * @param <R> what the calling operation returns
     * @return {@code mutate}'s value from the winning attempt
     * @throws SettingConflictException when every attempt lost the race.
     */
    public static <T, R> R update(ConditionalSettings settings, String key, Codec<T> codec, Mutation<T, R> mutate) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Optional<String> stored = settings.getSetting(key);
            T document = codec.decode(stored);
            // Compared against the document as decoded, not against the raw
            // stored value: on a missing key the raw is empty while the encoding
            // is the empty document, so comparing to the raw would read "changed
            // nothing" as a change and write a row for it.
            String before = codec.encode(document);
            R outcome = mutate.apply(document);
            String after = codec.encode(document);
            if (Objects.equals(after, before)) {
                return outcome;
            }
            if (settings.setSettingIf(key, stored, after)) {
                return outcome;
            }
        }
        throw new SettingConflictException(key);
    }

    /**
     * A codec built from a decode and an encode function.
     *
     * @param decode maps the stored row to a document; empty means the key is unset
     * @param encode maps a document back to its stored form
     * @param <T> the document type
     * @return the codec
     */
    public static <T> Codec<T> codec(Function<Optional<String>, T> decode, Function<T, String> encode) {
        return new Codec<T>() {
            @Override
            public T decode(Optional<String> raw) {
                return decode.apply(raw);
            }

            @Override
            public String encode(T document) {
                return encode.apply(document);
            }
        };
    }
}
