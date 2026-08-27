package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A job's structured {@code notes} violated the bounded-annotation contract — too many
 * fields, a key or value that is too long, excessive nesting, an unsupported value type,
 * or an encoded size over the limit. The message names the offending key or constraint.
 */
public class NotesValidationException extends FlexiQException {
    /**
     * A notes map that broke the bounded-annotation contract.
     *
     * @param message the offending key or the constraint it broke
     */
    public NotesValidationException(String message) {
        super(message);
    }

    /**
     * A notes map rejected because encoding it failed.
     *
     * @param message the offending key or the constraint it broke
     * @param cause the encoding failure underneath
     */
    public NotesValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
