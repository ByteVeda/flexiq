package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * An enqueue was rejected by a registered predicate (gate), so no job was
 * created.
 */
public class PredicateRejectedException extends FlexiQException {
    public PredicateRejectedException(String message) {
        super(message);
    }
}
