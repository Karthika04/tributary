package io.github.karthika04.tributary.storage;

/**
 * A record, or a whole batch, is bigger than the log accepts. Nothing from the
 * batch was written.
 */
public class RecordTooLargeException extends IllegalArgumentException {

    public RecordTooLargeException(String message) {
        super(message);
    }
}
