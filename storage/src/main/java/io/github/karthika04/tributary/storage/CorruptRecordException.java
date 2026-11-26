package io.github.karthika04.tributary.storage;

/**
 * Thrown when bytes on disk do not form a valid record: the checksum does not match,
 * a length field is impossible, or the record was only partly written.
 */
public class CorruptRecordException extends RuntimeException {

    public CorruptRecordException(String message) {
        super(message);
    }
}
