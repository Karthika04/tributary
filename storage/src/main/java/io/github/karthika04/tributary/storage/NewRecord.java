package io.github.karthika04.tributary.storage;

/**
 * A message that has not been written yet, so it has no offset.
 * The log turns it into a {@link Record} by assigning the next offset.
 */
public record NewRecord(long timestamp, byte[] key, byte[] value) {

    public NewRecord {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null (use an empty array)");
        }
    }

    /** Convenience for a message with no key. */
    public static NewRecord of(long timestamp, byte[] value) {
        return new NewRecord(timestamp, null, value);
    }

    int frameSize() {
        return RecordCodec.frameSize(key, value);
    }
}
