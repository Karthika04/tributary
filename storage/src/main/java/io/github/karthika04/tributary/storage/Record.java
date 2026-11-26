package io.github.karthika04.tributary.storage;

import java.util.Arrays;

/**
 * One message as it is stored in the log.
 *
 * <p>The log assigns {@code offset} when the message is appended: the first message
 * is offset 0, the next is 1, and so on. Offsets never change and are never reused,
 * which is what lets a consumer say "give me everything after offset 41".
 *
 * <p>{@code key} may be null (a message with no key). {@code value} may be empty but
 * never null. The arrays are not copied, for speed, so callers must not change them
 * after handing them over.
 */
public record Record(long offset, long timestamp, byte[] key, byte[] value) {

    public Record {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        if (value == null) {
            throw new IllegalArgumentException("value must not be null (use an empty array)");
        }
    }

    // A record's generated equals() compares arrays by reference, so two records with
    // the same bytes would not be equal. We compare the array contents instead.
    @Override
    public boolean equals(Object o) {
        return o instanceof Record other
                && offset == other.offset
                && timestamp == other.timestamp
                && Arrays.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        int h = Long.hashCode(offset);
        h = 31 * h + Long.hashCode(timestamp);
        h = 31 * h + Arrays.hashCode(key);
        h = 31 * h + Arrays.hashCode(value);
        return h;
    }

    @Override
    public String toString() {
        return "Record[offset=" + offset
                + ", timestamp=" + timestamp
                + ", keyBytes=" + (key == null ? "null" : key.length)
                + ", valueBytes=" + value.length + "]";
    }
}
