package io.github.karthika04.tributary.protocol;

import java.util.Arrays;

/** A stored record as returned by a fetch, with the offset the broker gave it. */
public record FetchedRecord(long offset, long timestamp, byte[] key, byte[] value) {

    public FetchedRecord {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof FetchedRecord other
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
        return 31 * h + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "FetchedRecord[offset=" + offset + ", timestamp=" + timestamp
                + ", keyBytes=" + (key == null ? "null" : key.length) + ", valueBytes=" + value.length + "]";
    }
}
