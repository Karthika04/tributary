package io.github.karthika04.tributary.protocol;

import java.util.Arrays;

/**
 * A record a producer sends. It has no offset or timestamp yet: when the broker
 * stores it, it assigns the next offset and stamps it with the broker's clock
 * (what Kafka calls "LogAppendTime").
 */
public record ProducedRecord(byte[] key, byte[] value) {

    public ProducedRecord {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null (use an empty array)");
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProducedRecord other
                && Arrays.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "ProducedRecord[keyBytes=" + (key == null ? "null" : key.length) + ", valueBytes=" + value.length + "]";
    }
}
