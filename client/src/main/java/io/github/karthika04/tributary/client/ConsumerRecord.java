package io.github.karthika04.tributary.client;

import java.nio.charset.StandardCharsets;

/** A record read by a {@link Consumer}, with where it came from. */
public record ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value) {

    /** The key as UTF-8 text, or null if there is no key. */
    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    /** The value as UTF-8 text. */
    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset;
    }
}
