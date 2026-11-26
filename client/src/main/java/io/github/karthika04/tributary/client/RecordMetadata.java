package io.github.karthika04.tributary.client;

/** Where a sent record was stored. */
public record RecordMetadata(String topic, int partition, long offset) {
}
