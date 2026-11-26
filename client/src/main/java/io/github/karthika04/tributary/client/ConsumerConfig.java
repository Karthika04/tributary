package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Compression;

/**
 * @param maxPollRecords  most records one {@code poll()} returns
 * @param fetchMaxWaitMs  how long the broker may hold a fetch open waiting for new data
 * @param autoOffsetReset where to start when there is no position yet, or when the
 *                        position was deleted by retention
 * @param fetchCompression how the broker should compress records it sends back
 */
public record ConsumerConfig(ClientConfig client, int maxPollRecords, int fetchMaxWaitMs,
                             AutoOffsetReset autoOffsetReset, Compression fetchCompression) {

    /** Where a consumer with no saved position starts reading. */
    public enum AutoOffsetReset {
        /** From the oldest record still stored. */
        EARLIEST,
        /** Only records written from now on. */
        LATEST
    }

    public ConsumerConfig {
        if (maxPollRecords <= 0 || fetchMaxWaitMs < 0) {
            throw new IllegalArgumentException("invalid consumer settings");
        }
    }

    public static ConsumerConfig of(ClientConfig client) {
        return new ConsumerConfig(client, 500, 500, AutoOffsetReset.EARLIEST, Compression.NONE);
    }

    public ConsumerConfig withMaxPollRecords(int value) {
        return new ConsumerConfig(client, value, fetchMaxWaitMs, autoOffsetReset, fetchCompression);
    }

    public ConsumerConfig withFetchMaxWaitMs(int value) {
        return new ConsumerConfig(client, maxPollRecords, value, autoOffsetReset, fetchCompression);
    }

    public ConsumerConfig withAutoOffsetReset(AutoOffsetReset value) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, value, fetchCompression);
    }

    public ConsumerConfig withFetchCompression(Compression value) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, autoOffsetReset, value);
    }
}
