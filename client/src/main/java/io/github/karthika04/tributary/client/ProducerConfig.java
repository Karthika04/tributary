package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Compression;

/**
 * @param lingerMs       how long to wait for more records before sending a batch.
 *                       A few milliseconds of delay lets many records share one
 *                       request, which raises throughput a lot
 * @param batchBytes     send a batch as soon as it reaches this size, without waiting
 * @param maxRetries     how many times to retry a batch after a retriable error
 * @param retryBackoffMs wait before the first retry; doubles on each retry (max 5 s)
 * @param compression    how batches are compressed on the wire
 */
public record ProducerConfig(ClientConfig client, int lingerMs, int batchBytes, int maxRetries,
                             long retryBackoffMs, Compression compression) {

    public ProducerConfig {
        if (lingerMs < 0 || batchBytes <= 0 || maxRetries < 0 || retryBackoffMs < 0) {
            throw new IllegalArgumentException("invalid producer settings");
        }
    }

    public static ProducerConfig of(ClientConfig client) {
        return new ProducerConfig(client, 5, 64 * 1024, 5, 100, Compression.NONE);
    }

    public ProducerConfig withLingerMs(int value) {
        return new ProducerConfig(client, value, batchBytes, maxRetries, retryBackoffMs, compression);
    }

    public ProducerConfig withBatchBytes(int value) {
        return new ProducerConfig(client, lingerMs, value, maxRetries, retryBackoffMs, compression);
    }

    public ProducerConfig withRetries(int maxRetries, long retryBackoffMs) {
        return new ProducerConfig(client, lingerMs, batchBytes, maxRetries, retryBackoffMs, compression);
    }

    public ProducerConfig withCompression(Compression value) {
        return new ProducerConfig(client, lingerMs, batchBytes, maxRetries, retryBackoffMs, value);
    }
}
