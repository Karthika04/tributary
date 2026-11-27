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
 * @param bufferBytes    most bytes of records waiting to be sent; beyond this,
 *                       {@code send()} blocks (backpressure), like Kafka's buffer.memory
 * @param maxBlockMs     how long {@code send()} may block for buffer space before it
 *                       fails with {@code PRODUCER_BUFFER_FULL}, like Kafka's max.block.ms
 */
public record ProducerConfig(ClientConfig client, int lingerMs, int batchBytes, int maxRetries,
                             long retryBackoffMs, Compression compression, long bufferBytes, long maxBlockMs) {

    public ProducerConfig {
        if (lingerMs < 0 || batchBytes <= 0 || maxRetries < 0 || retryBackoffMs < 0 || bufferBytes <= 0
                || maxBlockMs < 0) {
            throw new IllegalArgumentException("invalid producer settings");
        }
    }

    public static ProducerConfig of(ClientConfig client) {
        // 1 MB batches: with one request in flight per producer, each request's round trip
        // (replication and a disk flush in a cluster) caps throughput, so bigger batches
        // help a lot (17 -> 45 MB/s on a 3-broker cluster in the benchmarks). lingerMs still
        // sends a partial batch after 5 ms, so latency at low rates is unchanged.
        return new ProducerConfig(client, 5, 1024 * 1024, 5, 100, Compression.NONE, 32L * 1024 * 1024, 60_000);
    }

    public ProducerConfig withLingerMs(int value) {
        return new ProducerConfig(client, value, batchBytes, maxRetries, retryBackoffMs, compression, bufferBytes,
                maxBlockMs);
    }

    public ProducerConfig withBatchBytes(int value) {
        return new ProducerConfig(client, lingerMs, value, maxRetries, retryBackoffMs, compression, bufferBytes,
                maxBlockMs);
    }

    public ProducerConfig withRetries(int maxRetries, long retryBackoffMs) {
        return new ProducerConfig(client, lingerMs, batchBytes, maxRetries, retryBackoffMs, compression, bufferBytes,
                maxBlockMs);
    }

    public ProducerConfig withCompression(Compression value) {
        return new ProducerConfig(client, lingerMs, batchBytes, maxRetries, retryBackoffMs, value, bufferBytes,
                maxBlockMs);
    }

    public ProducerConfig withBuffer(long bufferBytes, long maxBlockMs) {
        return new ProducerConfig(client, lingerMs, batchBytes, maxRetries, retryBackoffMs, compression, bufferBytes,
                maxBlockMs);
    }
}
