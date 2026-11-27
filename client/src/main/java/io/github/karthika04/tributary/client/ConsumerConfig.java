package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.Compression;

/**
 * Consumer settings. Start from {@link #of(ClientConfig)}; add {@link #withGroup(String)}
 * to join a consumer group.
 *
 * @param maxPollRecords       most records one {@code poll()} returns
 * @param fetchMaxWaitMs       how long the broker may hold a fetch open waiting for new data
 * @param autoOffsetReset      where to start when there is no committed or current
 *                             position, or when the position was deleted by retention
 * @param fetchCompression     how the broker should compress records it sends back
 * @param groupId              the consumer group to join, or null to assign partitions by hand
 * @param assignor             how the group divides partitions: "range" or "roundrobin"
 * @param sessionTimeoutMs     with no heartbeat for this long, the broker gives this
 *                             consumer's partitions to others. Heartbeats are sent from
 *                             {@code poll()}, so call it at least this often
 * @param heartbeatIntervalMs  how often to send heartbeats
 * @param autoCommit           commit positions automatically from {@code poll()} and on close
 * @param autoCommitIntervalMs how often auto-commit runs
 */
public record ConsumerConfig(ClientConfig client, int maxPollRecords, int fetchMaxWaitMs,
                             AutoOffsetReset autoOffsetReset, Compression fetchCompression,
                             String groupId, String assignor, int sessionTimeoutMs, int heartbeatIntervalMs,
                             boolean autoCommit, int autoCommitIntervalMs) {

    /** Where a consumer with no position starts reading. */
    public enum AutoOffsetReset {
        /** From the oldest record still stored. */
        EARLIEST,
        /** Only records written from now on. */
        LATEST
    }

    public ConsumerConfig {
        if (maxPollRecords <= 0 || fetchMaxWaitMs < 0 || sessionTimeoutMs <= 0 || heartbeatIntervalMs <= 0
                || autoCommitIntervalMs <= 0) {
            throw new IllegalArgumentException("invalid consumer settings");
        }
        if (heartbeatIntervalMs >= sessionTimeoutMs) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be shorter than sessionTimeoutMs");
        }
    }

    public static ConsumerConfig of(ClientConfig client) {
        return new ConsumerConfig(client, 500, 500, AutoOffsetReset.EARLIEST, Compression.NONE,
                null, "range", 10_000, 1_000, true, 1_000);
    }

    public ConsumerConfig withMaxPollRecords(int value) {
        return new ConsumerConfig(client, value, fetchMaxWaitMs, autoOffsetReset, fetchCompression, groupId,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withFetchMaxWaitMs(int value) {
        return new ConsumerConfig(client, maxPollRecords, value, autoOffsetReset, fetchCompression, groupId,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withAutoOffsetReset(AutoOffsetReset value) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, value, fetchCompression, groupId,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withFetchCompression(Compression value) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, autoOffsetReset, value, groupId,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withGroup(String value) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, autoOffsetReset, fetchCompression, value,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withAssignor(String value) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, autoOffsetReset, fetchCompression, groupId,
                value, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withSession(int sessionTimeoutMs, int heartbeatIntervalMs) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, autoOffsetReset, fetchCompression, groupId,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, autoCommit, autoCommitIntervalMs);
    }

    public ConsumerConfig withAutoCommit(boolean enabled, int intervalMs) {
        return new ConsumerConfig(client, maxPollRecords, fetchMaxWaitMs, autoOffsetReset, fetchCompression, groupId,
                assignor, sessionTimeoutMs, heartbeatIntervalMs, enabled, intervalMs);
    }
}
