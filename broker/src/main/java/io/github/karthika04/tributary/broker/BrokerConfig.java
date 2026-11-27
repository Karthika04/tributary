package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.storage.LogConfig;
import java.nio.file.Path;

/**
 * Settings for one broker.
 *
 * @param port                     TCP port to listen on; 0 picks any free port (useful in tests)
 * @param dataDir                  where topic data lives, one sub-folder per topic
 * @param logConfig                storage settings used for every partition
 * @param retentionCheckIntervalMs how often old segments are checked for deletion
 * @param maxFrameBytes            largest request accepted
 * @param maxFetchWaitMs           cap on how long a fetch may wait for new data
 * @param maxFetchRecords          cap on records returned by one fetch
 * @param groupCheckIntervalMs     how often consumer-group sessions are checked for expiry
 */
public record BrokerConfig(
        int port,
        Path dataDir,
        LogConfig logConfig,
        long retentionCheckIntervalMs,
        int maxFrameBytes,
        int maxFetchWaitMs,
        int maxFetchRecords,
        long groupCheckIntervalMs) {

    public static final int DEFAULT_PORT = 9292;

    public BrokerConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be 0-65535, got " + port);
        }
        if (retentionCheckIntervalMs <= 0 || maxFrameBytes <= 0 || maxFetchWaitMs < 0 || maxFetchRecords <= 0
                || groupCheckIntervalMs <= 0) {
            throw new IllegalArgumentException("broker limits must be positive");
        }
    }

    public static BrokerConfig defaults(Path dataDir) {
        return new BrokerConfig(DEFAULT_PORT, dataDir, LogConfig.defaults(), 60_000,
                Frames.DEFAULT_MAX_FRAME_BYTES, 30_000, 10_000, 500);
    }

    public BrokerConfig withPort(int value) {
        return new BrokerConfig(value, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes,
                maxFetchWaitMs, maxFetchRecords, groupCheckIntervalMs);
    }

    public BrokerConfig withLogConfig(LogConfig value) {
        return new BrokerConfig(port, dataDir, value, retentionCheckIntervalMs, maxFrameBytes,
                maxFetchWaitMs, maxFetchRecords, groupCheckIntervalMs);
    }

    public BrokerConfig withRetentionCheckIntervalMs(long value) {
        return new BrokerConfig(port, dataDir, logConfig, value, maxFrameBytes, maxFetchWaitMs, maxFetchRecords,
                groupCheckIntervalMs);
    }

    public BrokerConfig withMaxFetchWaitMs(int value) {
        return new BrokerConfig(port, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes, value,
                maxFetchRecords, groupCheckIntervalMs);
    }

    public BrokerConfig withGroupCheckIntervalMs(long value) {
        return new BrokerConfig(port, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes, maxFetchWaitMs,
                maxFetchRecords, value);
    }
}
