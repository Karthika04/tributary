package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.Frames;
import io.github.karthika04.tributary.storage.LogConfig;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * Settings for one broker.
 *
 * @param port                     TCP port to listen on; 0 picks any free port (useful in tests)
 * @param dataDir                  where this broker keeps its data
 * @param logConfig                storage settings used for every partition
 * @param retentionCheckIntervalMs how often old segments are checked for deletion
 * @param maxFrameBytes            largest request accepted
 * @param maxFetchWaitMs           cap on how long a fetch may wait for new data
 * @param maxFetchRecords          cap on records returned by one fetch
 * @param groupCheckIntervalMs     how often consumer-group sessions are checked for expiry
 * @param nodeId                   this broker's id in a cluster, or null for a single broker
 * @param clusterNodes             every broker in the cluster (this one included), id to
 *                                 {@code host:port}; empty for a single broker
 */
public record BrokerConfig(
        int port,
        Path dataDir,
        LogConfig logConfig,
        long retentionCheckIntervalMs,
        int maxFrameBytes,
        int maxFetchWaitMs,
        int maxFetchRecords,
        long groupCheckIntervalMs,
        String nodeId,
        Map<String, String> clusterNodes) {

    public static final int DEFAULT_PORT = 9292;

    public BrokerConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be 0-65535, got " + port);
        }
        if (retentionCheckIntervalMs <= 0 || maxFrameBytes <= 0 || maxFetchWaitMs < 0 || maxFetchRecords <= 0
                || groupCheckIntervalMs <= 0) {
            throw new IllegalArgumentException("broker limits must be positive");
        }
        clusterNodes = Map.copyOf(clusterNodes);
        if (!clusterNodes.isEmpty() && (nodeId == null || !clusterNodes.containsKey(nodeId))) {
            throw new IllegalArgumentException("in a cluster, nodeId must be one of " + new TreeMap<>(clusterNodes).keySet());
        }
    }

    public static BrokerConfig defaults(Path dataDir) {
        return new BrokerConfig(DEFAULT_PORT, dataDir, LogConfig.defaults(), 60_000,
                Frames.DEFAULT_MAX_FRAME_BYTES, 30_000, 10_000, 500, null, Map.of());
    }

    public boolean isClustered() {
        return !clusterNodes.isEmpty();
    }

    public BrokerConfig withPort(int value) {
        return new BrokerConfig(value, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes, maxFetchWaitMs,
                maxFetchRecords, groupCheckIntervalMs, nodeId, clusterNodes);
    }

    public BrokerConfig withLogConfig(LogConfig value) {
        return new BrokerConfig(port, dataDir, value, retentionCheckIntervalMs, maxFrameBytes, maxFetchWaitMs,
                maxFetchRecords, groupCheckIntervalMs, nodeId, clusterNodes);
    }

    public BrokerConfig withRetentionCheckIntervalMs(long value) {
        return new BrokerConfig(port, dataDir, logConfig, value, maxFrameBytes, maxFetchWaitMs, maxFetchRecords,
                groupCheckIntervalMs, nodeId, clusterNodes);
    }

    public BrokerConfig withMaxFetchWaitMs(int value) {
        return new BrokerConfig(port, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes, value,
                maxFetchRecords, groupCheckIntervalMs, nodeId, clusterNodes);
    }

    public BrokerConfig withGroupCheckIntervalMs(long value) {
        return new BrokerConfig(port, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes, maxFetchWaitMs,
                maxFetchRecords, value, nodeId, clusterNodes);
    }

    /** Makes this broker one member of a cluster. */
    public BrokerConfig withCluster(String id, Map<String, String> nodes) {
        return new BrokerConfig(port, dataDir, logConfig, retentionCheckIntervalMs, maxFrameBytes, maxFetchWaitMs,
                maxFetchRecords, groupCheckIntervalMs, id, nodes);
    }
}
