package io.github.karthika04.tributary.raft;

import java.nio.file.Path;
import java.util.List;

/**
 * @param nodeId               this node's id
 * @param peerIds              every other node in the cluster
 * @param dataDir              where the log and the term/vote file live
 * @param electionTimeoutMinMs a follower that hears nothing from a leader for a random
 *                             time between min and max starts an election. Randomness
 *                             makes it unlikely that two nodes start at the same moment
 * @param heartbeatIntervalMs  how often a leader contacts followers when idle; must be
 *                             well below the election timeout
 * @param maxEntriesPerRequest cap on entries sent in one AppendEntries
 * @param maxBytesPerRequest   cap on command bytes sent in one AppendEntries (at least one
 *                             entry is always sent, however big)
 */
public record RaftConfig(String nodeId, List<String> peerIds, Path dataDir, int electionTimeoutMinMs,
                         int electionTimeoutMaxMs, int heartbeatIntervalMs, int maxEntriesPerRequest,
                         int maxBytesPerRequest) {

    public RaftConfig {
        peerIds = List.copyOf(peerIds);
        if (peerIds.contains(nodeId)) {
            throw new IllegalArgumentException("peerIds must not include this node");
        }
        if (electionTimeoutMinMs <= heartbeatIntervalMs || electionTimeoutMaxMs <= electionTimeoutMinMs) {
            throw new IllegalArgumentException("need heartbeat < election timeout min < max");
        }
        if (maxEntriesPerRequest <= 0 || maxBytesPerRequest <= 0) {
            throw new IllegalArgumentException("request limits must be positive");
        }
    }

    public static RaftConfig of(String nodeId, List<String> peerIds, Path dataDir) {
        return new RaftConfig(nodeId, peerIds, dataDir, 400, 800, 100, 500, 4 * 1024 * 1024);
    }

    public RaftConfig withTimeouts(int electionMinMs, int electionMaxMs, int heartbeatMs) {
        return new RaftConfig(nodeId, peerIds, dataDir, electionMinMs, electionMaxMs, heartbeatMs,
                maxEntriesPerRequest, maxBytesPerRequest);
    }

    /** Votes needed to win an election or entries' copies needed to commit: a majority. */
    int majority() {
        return (peerIds.size() + 1) / 2 + 1;
    }
}
