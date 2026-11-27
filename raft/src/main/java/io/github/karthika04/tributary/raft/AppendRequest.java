package io.github.karthika04.tributary.raft;

import java.util.List;

/**
 * AppendEntries RPC: the leader sends new entries, or none as a heartbeat.
 *
 * @param prevLogIndex index of the entry just before the new ones; the follower must
 *                     already have it, with {@code prevLogTerm}, or it refuses. This one
 *                     check is what keeps every log identical up to the commit point
 * @param leaderCommit the leader's commit index, so followers can apply entries too
 */
public record AppendRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                            List<LogEntry> entries, long leaderCommit) {

    public AppendRequest {
        entries = List.copyOf(entries);
    }
}
