package io.github.karthika04.tributary.raft;

/** Only the leader accepts proposals. {@link #leaderId()} is who to ask, if known. */
public class NotLeaderException extends RuntimeException {

    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super(leaderId == null ? "not the leader; no leader is known yet" : "not the leader; the leader is " + leaderId);
        this.leaderId = leaderId;
    }

    /** The current leader's id, or null if an election is in progress. */
    public String leaderId() {
        return leaderId;
    }
}
