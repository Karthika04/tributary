package io.github.karthika04.tributary.raft;

/** RequestVote RPC: a candidate asks for a node's vote in {@code term}. */
public record VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
}
