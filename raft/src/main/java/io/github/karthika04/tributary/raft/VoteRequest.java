package io.github.karthika04.tributary.raft;

/**
 * RequestVote RPC: a candidate asks for a node's vote in {@code term}. With
 * {@code preVote}, it only asks whether the node <em>would</em> vote for it in
 * {@code term}; nobody changes term, votes or timers.
 */
public record VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm, boolean preVote) {
}
