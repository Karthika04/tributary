package io.github.karthika04.tributary.raft;

/** The answer to a {@link VoteRequest}. {@code term} lets a stale candidate catch up. */
public record VoteResponse(long term, boolean granted) {
}
