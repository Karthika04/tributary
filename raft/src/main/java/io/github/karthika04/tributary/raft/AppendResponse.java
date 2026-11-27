package io.github.karthika04.tributary.raft;

/**
 * The answer to an {@link AppendRequest}.
 *
 * @param matchIndex on success, the last index the follower now shares with the leader.
 *                   On failure, a hint: the leader should retry from just after it,
 *                   which lets it skip back over a whole conflicting term at once
 */
public record AppendResponse(long term, boolean success, long matchIndex) {
}
