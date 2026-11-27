package io.github.karthika04.tributary.raft;

import java.io.IOException;

/**
 * How a node talks to its peers. The broker sends these over TCP; tests use an
 * in-memory network that can drop messages and split the cluster.
 */
public interface RaftTransport {

    VoteResponse requestVote(String peerId, VoteRequest request) throws IOException;

    AppendResponse appendEntries(String peerId, AppendRequest request) throws IOException;
}
