package io.github.karthika04.tributary.raft;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory network for testing. Nodes call each other directly, unless a node is
 * down or a link between two nodes is cut, in which case the call fails like a lost
 * connection would.
 */
final class SimulatedNetwork {

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Set<String> cutLinks = ConcurrentHashMap.newKeySet();

    void register(RaftNode node) {
        nodes.put(node.nodeId(), node);
    }

    void unregister(String nodeId) {
        nodes.remove(nodeId);
    }

    /** Cuts both directions between a and b. */
    void cut(String a, String b) {
        cutLinks.add(a + "|" + b);
        cutLinks.add(b + "|" + a);
    }

    /** Puts {@code isolated} on its own: it can reach nobody and nobody can reach it. */
    void isolate(String isolated, Iterable<String> everyone) {
        for (String other : everyone) {
            if (!other.equals(isolated)) {
                cut(isolated, other);
            }
        }
    }

    void healAll() {
        cutLinks.clear();
    }

    RaftTransport transportFor(String from) {
        return new RaftTransport() {
            @Override
            public VoteResponse requestVote(String peerId, VoteRequest request) throws IOException {
                return target(from, peerId).handleVote(request);
            }

            @Override
            public AppendResponse appendEntries(String peerId, AppendRequest request) throws IOException {
                return target(from, peerId).handleAppend(request);
            }
        };
    }

    private RaftNode target(String from, String to) throws IOException {
        if (cutLinks.contains(from + "|" + to)) {
            throw new IOException("link " + from + " -> " + to + " is cut");
        }
        RaftNode node = nodes.get(to);
        if (node == null) {
            throw new IOException(to + " is down");
        }
        return node;
    }
}
