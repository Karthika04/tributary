package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.protocol.RaftEntry;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.raft.AppendRequest;
import io.github.karthika04.tributary.raft.AppendResponse;
import io.github.karthika04.tributary.raft.LogEntry;
import io.github.karthika04.tributary.raft.RaftTransport;
import io.github.karthika04.tributary.raft.VoteRequest;
import io.github.karthika04.tributary.raft.VoteResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Carries Raft messages between brokers over the normal Tributary protocol. Each peer
 * gets two connections, one for votes and one for log replication, so a slow
 * replication call never delays an election.
 */
final class BrokerRaftTransport implements RaftTransport, AutoCloseable {

    private final Map<String, TributaryClient> voteClients = new HashMap<>();
    private final Map<String, TributaryClient> appendClients = new HashMap<>();

    BrokerRaftTransport(String selfId, Map<String, String> peerAddresses) {
        peerAddresses.forEach((peer, address) -> {
            ClientConfig config = ClientConfig.parse(address)
                    .withClientId("raft-" + selfId)
                    .withConnectTimeoutMs(1_000)
                    .withRequestTimeoutMs(2_000)
                    .withMaxAttempts(1); // Raft does its own retrying
            voteClients.put(peer, new TributaryClient(config));
            appendClients.put(peer, new TributaryClient(config));
        });
    }

    @Override
    public VoteResponse requestVote(String peerId, VoteRequest r) throws IOException {
        Response.RaftVote response = (Response.RaftVote) call(voteClients, peerId,
                new Request.RaftVote(r.term(), r.candidateId(), r.lastLogIndex(), r.lastLogTerm(), r.preVote()));
        return new VoteResponse(response.term(), response.granted());
    }

    @Override
    public AppendResponse appendEntries(String peerId, AppendRequest r) throws IOException {
        List<RaftEntry> entries = new ArrayList<>(r.entries().size());
        for (LogEntry e : r.entries()) {
            entries.add(new RaftEntry(e.term(), e.command()));
        }
        Response.RaftAppend response = (Response.RaftAppend) call(appendClients, peerId, new Request.RaftAppend(
                r.term(), r.leaderId(), r.prevLogIndex(), r.prevLogTerm(), entries, r.leaderCommit()));
        return new AppendResponse(response.term(), response.success(), response.matchIndex());
    }

    private static Response call(Map<String, TributaryClient> clients, String peerId, Request request)
            throws IOException {
        TributaryClient client = clients.get(peerId);
        if (client == null) {
            throw new IOException("unknown peer " + peerId);
        }
        try {
            return client.send(request);
        } catch (TributaryException | IllegalStateException e) {
            throw new IOException(peerId + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        voteClients.values().forEach(TributaryClient::close);
        appendClients.values().forEach(TributaryClient::close);
    }
}
