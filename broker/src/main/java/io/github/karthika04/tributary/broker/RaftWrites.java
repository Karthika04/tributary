package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.raft.NotLeaderException;
import io.github.karthika04.tributary.raft.RaftNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Writes in a cluster: each becomes a command in the Raft log, and returns once a
 * majority of brokers has stored it and this broker has applied it. That is what makes
 * an acknowledged write survive the loss of any one broker in a 3-broker cluster.
 */
final class RaftWrites implements Writes {

    private static final long TIMEOUT_MS = 10_000;

    private final RaftNode raft;
    private final Map<String, String> addresses;

    RaftWrites(RaftNode raft, Map<String, String> addresses) {
        this.raft = raft;
        this.addresses = addresses;
    }

    @Override
    public void createTopic(String topic, int partitions) {
        replicate(Commands.createTopic(topic, partitions));
    }

    @Override
    public long produce(String topic, int partition, long timestamp, List<ProducedRecord> records) {
        return (Long) replicate(Commands.produce(topic, partition, timestamp, records));
    }

    @Override
    public void commitOffsets(String group, List<PartitionOffset> offsets) {
        if (!offsets.isEmpty()) {
            replicate(Commands.commitOffsets(group, offsets));
        }
    }

    private Object replicate(byte[] command) {
        CompletableFuture<Object> future = raft.propose(command);
        try {
            return future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof NotLeaderException notLeader) {
                throw notLeader(notLeader.leaderId());
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime; // rejected by the state machine, e.g. TOPIC_ALREADY_EXISTS
            }
            throw new IllegalStateException(cause);
        } catch (TimeoutException e) {
            // Usually a lost majority. The write may still commit later, so the client's
            // retry can create a duplicate: at-least-once.
            throw new BrokerException(ErrorCode.INTERNAL_ERROR, "replication timed out after " + TIMEOUT_MS + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerException(ErrorCode.BROKER_SHUTTING_DOWN, "broker is shutting down");
        }
    }

    /** NOT_LEADER, naming the leader's address so the client can go straight there. */
    BrokerException notLeader(String leaderId) {
        String address = leaderId == null ? null : addresses.get(leaderId);
        return new BrokerException(ErrorCode.NOT_LEADER, address == null
                ? "not the leader; no leader elected yet"
                : "not the leader; leader is " + address);
    }
}
