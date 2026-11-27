package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.RaftEntry;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.RequestEnvelope;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
import io.github.karthika04.tributary.raft.AppendRequest;
import io.github.karthika04.tributary.raft.AppendResponse;
import io.github.karthika04.tributary.raft.LogEntry;
import io.github.karthika04.tributary.raft.RaftNode;
import io.github.karthika04.tributary.raft.VoteRequest;
import io.github.karthika04.tributary.raft.VoteResponse;
import io.github.karthika04.tributary.storage.CorruptRecordException;
import io.github.karthika04.tributary.storage.OffsetOutOfRangeException;
import io.github.karthika04.tributary.storage.Record;
import io.github.karthika04.tributary.storage.RecordTooLargeException;
import java.lang.System.Logger.Level;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns one decoded request into one response. This is the broker's business logic;
 * it knows nothing about sockets, which keeps it easy to test.
 *
 * <p>In a cluster, only the Raft leader serves clients; other brokers answer
 * {@code NOT_LEADER} with the leader's address. Raft messages between brokers are
 * answered by every broker.
 */
final class RequestHandler {

    /** Produce requests in a cluster stay well under the frame limit, because they travel again between brokers. */
    static final int MAX_CLUSTER_PRODUCE_BYTES = 8 * 1024 * 1024;

    private static final System.Logger LOG = System.getLogger(RequestHandler.class.getName());

    private final TopicManager topics;
    private final GroupCoordinator groups;
    private final Writes writes;
    private final RaftNode raft; // null for a single broker
    private final RaftWrites raftWrites;
    private final BrokerConfig config;
    private final Clock clock;

    RequestHandler(TopicManager topics, GroupCoordinator groups, Writes writes, RaftNode raft,
                   BrokerConfig config, Clock clock) {
        this.topics = topics;
        this.groups = groups;
        this.writes = writes;
        this.raft = raft;
        this.raftWrites = writes instanceof RaftWrites rw ? rw : null;
        this.config = config;
        this.clock = clock;
    }

    ResponseEnvelope handle(RequestEnvelope envelope) {
        int id = envelope.correlationId();
        Request request = envelope.request();
        try {
            boolean brokerToBroker = request instanceof Request.RaftVote || request instanceof Request.RaftAppend;
            if (raft != null && !brokerToBroker && !raft.isReadyLeader()) {
                // Only a leader that has caught up on everything committed serves clients,
                // so nobody reads stale data from a follower or a brand-new leader.
                throw raftWrites.notLeader(raft.isLeader() ? null : raft.leaderId());
            }
            Response body = switch (request) {
                case Request.CreateTopic r -> {
                    topics.checkNewTopic(r.topic(), r.partitions()); // fail fast, before replicating
                    writes.createTopic(r.topic(), r.partitions());
                    yield new Response.CreateTopic();
                }
                case Request.Metadata r -> new Response.Metadata(topics.metadata());
                case Request.Produce r -> produce(r);
                case Request.Fetch r -> fetch(r);
                case Request.ListOffsets r -> {
                    Partition p = topics.partition(r.topic(), r.partition());
                    yield new Response.ListOffsets(p.startOffset(), p.endOffset());
                }
                case Request.JoinGroup r ->
                        groups.join(r.groupId(), r.memberId(), r.topics(), r.sessionTimeoutMs(), r.assignor());
                case Request.Heartbeat r -> {
                    groups.heartbeat(r.groupId(), r.memberId(), r.generation());
                    yield new Response.Heartbeat();
                }
                case Request.LeaveGroup r -> {
                    groups.leave(r.groupId(), r.memberId());
                    yield new Response.LeaveGroup();
                }
                case Request.CommitOffsets r -> commitOffsets(r);
                case Request.FetchOffsets r -> new Response.FetchOffsets(groups.fetchOffsets(r.groupId(),
                        r.partitions()));
                case Request.RaftVote r -> {
                    VoteResponse vote = raft().handleVote(
                            new VoteRequest(r.term(), r.candidateId(), r.lastLogIndex(), r.lastLogTerm(), r.preVote()));
                    yield new Response.RaftVote(vote.term(), vote.granted());
                }
                case Request.RaftAppend r -> {
                    List<LogEntry> entries = new ArrayList<>(r.entries().size());
                    for (RaftEntry e : r.entries()) {
                        entries.add(new LogEntry(e.term(), e.command()));
                    }
                    AppendResponse append = raft().handleAppend(new AppendRequest(r.term(), r.leaderId(),
                            r.prevLogIndex(), r.prevLogTerm(), entries, r.leaderCommit()));
                    yield new Response.RaftAppend(append.term(), append.success(), append.matchIndex());
                }
            };
            return ResponseEnvelope.success(id, body);
        } catch (BrokerException e) {
            return ResponseEnvelope.failure(id, request.apiKey(), e.error(), e.getMessage());
        } catch (OffsetOutOfRangeException e) {
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.OFFSET_OUT_OF_RANGE, e.getMessage());
        } catch (RecordTooLargeException e) {
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.RECORD_TOO_LARGE, e.getMessage());
        } catch (CorruptRecordException e) {
            LOG.log(Level.ERROR, "corrupt data while serving " + request.apiKey(), e);
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.CORRUPT_MESSAGE, e.getMessage());
        } catch (IllegalArgumentException e) {
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.INVALID_REQUEST, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.BROKER_SHUTTING_DOWN,
                    "broker is shutting down");
        } catch (IllegalStateException e) {
            if (raft != null && e.getMessage() != null && e.getMessage().contains("is stopped")) {
                return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.BROKER_SHUTTING_DOWN,
                        "broker is shutting down");
            }
            LOG.log(Level.ERROR, "unexpected error while serving " + request.apiKey(), e);
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.INTERNAL_ERROR,
                    "internal error: " + e.getClass().getSimpleName());
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "unexpected error while serving " + request.apiKey(), e);
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.INTERNAL_ERROR,
                    "internal error: " + e.getClass().getSimpleName());
        }
    }

    private RaftNode raft() {
        if (raft == null) {
            throw new BrokerException(ErrorCode.INVALID_REQUEST, "this broker is not part of a cluster");
        }
        return raft;
    }

    private Response commitOffsets(Request.CommitOffsets r) {
        for (PartitionOffset po : r.offsets()) {
            topics.partition(po.topic(), po.partition()); // UNKNOWN_TOPIC / UNKNOWN_PARTITION
        }
        return new Response.CommitOffsets(groups.commit(r.groupId(), r.memberId(), r.offsets()));
    }

    private Response produce(Request.Produce r) {
        if (r.records().isEmpty()) {
            throw new BrokerException(ErrorCode.INVALID_REQUEST, "produce request has no records");
        }
        if (TopicManager.isInternal(r.topic())) {
            throw new BrokerException(ErrorCode.INVALID_TOPIC, "topic '" + r.topic() + "' is internal to the broker");
        }
        topics.partition(r.topic(), r.partition()); // UNKNOWN_TOPIC / UNKNOWN_PARTITION
        if (raft != null) {
            long bytes = 0;
            for (var record : r.records()) {
                bytes += record.value().length + (record.key() == null ? 0 : record.key().length);
            }
            if (bytes > MAX_CLUSTER_PRODUCE_BYTES) {
                throw new BrokerException(ErrorCode.RECORD_TOO_LARGE, "a produce request in a cluster may carry at most "
                        + MAX_CLUSTER_PRODUCE_BYTES + " bytes; send smaller batches");
            }
        }
        return new Response.Produce(writes.produce(r.topic(), r.partition(), clock.millis(), r.records()));
    }

    private Response fetch(Request.Fetch r) throws InterruptedException {
        if (r.offset() < 0 || r.maxRecords() <= 0 || r.maxWaitMs() < 0) {
            throw new BrokerException(ErrorCode.INVALID_REQUEST,
                    "fetch needs offset >= 0, maxRecords > 0 and maxWaitMs >= 0");
        }
        Partition partition = topics.partition(r.topic(), r.partition());
        int maxRecords = Math.min(r.maxRecords(), config.maxFetchRecords());
        long maxWait = Math.min(r.maxWaitMs(), config.maxFetchWaitMs());
        List<Record> stored = partition.read(r.offset(), maxRecords, maxWait);
        List<FetchedRecord> records = new ArrayList<>(stored.size());
        for (Record s : stored) {
            records.add(new FetchedRecord(s.offset(), s.timestamp(), s.key(), s.value()));
        }
        return new Response.Fetch(partition.startOffset(), partition.endOffset(), records);
    }
}
