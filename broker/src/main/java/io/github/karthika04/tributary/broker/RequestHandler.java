package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.RequestEnvelope;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.ResponseEnvelope;
import io.github.karthika04.tributary.storage.CorruptRecordException;
import io.github.karthika04.tributary.storage.NewRecord;
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
 */
final class RequestHandler {

    private static final System.Logger LOG = System.getLogger(RequestHandler.class.getName());

    private final TopicManager topics;
    private final GroupCoordinator groups;
    private final BrokerConfig config;
    private final Clock clock;

    RequestHandler(TopicManager topics, GroupCoordinator groups, BrokerConfig config, Clock clock) {
        this.topics = topics;
        this.groups = groups;
        this.config = config;
        this.clock = clock;
    }

    ResponseEnvelope handle(RequestEnvelope envelope) {
        int id = envelope.correlationId();
        Request request = envelope.request();
        try {
            Response body = switch (request) {
                case Request.CreateTopic r -> {
                    topics.createTopic(r.topic(), r.partitions());
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
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "unexpected error while serving " + request.apiKey(), e);
            return ResponseEnvelope.failure(id, request.apiKey(), ErrorCode.INTERNAL_ERROR,
                    "internal error: " + e.getClass().getSimpleName());
        }
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
        Partition partition = topics.partition(r.topic(), r.partition());
        long now = clock.millis();
        List<NewRecord> records = new ArrayList<>(r.records().size());
        for (ProducedRecord record : r.records()) {
            records.add(new NewRecord(now, record.key(), record.value()));
        }
        return new Response.Produce(partition.append(records));
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
