package io.github.karthika04.tributary.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns requests and responses into frame payloads and back.
 *
 * <pre>
 * request  = [apiKey: short][version: short][correlationId: int][clientId: string][body]
 * response = [correlationId: int][apiKey: short][error: short][errorMessage: string][body if no error]
 * </pre>
 *
 * <p>The version field lets the protocol change later without breaking old clients:
 * a broker can keep answering version 0 while also understanding version 1.
 */
public final class Codec {

    public static final short VERSION = 0;

    private Codec() {
    }

    // ---- requests ----------------------------------------------------------------

    public static byte[] encodeRequest(int correlationId, String clientId, Request request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeShort(request.apiKey().code());
            out.writeShort(VERSION);
            out.writeInt(correlationId);
            Wire.writeString(out, clientId);
            switch (request) {
                case Request.CreateTopic r -> {
                    Wire.writeString(out, r.topic());
                    out.writeInt(r.partitions());
                }
                case Request.Metadata r -> {
                    // no body
                }
                case Request.Produce r -> {
                    Wire.writeString(out, r.topic());
                    out.writeInt(r.partition());
                    RecordBlocks.writeProduced(out, r.compression(), r.records());
                }
                case Request.Fetch r -> {
                    Wire.writeString(out, r.topic());
                    out.writeInt(r.partition());
                    out.writeLong(r.offset());
                    out.writeInt(r.maxRecords());
                    out.writeInt(r.maxWaitMs());
                    out.writeByte(r.compression().code());
                }
                case Request.ListOffsets r -> {
                    Wire.writeString(out, r.topic());
                    out.writeInt(r.partition());
                }
                case Request.JoinGroup r -> {
                    Wire.writeString(out, r.groupId());
                    Wire.writeString(out, r.memberId());
                    Wire.writeStrings(out, r.topics());
                    out.writeInt(r.sessionTimeoutMs());
                    Wire.writeString(out, r.assignor());
                }
                case Request.Heartbeat r -> {
                    Wire.writeString(out, r.groupId());
                    Wire.writeString(out, r.memberId());
                    out.writeInt(r.generation());
                }
                case Request.LeaveGroup r -> {
                    Wire.writeString(out, r.groupId());
                    Wire.writeString(out, r.memberId());
                }
                case Request.CommitOffsets r -> {
                    Wire.writeString(out, r.groupId());
                    Wire.writeString(out, r.memberId());
                    Wire.writePartitionOffsets(out, r.offsets());
                }
                case Request.FetchOffsets r -> {
                    Wire.writeString(out, r.groupId());
                    Wire.writeTopicPartitions(out, r.partitions());
                }
                case Request.RaftVote r -> {
                    out.writeLong(r.term());
                    Wire.writeString(out, r.candidateId());
                    out.writeLong(r.lastLogIndex());
                    out.writeLong(r.lastLogTerm());
                    out.writeBoolean(r.preVote());
                }
                case Request.RaftAppend r -> {
                    out.writeLong(r.term());
                    Wire.writeString(out, r.leaderId());
                    out.writeLong(r.prevLogIndex());
                    out.writeLong(r.prevLogTerm());
                    out.writeInt(r.entries().size());
                    for (RaftEntry e : r.entries()) {
                        out.writeLong(e.term());
                        Wire.writeBytes(out, e.command());
                    }
                    out.writeLong(r.leaderCommit());
                }
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e); // cannot happen with an in-memory stream
        }
    }

    /**
     * @throws ProtocolException if even the header is unreadable (the broker should
     *                           close the connection)
     * @throws RequestDecodeException if the header was read but the rest was not (the
     *                                broker can answer with an error)
     */
    public static RequestEnvelope decodeRequest(byte[] frame) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame));
        ApiKey apiKey;
        short version;
        int correlationId;
        String clientId;
        try {
            apiKey = ApiKey.fromCode(in.readShort());
            version = in.readShort();
            correlationId = in.readInt();
            clientId = Wire.readString(in);
        } catch (IOException | ProtocolException e) {
            throw new ProtocolException("unreadable request header", e);
        }
        if (version != VERSION) {
            throw new RequestDecodeException(correlationId, apiKey, ErrorCode.UNSUPPORTED_VERSION,
                    "unsupported version " + version + " of " + apiKey + "; this broker speaks " + VERSION, null);
        }
        try {
            Request request = switch (apiKey) {
                case CREATE_TOPIC -> new Request.CreateTopic(Wire.readString(in), in.readInt());
                case METADATA -> new Request.Metadata();
                case PRODUCE -> {
                    String topic = Wire.readString(in);
                    int partition = in.readInt();
                    RecordBlocks.ProducedBlock block = RecordBlocks.readProduced(in);
                    yield new Request.Produce(topic, partition, block.compression(), block.records());
                }
                case FETCH -> new Request.Fetch(Wire.readString(in), in.readInt(), in.readLong(),
                        in.readInt(), in.readInt(), Compression.fromCode(in.readByte()));
                case LIST_OFFSETS -> new Request.ListOffsets(Wire.readString(in), in.readInt());
                case JOIN_GROUP -> new Request.JoinGroup(Wire.readString(in), Wire.readString(in),
                        Wire.readStrings(in), in.readInt(), Wire.readString(in));
                case HEARTBEAT -> new Request.Heartbeat(Wire.readString(in), Wire.readString(in), in.readInt());
                case LEAVE_GROUP -> new Request.LeaveGroup(Wire.readString(in), Wire.readString(in));
                case COMMIT_OFFSETS -> new Request.CommitOffsets(Wire.readString(in), Wire.readString(in),
                        Wire.readPartitionOffsets(in));
                case FETCH_OFFSETS -> new Request.FetchOffsets(Wire.readString(in), Wire.readTopicPartitions(in));
                case RAFT_VOTE -> new Request.RaftVote(in.readLong(), Wire.readString(in), in.readLong(),
                        in.readLong(), in.readBoolean());
                case RAFT_APPEND -> {
                    long term = in.readLong();
                    String leaderId = Wire.readString(in);
                    long prevLogIndex = in.readLong();
                    long prevLogTerm = in.readLong();
                    int count = Wire.readCount(in, 8 + 4);
                    List<RaftEntry> entries = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        long entryTerm = in.readLong();
                        byte[] command = Wire.readBytes(in);
                        if (command == null) {
                            throw new ProtocolException("raft entry without a command");
                        }
                        entries.add(new RaftEntry(entryTerm, command));
                    }
                    yield new Request.RaftAppend(term, leaderId, prevLogIndex, prevLogTerm, entries, in.readLong());
                }
            };
            if (in.available() != 0) {
                throw new ProtocolException(in.available() + " unexpected bytes after the request");
            }
            return new RequestEnvelope(correlationId, clientId, request);
        } catch (IOException | ProtocolException | IllegalArgumentException e) {
            String why = e instanceof EOFException ? "request body is cut off" : e.getMessage();
            throw new RequestDecodeException(correlationId, apiKey, ErrorCode.INVALID_REQUEST,
                    "malformed " + apiKey + " request: " + why, e);
        }
    }

    // ---- responses ---------------------------------------------------------------

    public static byte[] encodeResponse(ResponseEnvelope envelope, Compression fetchCompression) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(envelope.correlationId());
            out.writeShort(envelope.apiKey().code());
            out.writeShort(envelope.error().code());
            Wire.writeString(out, envelope.errorMessage());
            if (envelope.error() == ErrorCode.NONE) {
                switch (envelope.body()) {
                    case Response.CreateTopic r -> {
                        // no body
                    }
                    case Response.Metadata r -> {
                        out.writeInt(r.topics().size());
                        for (Response.TopicMetadata t : r.topics()) {
                            Wire.writeString(out, t.name());
                            out.writeInt(t.partitions());
                        }
                    }
                    case Response.Produce r -> out.writeLong(r.baseOffset());
                    case Response.Fetch r -> {
                        out.writeLong(r.startOffset());
                        out.writeLong(r.endOffset());
                        RecordBlocks.writeFetched(out, fetchCompression, r.records());
                    }
                    case Response.ListOffsets r -> {
                        out.writeLong(r.startOffset());
                        out.writeLong(r.endOffset());
                    }
                    case Response.JoinGroup r -> {
                        Wire.writeString(out, r.memberId());
                        out.writeInt(r.generation());
                        Wire.writeTopicPartitions(out, r.assignment());
                    }
                    case Response.Heartbeat r -> {
                        // no body
                    }
                    case Response.LeaveGroup r -> {
                        // no body
                    }
                    case Response.CommitOffsets r -> Wire.writeTopicPartitions(out, r.rejected());
                    case Response.FetchOffsets r -> Wire.writePartitionOffsets(out, r.offsets());
                    case Response.RaftVote r -> {
                        out.writeLong(r.term());
                        out.writeBoolean(r.granted());
                    }
                    case Response.RaftAppend r -> {
                        out.writeLong(r.term());
                        out.writeBoolean(r.success());
                        out.writeLong(r.matchIndex());
                    }
                }
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] encodeResponse(ResponseEnvelope envelope) {
        return encodeResponse(envelope, Compression.NONE);
    }

    public static ResponseEnvelope decodeResponse(byte[] frame) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame));
        try {
            int correlationId = in.readInt();
            ApiKey apiKey = ApiKey.fromCode(in.readShort());
            ErrorCode error = ErrorCode.fromCode(in.readShort());
            String message = Wire.readString(in);
            if (error != ErrorCode.NONE) {
                return ResponseEnvelope.failure(correlationId, apiKey, error, message);
            }
            Response body = switch (apiKey) {
                case CREATE_TOPIC -> new Response.CreateTopic();
                case METADATA -> {
                    int count = Wire.readCount(in, 2 + 4);
                    List<Response.TopicMetadata> topics = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        topics.add(new Response.TopicMetadata(Wire.readString(in), in.readInt()));
                    }
                    yield new Response.Metadata(topics);
                }
                case PRODUCE -> new Response.Produce(in.readLong());
                case FETCH -> new Response.Fetch(in.readLong(), in.readLong(), RecordBlocks.readFetched(in));
                case LIST_OFFSETS -> new Response.ListOffsets(in.readLong(), in.readLong());
                case JOIN_GROUP -> new Response.JoinGroup(Wire.readString(in), in.readInt(),
                        Wire.readTopicPartitions(in));
                case HEARTBEAT -> new Response.Heartbeat();
                case LEAVE_GROUP -> new Response.LeaveGroup();
                case COMMIT_OFFSETS -> new Response.CommitOffsets(Wire.readTopicPartitions(in));
                case FETCH_OFFSETS -> new Response.FetchOffsets(Wire.readPartitionOffsets(in));
                case RAFT_VOTE -> new Response.RaftVote(in.readLong(), in.readBoolean());
                case RAFT_APPEND -> new Response.RaftAppend(in.readLong(), in.readBoolean(), in.readLong());
            };
            if (in.available() != 0) {
                throw new ProtocolException(in.available() + " unexpected bytes after the response");
            }
            return new ResponseEnvelope(correlationId, apiKey, ErrorCode.NONE, null, body);
        } catch (IOException | IllegalArgumentException e) {
            // IllegalArgumentException: well-formed bytes holding an invalid value, such
            // as a topic partition with no topic. Found by CodecFuzzTest.
            throw new ProtocolException("malformed response: " + e, e);
        }
    }
}
