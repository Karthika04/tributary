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
            };
            if (in.available() != 0) {
                throw new ProtocolException(in.available() + " unexpected bytes after the response");
            }
            return new ResponseEnvelope(correlationId, apiKey, ErrorCode.NONE, null, body);
        } catch (IOException e) {
            throw new ProtocolException("malformed response: " + e, e);
        }
    }
}
