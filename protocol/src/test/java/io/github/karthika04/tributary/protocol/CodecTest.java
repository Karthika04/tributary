package io.github.karthika04.tributary.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CodecTest {

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static Request roundTrip(Request request) {
        RequestEnvelope decoded = Codec.decodeRequest(Codec.encodeRequest(7, "test-client", request));
        assertEquals(7, decoded.correlationId());
        assertEquals("test-client", decoded.clientId());
        return decoded.request();
    }

    private static ResponseEnvelope roundTrip(ResponseEnvelope response, Compression compression) {
        return Codec.decodeResponse(Codec.encodeResponse(response, compression));
    }

    @Test
    void roundTripsEveryRequestType() {
        List<Request> requests = List.of(
                new Request.CreateTopic("orders", 3),
                new Request.Metadata(),
                new Request.Fetch("orders", 2, 41, 500, 1000, Compression.GZIP),
                new Request.ListOffsets("orders", 1),
                new Request.JoinGroup("billing", null, List.of("orders", "payments"), 10_000, "range"),
                new Request.JoinGroup("billing", "billing-1234", List.of("orders"), 30_000, "roundrobin"),
                new Request.Heartbeat("billing", "billing-1234", 3),
                new Request.LeaveGroup("billing", "billing-1234"),
                new Request.CommitOffsets("billing", "billing-1234", List.of(
                        new PartitionOffset("orders", 0, 42), new PartitionOffset("orders", 2, 7))),
                new Request.FetchOffsets("billing", List.of(new TopicPartition("orders", 1))));
        for (Request request : requests) {
            assertEquals(request, roundTrip(request));
        }
    }

    @Test
    void roundTripsProduceWithAndWithoutCompression() {
        List<ProducedRecord> records = List.of(
                new ProducedRecord(bytes("user-1"), bytes("{\"event\":\"signup\"}")),
                new ProducedRecord(null, new byte[0]));
        for (Compression compression : Compression.values()) {
            Request request = new Request.Produce("events", 0, compression, records);
            assertEquals(request, roundTrip(request));
        }
    }

    @Test
    void roundTripsEveryResponseType() {
        List<Response> bodies = List.of(
                new Response.CreateTopic(),
                new Response.Metadata(List.of(new Response.TopicMetadata("a", 1),
                        new Response.TopicMetadata("b", 4))),
                new Response.Produce(12345),
                new Response.Fetch(3, 10, List.of(
                        new FetchedRecord(3, 100, bytes("k"), bytes("v")),
                        new FetchedRecord(4, 101, null, bytes("w")))),
                new Response.ListOffsets(5, 9),
                new Response.JoinGroup("billing-1234", 4, List.of(
                        new TopicPartition("orders", 0), new TopicPartition("payments", 2))),
                new Response.JoinGroup("billing-5678", 4, List.of()),
                new Response.Heartbeat(),
                new Response.LeaveGroup(),
                new Response.CommitOffsets(List.of(new TopicPartition("orders", 1))),
                new Response.FetchOffsets(List.of(new PartitionOffset("orders", 0, -1))));
        for (Response body : bodies) {
            for (Compression compression : Compression.values()) {
                ResponseEnvelope decoded = roundTrip(ResponseEnvelope.success(99, body), compression);
                assertEquals(99, decoded.correlationId());
                assertEquals(ErrorCode.NONE, decoded.error());
                assertEquals(body, decoded.body());
            }
        }
    }

    @Test
    void roundTripsAnErrorResponse() {
        ResponseEnvelope error = ResponseEnvelope.failure(5, ApiKey.FETCH, ErrorCode.OFFSET_OUT_OF_RANGE,
                "offset 99 is out of range");

        ResponseEnvelope decoded = roundTrip(error, Compression.NONE);

        assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, decoded.error());
        assertEquals("offset 99 is out of range", decoded.errorMessage());
        assertEquals(ApiKey.FETCH, decoded.apiKey());
        assertNull(decoded.body());
    }

    @Test
    void gzipShrinksRepetitivePayloads() {
        List<ProducedRecord> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            records.add(new ProducedRecord(null,
                    bytes("{\"user\":\"someone\",\"action\":\"page_view\",\"page\":\"/products/" + i + "\"}")));
        }
        int plain = Codec.encodeRequest(1, "c", new Request.Produce("t", 0, Compression.NONE, records)).length;
        int gzipped = Codec.encodeRequest(1, "c", new Request.Produce("t", 0, Compression.GZIP, records)).length;

        assertTrue(gzipped * 4 < plain, "gzip " + gzipped + " bytes vs plain " + plain);
    }

    @Test
    void aCutOffBodyIsAnsweredWithInvalidRequest() {
        byte[] full = Codec.encodeRequest(42, "c", new Request.ListOffsets("orders", 1));
        byte[] cutOff = java.util.Arrays.copyOf(full, full.length - 2);

        RequestDecodeException e = assertThrows(RequestDecodeException.class, () -> Codec.decodeRequest(cutOff));

        assertEquals(42, e.correlationId());
        assertEquals(ApiKey.LIST_OFFSETS, e.apiKey());
        assertEquals(ErrorCode.INVALID_REQUEST, e.error());
    }

    @Test
    void extraBytesAfterARequestAreRejected() {
        byte[] full = Codec.encodeRequest(1, "c", new Request.Metadata());
        byte[] padded = java.util.Arrays.copyOf(full, full.length + 3);

        assertThrows(RequestDecodeException.class, () -> Codec.decodeRequest(padded));
    }

    @Test
    void anUnknownVersionIsAnsweredWithUnsupportedVersion() {
        byte[] frame = Codec.encodeRequest(8, "c", new Request.Metadata());
        frame[3] = 7; // version is bytes 2-3

        RequestDecodeException e = assertThrows(RequestDecodeException.class, () -> Codec.decodeRequest(frame));

        assertEquals(ErrorCode.UNSUPPORTED_VERSION, e.error());
        assertEquals(8, e.correlationId());
    }

    @Test
    void anUnreadableHeaderIsAProtocolError() {
        assertThrows(ProtocolException.class, () -> Codec.decodeRequest(new byte[] {0, 99, 0}));
        assertThrows(ProtocolException.class, () -> Codec.decodeRequest(new byte[0]));
    }

    @Test
    void aLyingCompressedSizeIsRejected() {
        byte[] frame = Codec.encodeRequest(1, "c", new Request.Produce("t", 0, Compression.GZIP,
                List.of(new ProducedRecord(null, bytes("hello hello hello")))));
        // Layout: header(2+2+4+2+1) + topic(2+1) + partition(4) + compression(1), then rawSize.
        int rawSizeAt = 11 + 3 + 4 + 1;
        frame[rawSizeAt + 3] += 10; // claim the block is bigger than it really is

        assertThrows(RequestDecodeException.class, () -> Codec.decodeRequest(frame));
    }
}
