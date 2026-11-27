package io.github.karthika04.tributary.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Mutation fuzzing: take valid messages, damage them in many random ways, and check
 * that decoding either succeeds or fails with {@link ProtocolException}. Anything else
 * (an IndexOutOfBoundsException, a NegativeArraySizeException, an OutOfMemoryError,
 * an IllegalArgumentException from a record) is a bug: on the broker it would kill
 * the connection's thread with an unhelpful error, and on the client it would escape
 * as an exception callers do not expect.
 *
 * <p>The seed is fixed, so a failure reproduces exactly. Set the system property
 * {@code fuzz.iterations} to run longer.
 */
class CodecFuzzTest {

    private static final long SEED = 20261126L;
    private static final int ITERATIONS = Integer.getInteger("fuzz.iterations", 20_000);

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static List<byte[]> validRequests() {
        List<ProducedRecord> records = List.of(
                new ProducedRecord(bytes("user-1"), bytes("{\"event\":\"signup\"}")),
                new ProducedRecord(null, new byte[0]),
                new ProducedRecord(bytes("k"), bytes("value ".repeat(40))));
        List<Request> requests = new ArrayList<>(List.of(
                new Request.CreateTopic("orders", 3),
                new Request.Metadata(),
                new Request.Fetch("orders", 2, 41, 500, 1000, Compression.GZIP),
                new Request.ListOffsets("orders", 1),
                new Request.JoinGroup("billing", null, List.of("orders", "payments"), 10_000, "range"),
                new Request.Heartbeat("billing", "billing-1234", 3),
                new Request.LeaveGroup("billing", "billing-1234"),
                new Request.CommitOffsets("billing", "billing-1234", List.of(
                        new PartitionOffset("orders", 0, 42), new PartitionOffset("orders", 2, 7))),
                new Request.FetchOffsets("billing", List.of(new TopicPartition("orders", 1))),
                new Request.RaftVote(7, "n2", 41, 6),
                new Request.RaftAppend(7, "n1", 40, 6, List.of(new RaftEntry(7, bytes("cmd")),
                        new RaftEntry(7, new byte[0])), 39)));
        for (Compression c : Compression.values()) {
            requests.add(new Request.Produce("events", 0, c, records));
        }
        List<byte[]> encoded = new ArrayList<>();
        for (Request r : requests) {
            encoded.add(Codec.encodeRequest(17, "fuzzer", r));
        }
        return encoded;
    }

    private static List<byte[]> validResponses() {
        List<Response> bodies = List.of(
                new Response.CreateTopic(),
                new Response.Metadata(List.of(new Response.TopicMetadata("a", 1),
                        new Response.TopicMetadata("b", 4))),
                new Response.Produce(12345),
                new Response.Fetch(3, 10, List.of(
                        new FetchedRecord(3, 100, bytes("k"), bytes("v")),
                        new FetchedRecord(4, 101, null, bytes("w".repeat(300))))),
                new Response.ListOffsets(5, 9),
                new Response.JoinGroup("billing-1234", 4, List.of(
                        new TopicPartition("orders", 0), new TopicPartition("payments", 2))),
                new Response.Heartbeat(),
                new Response.CommitOffsets(List.of(new TopicPartition("orders", 1))),
                new Response.FetchOffsets(List.of(new PartitionOffset("orders", 0, -1))),
                new Response.RaftVote(9, true),
                new Response.RaftAppend(9, false, 12));
        List<byte[]> encoded = new ArrayList<>();
        for (Response body : bodies) {
            for (Compression c : Compression.values()) {
                encoded.add(Codec.encodeResponse(ResponseEnvelope.success(99, body), c));
            }
        }
        encoded.add(Codec.encodeResponse(ResponseEnvelope.failure(5, ApiKey.FETCH,
                ErrorCode.OFFSET_OUT_OF_RANGE, "offset 99 is out of range")));
        return encoded;
    }

    /** One random kind of damage. Several are aimed at length and count fields. */
    static byte[] mutate(byte[] input, Random random) {
        byte[] b = input.clone();
        int kind = random.nextInt(8);
        switch (kind) {
            case 0 -> { // flip a few bits
                for (int i = 0, n = 1 + random.nextInt(4); i < n && b.length > 0; i++) {
                    b[random.nextInt(b.length)] ^= (byte) (1 << random.nextInt(8));
                }
            }
            case 1 -> { // cut it off
                b = java.util.Arrays.copyOf(b, b.length == 0 ? 0 : random.nextInt(b.length));
            }
            case 2 -> { // add garbage at the end
                byte[] extra = new byte[1 + random.nextInt(16)];
                random.nextBytes(extra);
                byte[] longer = java.util.Arrays.copyOf(b, b.length + extra.length);
                System.arraycopy(extra, 0, longer, b.length, extra.length);
                b = longer;
            }
            case 3, 4 -> { // write an extreme int or short somewhere (lengths, counts, sizes)
                int[] extremes = {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 1 << 20, 1 << 30, 65_535, 0x7fff};
                int value = extremes[random.nextInt(extremes.length)];
                int width = kind == 3 ? 4 : 2;
                if (b.length >= width) {
                    int at = random.nextInt(b.length - width + 1);
                    for (int i = 0; i < width; i++) {
                        b[at + i] = (byte) (value >>> (8 * (width - 1 - i)));
                    }
                }
            }
            case 5 -> { // overwrite a stretch with random bytes
                if (b.length > 0) {
                    int at = random.nextInt(b.length);
                    for (int i = at; i < Math.min(b.length, at + 1 + random.nextInt(8)); i++) {
                        b[i] = (byte) random.nextInt(256);
                    }
                }
            }
            case 6 -> { // duplicate a slice in place (repeated fields)
                if (b.length > 2) {
                    int from = random.nextInt(b.length - 1);
                    int len = 1 + random.nextInt(Math.min(16, b.length - from));
                    byte[] longer = new byte[b.length + len];
                    System.arraycopy(b, 0, longer, 0, from + len);
                    System.arraycopy(b, from, longer, from + len, b.length - from);
                    b = longer;
                }
            }
            default -> { // completely random bytes of a similar size
                b = new byte[random.nextInt(input.length + 8)];
                random.nextBytes(b);
            }
        }
        return b;
    }

    @Test
    void damagedRequestsOnlyEverFailWithAProtocolException() {
        Random random = new Random(SEED);
        List<byte[]> seeds = validRequests();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] input = mutate(seeds.get(random.nextInt(seeds.size())), random);
            try {
                Codec.decodeRequest(input);
            } catch (ProtocolException expected) {
                // the documented failure
            } catch (Throwable t) {
                fail("iteration " + i + ": decodeRequest threw " + t + " for " + hex(input), t);
            }
        }
    }

    @Test
    void damagedResponsesOnlyEverFailWithAProtocolException() {
        Random random = new Random(SEED + 1);
        List<byte[]> seeds = validResponses();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] input = mutate(seeds.get(random.nextInt(seeds.size())), random);
            try {
                Codec.decodeResponse(input);
            } catch (ProtocolException expected) {
                // the documented failure
            } catch (Throwable t) {
                fail("iteration " + i + ": decodeResponse threw " + t + " for " + hex(input), t);
            }
        }
    }

    @Test
    void randomStreamsNeverMakeFramesAllocateTooMuch() {
        Random random = new Random(SEED + 2);
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] stream = new byte[random.nextInt(12)];
            random.nextBytes(stream);
            try {
                Frames.read(new ByteArrayInputStream(stream), 1024);
            } catch (IOException | ProtocolException expected) {
                // cut off, or over the limit
            } catch (Throwable t) {
                fail("iteration " + i + ": Frames.read threw " + t + " for " + hex(stream), t);
            }
        }
    }

    /** The exact inputs that found bugs, kept so the bugs stay fixed. */
    @Test
    void inputsThatOnceBrokeTheDecoders() {
        // A JoinGroup whose topic list holds a null string: was a NullPointerException.
        assertProtocolError(() -> Codec.decodeRequest(unhex("0005000000000011000666757a7a6572000762696c6c696e67ffff"
                + "000000020000ffff6465727300087061796d656e747300002710000572616e6765")));
        // A response with a topic partition missing its topic: was an IllegalArgumentException.
        assertProtocolError(() -> Codec.decodeResponse(unhex("0000006300080000ffff00000001ffff6f726465727300000001")));
        // A 150-byte produce whose GZIP block declares about 2 GB: was an OutOfMemoryError.
        assertProtocolError(() -> Codec.decodeRequest(unhex("0002000000000011000666757a7a657200066576656e747300000000"
                + "0175cc4e250000004d1f8b08000000000000ff63606060666060602b2d4e2dd23564606010aa564a2d4bcd2b51b252"
                + "2ace4ccf2b2d50aafdffffff7f060860cc666060f8509698539aaa30724800cd9717c825010000")));
    }

    private static void assertProtocolError(Runnable decode) {
        try {
            decode.run();
            fail("expected a ProtocolException");
        } catch (ProtocolException expected) {
            // good
        }
    }

    static byte[] unhex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    static String hex(byte[] b) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < Math.min(b.length, 200); i++) {
            s.append(String.format("%02x", b[i]));
        }
        return b.length > 200 ? s + "... (" + b.length + " bytes)" : s.toString();
    }
}
