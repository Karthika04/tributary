package io.github.karthika04.tributary.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * Mutation fuzzing for the on-disk record format. A damaged frame must either decode
 * or fail with {@link CorruptRecordException}, which crash recovery relies on to find
 * where good data ends. Any other exception would stop a broker from starting.
 *
 * <p>Random damage almost always breaks the checksum, so half the inputs get a fresh,
 * valid checksum after the damage. That sends them past the checksum into the field
 * parsing, the way a bug in our own writer (rather than a bad disk) would.
 */
class RecordCodecFuzzTest {

    private static final int ITERATIONS = Integer.getInteger("fuzz.iterations", 20_000);

    private static List<byte[]> validFrames() {
        return List.of(
                frame(new Record(0, 1_700_000_000_000L, null, "v".getBytes(UTF_8))),
                frame(new Record(42, 5, "key".getBytes(UTF_8), "value".getBytes(UTF_8))),
                frame(new Record(Long.MAX_VALUE - 1, 0, new byte[0], new byte[0])),
                frame(new Record(7, 9, "k".repeat(50).getBytes(UTF_8), "x".repeat(500).getBytes(UTF_8))));
    }

    private static byte[] frame(Record record) {
        ByteBuffer b = RecordCodec.encode(record);
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    /** Recomputes the checksum over whatever the length field says the body is. */
    private static void fixChecksum(byte[] b) {
        if (b.length < 8) {
            return;
        }
        int length = ByteBuffer.wrap(b).getInt(0);
        int bodyLength = length - 4;
        if (bodyLength < 0 || 8L + bodyLength > b.length) {
            return;
        }
        CRC32C crc = new CRC32C();
        crc.update(b, 8, bodyLength);
        ByteBuffer.wrap(b).putInt(4, (int) crc.getValue());
    }

    private static byte[] mutate(byte[] input, Random random) {
        byte[] b = input.clone();
        switch (random.nextInt(5)) {
            case 0 -> b[random.nextInt(b.length)] ^= (byte) (1 << random.nextInt(8));
            case 1 -> b = Arrays.copyOf(b, random.nextInt(b.length));
            case 2 -> { // an extreme value in a 4-byte field: length, key length, value length
                int[] extremes = {-1, -2, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 1 << 30};
                int at = random.nextInt(b.length - 3);
                ByteBuffer.wrap(b).putInt(at, extremes[random.nextInt(extremes.length)]);
            }
            case 3 -> { // an extreme 8-byte value: offset, timestamp
                long[] extremes = {-1, Long.MIN_VALUE, Long.MAX_VALUE, 0};
                int at = random.nextInt(b.length - 7);
                ByteBuffer.wrap(b).putLong(at, extremes[random.nextInt(extremes.length)]);
            }
            default -> {
                b = Arrays.copyOf(b, b.length + 1 + random.nextInt(8));
                random.nextBytes(b);
            }
        }
        return b;
    }

    @Test
    void damagedFramesOnlyEverFailAsCorrupt() {
        Random random = new Random(20261126L);
        List<byte[]> seeds = validFrames();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] input = mutate(seeds.get(random.nextInt(seeds.size())), random);
            if (random.nextBoolean()) {
                fixChecksum(input);
            }
            try {
                RecordCodec.decode(ByteBuffer.wrap(input));
            } catch (CorruptRecordException expected) {
                // the documented failure
            } catch (Throwable t) {
                fail("iteration " + i + ": decode threw " + t + " for " + Arrays.toString(input), t);
            }
        }
    }
}
