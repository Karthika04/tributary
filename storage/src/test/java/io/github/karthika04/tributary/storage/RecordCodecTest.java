package io.github.karthika04.tributary.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class RecordCodecTest {

    @Test
    void roundTripsKeyAndValue() {
        Record original = new Record(42, 1_700_000_000_000L, "user-42".getBytes(UTF_8),
                "clicked checkout".getBytes(UTF_8));

        Record decoded = RecordCodec.decode(RecordCodec.encode(original));

        assertEquals(original, decoded);
    }

    @Test
    void roundTripsNullKeyAndEmptyValue() {
        Record original = new Record(0, 5, null, new byte[0]);

        Record decoded = RecordCodec.decode(RecordCodec.encode(original));

        assertNull(decoded.key());
        assertEquals(original, decoded);
    }

    @Test
    void frameSizeMatchesEncodedBytes() {
        Record record = new Record(7, 9, "k".getBytes(UTF_8), "value".getBytes(UTF_8));

        assertEquals(RecordCodec.frameSize(record), RecordCodec.encode(record).remaining());
        assertEquals(RecordCodec.FRAME_OVERHEAD + 1 + 5, RecordCodec.frameSize(record));
    }

    @Test
    void detectsAFlippedBitAnywhereInTheBody() {
        Record record = new Record(3, 4, "key".getBytes(UTF_8), "important data".getBytes(UTF_8));
        int frameSize = RecordCodec.frameSize(record);

        // Damage every byte after the crc field, one at a time; each must be caught.
        for (int i = RecordCodec.LENGTH_FIELD_SIZE + RecordCodec.CRC_FIELD_SIZE; i < frameSize; i++) {
            ByteBuffer frame = RecordCodec.encode(record);
            frame.put(i, (byte) (frame.get(i) ^ 0x01));
            assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(frame), "byte " + i);
        }
    }

    @Test
    void rejectsAFrameThatIsCutOff() {
        ByteBuffer full = RecordCodec.encode(new Record(1, 1, null, "hello".getBytes(UTF_8)));
        ByteBuffer cutOff = full.slice(0, full.remaining() - 2);

        assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(cutOff));
    }

    @Test
    void rejectsAnImpossibleLength() {
        ByteBuffer frame = RecordCodec.encode(new Record(1, 1, null, "hello".getBytes(UTF_8)));
        frame.putInt(0, -5);

        assertThrows(CorruptRecordException.class, () -> RecordCodec.decode(frame));
    }

    @Test
    void decodeDoesNotMoveTheCallersBuffer() {
        ByteBuffer frame = RecordCodec.encode(new Record(1, 1, null, "hello".getBytes(UTF_8)));
        int before = frame.position();

        RecordCodec.decode(frame);

        assertEquals(before, frame.position());
    }
}
