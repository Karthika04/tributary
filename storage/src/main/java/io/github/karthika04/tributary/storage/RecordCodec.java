package io.github.karthika04.tributary.storage;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * Turns a {@link Record} into bytes for disk, and back.
 *
 * <p>Every record is written as one "frame". All numbers are big-endian.
 *
 * <pre>
 * +---------+---------+----------+-----------+--------+-----+----------+-------+
 * | length  | crc     | offset   | timestamp | keyLen | key | valueLen | value |
 * | 4 bytes | 4 bytes | 8 bytes  | 8 bytes   | 4      | ... | 4        | ...   |
 * +---------+---------+----------+-----------+--------+-----+----------+-------+
 *            \_________________ length counts these bytes _________________/
 *                      \____________ crc covers these bytes ______________/
 * </pre>
 *
 * <ul>
 *   <li>{@code length} lets a reader jump straight to the next frame.</li>
 *   <li>{@code crc} (CRC32C, the same checksum Kafka uses) detects bytes that were
 *       damaged or only partly written, for example by a crash mid-write.</li>
 *   <li>{@code keyLen} is -1 for a null key.</li>
 * </ul>
 */
final class RecordCodec {

    static final int LENGTH_FIELD_SIZE = 4;
    static final int CRC_FIELD_SIZE = 4;
    /** offset + timestamp + keyLen + valueLen. */
    static final int FIXED_BODY_SIZE = 8 + 8 + 4 + 4;
    /** Bytes every frame has, before the key and value. */
    static final int FRAME_OVERHEAD = LENGTH_FIELD_SIZE + CRC_FIELD_SIZE + FIXED_BODY_SIZE;

    private RecordCodec() {
    }

    static int frameSize(byte[] key, byte[] value) {
        return FRAME_OVERHEAD + (key == null ? 0 : key.length) + value.length;
    }

    static int frameSize(Record record) {
        return frameSize(record.key(), record.value());
    }

    /** Encodes into a new buffer, ready to read (position 0, limit = frame size). */
    static ByteBuffer encode(Record record) {
        ByteBuffer buffer = ByteBuffer.allocate(frameSize(record));
        encode(record, buffer);
        return buffer.flip();
    }

    /** Writes one frame at the buffer's current position and advances it. */
    static void encode(Record record, ByteBuffer dst) {
        int frameSize = frameSize(record);
        dst.putInt(frameSize - LENGTH_FIELD_SIZE);
        int crcPosition = dst.position();
        dst.putInt(0); // placeholder, filled in once the body is written
        int bodyStart = dst.position();

        dst.putLong(record.offset());
        dst.putLong(record.timestamp());
        if (record.key() == null) {
            dst.putInt(-1);
        } else {
            dst.putInt(record.key().length);
            dst.put(record.key());
        }
        dst.putInt(record.value().length);
        dst.put(record.value());

        CRC32C crc = new CRC32C();
        crc.update(dst.slice(bodyStart, dst.position() - bodyStart));
        dst.putInt(crcPosition, (int) crc.getValue());
    }

    /**
     * Decodes one frame. The buffer must start at the frame's length field and hold
     * the whole frame. The buffer itself is not modified.
     *
     * @throws CorruptRecordException if the bytes are not a valid frame
     */
    static Record decode(ByteBuffer frame) {
        ByteBuffer in = frame.duplicate();
        if (in.remaining() < LENGTH_FIELD_SIZE + CRC_FIELD_SIZE) {
            throw new CorruptRecordException("frame too short: " + in.remaining() + " bytes");
        }
        int length = in.getInt();
        if (length < CRC_FIELD_SIZE + FIXED_BODY_SIZE || length > in.remaining()) {
            throw new CorruptRecordException(
                    "invalid frame length " + length + " (" + in.remaining() + " bytes available)");
        }
        int storedCrc = in.getInt();
        ByteBuffer body = in.slice(in.position(), length - CRC_FIELD_SIZE);

        CRC32C crc = new CRC32C();
        crc.update(body.duplicate());
        if ((int) crc.getValue() != storedCrc) {
            throw new CorruptRecordException("checksum mismatch: stored " + storedCrc
                    + ", computed " + (int) crc.getValue());
        }

        long offset = body.getLong();
        long timestamp = body.getLong();
        int keyLength = body.getInt();
        if (keyLength < -1 || keyLength > body.remaining() - 4) {
            throw new CorruptRecordException("invalid key length " + keyLength);
        }
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            body.get(key);
        }
        int valueLength = body.getInt();
        if (valueLength != body.remaining()) {
            throw new CorruptRecordException("invalid value length " + valueLength);
        }
        byte[] value = new byte[valueLength];
        body.get(value);
        if (offset < 0) {
            throw new CorruptRecordException("negative offset " + offset);
        }
        return new Record(offset, timestamp, key, value);
    }
}
