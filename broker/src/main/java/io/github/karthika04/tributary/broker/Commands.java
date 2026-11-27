package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The commands stored in the Raft log: one per write. Each carries everything needed to
 * apply it identically on every broker, including the timestamp the leader chose, so no
 * broker ever consults its own clock while applying.
 */
final class Commands {

    private static final byte CREATE_TOPIC = 1;
    private static final byte PRODUCE = 2;
    private static final byte COMMIT_OFFSETS = 3;

    private Commands() {
    }

    static byte[] createTopic(String topic, int partitions) {
        return encode(out -> {
            out.writeByte(CREATE_TOPIC);
            out.writeUTF(topic);
            out.writeInt(partitions);
        });
    }

    /**
     * Encodes a produce command into a buffer of exactly the right size.
     *
     * <p>This is the hot path: every produced byte passes through it on the leader, then
     * through {@link #apply} on every broker. A growing {@code ByteArrayOutputStream} here
     * copied a 1 MB command about twice over while doubling its buffer, and profiling
     * (JFR) put a third of the leader's CPU in that copying. The bytes are the same as
     * {@code DataOutputStream} wrote, so existing Raft logs still decode. Encoding got
     * about 3x faster (540 to 175 us for 1 MB). Decoding the same way was tried too and
     * measured no faster (the time there is copying the record bytes, which any decoder
     * must do), so it stays the simpler stream-based code.
     */
    static byte[] produce(String topic, int partition, long timestamp, List<ProducedRecord> records) {
        byte[] name = asciiName(topic);
        long size = 1 + 2 + name.length + 4 + 8 + 4;
        for (ProducedRecord r : records) {
            size += 4 + (r.key() == null ? 0 : r.key().length) + 4 + r.value().length;
        }
        if (size > Integer.MAX_VALUE - 8) {
            throw new IllegalArgumentException("produce command of " + size + " bytes is too large");
        }
        ByteBuffer out = ByteBuffer.allocate((int) size);
        out.put(PRODUCE).putShort((short) name.length).put(name).putInt(partition).putLong(timestamp)
                .putInt(records.size());
        for (ProducedRecord r : records) {
            putBytes(out, r.key());
            putBytes(out, r.value());
        }
        return out.array();
    }

    /**
     * Topic names are ASCII (letters, digits, '.', '_', '-'), and for ASCII the "modified
     * UTF-8" of {@code DataOutputStream.writeUTF} is plain UTF-8, so this matches it byte for byte.
     */
    private static byte[] asciiName(String topic) {
        byte[] bytes = topic.getBytes(StandardCharsets.US_ASCII);
        if (bytes.length > 0xFFFF || !topic.equals(new String(bytes, StandardCharsets.US_ASCII))) {
            throw new IllegalArgumentException("topic names must be short ASCII: " + topic);
        }
        return bytes;
    }

    private static void putBytes(ByteBuffer out, byte[] bytes) {
        if (bytes == null) {
            out.putInt(-1);
        } else {
            out.putInt(bytes.length).put(bytes);
        }
    }

    static byte[] commitOffsets(String group, List<PartitionOffset> offsets) {
        return encode(out -> {
            out.writeByte(COMMIT_OFFSETS);
            out.writeUTF(group);
            out.writeInt(offsets.size());
            for (PartitionOffset po : offsets) {
                out.writeUTF(po.topic());
                out.writeInt(po.partition());
                out.writeLong(po.offset());
            }
        });
    }

    /** Decodes a command and performs it. @return the produce's base offset, else null */
    static Object apply(byte[] command, Writes target) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
            byte type = in.readByte();
            switch (type) {
                case CREATE_TOPIC -> {
                    target.createTopic(in.readUTF(), in.readInt());
                    return null;
                }
                case PRODUCE -> {
                    String topic = in.readUTF();
                    int partition = in.readInt();
                    long timestamp = in.readLong();
                    int count = in.readInt();
                    List<ProducedRecord> records = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        records.add(new ProducedRecord(readBytes(in), readBytes(in)));
                    }
                    return target.produce(topic, partition, timestamp, records);
                }
                case COMMIT_OFFSETS -> {
                    String group = in.readUTF();
                    int count = in.readInt();
                    List<PartitionOffset> offsets = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        offsets.add(new PartitionOffset(in.readUTF(), in.readInt(), in.readLong()));
                    }
                    target.commitOffsets(group, offsets);
                    return null;
                }
                default -> throw new IllegalStateException("unknown command type " + type);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt command in the raft log", e);
        }
    }

    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] encode(Body body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            body.write(new DataOutputStream(bytes));
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e); // in-memory stream
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes); // EOFException if the command claims more bytes than it has
        return bytes;
    }
}
