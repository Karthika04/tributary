package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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

    static byte[] produce(String topic, int partition, long timestamp, List<ProducedRecord> records) {
        return encode(out -> {
            out.writeByte(PRODUCE);
            out.writeUTF(topic);
            out.writeInt(partition);
            out.writeLong(timestamp);
            out.writeInt(records.size());
            for (ProducedRecord r : records) {
                writeBytes(out, r.key());
                writeBytes(out, r.value());
            }
        });
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

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }
}
