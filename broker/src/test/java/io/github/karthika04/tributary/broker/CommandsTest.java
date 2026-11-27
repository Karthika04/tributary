package io.github.karthika04.tributary.broker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandsTest {

    private static final List<ProducedRecord> RECORDS = List.of(
            new ProducedRecord("k1".getBytes(UTF_8), "first".getBytes(UTF_8)),
            new ProducedRecord(null, "second, no key".getBytes(UTF_8)),
            new ProducedRecord(new byte[0], new byte[0]));

    /** Records what a decoded command asked for. */
    private static final class Recording implements Writes {
        String topic;
        int partition;
        long timestamp;
        List<ProducedRecord> records = new ArrayList<>();

        @Override
        public void createTopic(String t, int partitions) {
        }

        @Override
        public long produce(String t, int p, long ts, List<ProducedRecord> rs) {
            topic = t;
            partition = p;
            timestamp = ts;
            records = rs;
            return 77;
        }

        @Override
        public void commitOffsets(String group, List<PartitionOffset> offsets) {
        }
    }

    @Test
    void produceCommandsAreByteForByteTheOldFormat() throws IOException {
        // Exactly what the DataOutputStream version wrote. Raft logs on disk hold this,
        // so the faster encoder must not change a byte.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream old = new DataOutputStream(bytes);
        old.writeByte(2);
        old.writeUTF("orders.eu-1_v2");
        old.writeInt(3);
        old.writeLong(1_700_000_000_123L);
        old.writeInt(RECORDS.size());
        for (ProducedRecord r : RECORDS) {
            if (r.key() == null) {
                old.writeInt(-1);
            } else {
                old.writeInt(r.key().length);
                old.write(r.key());
            }
            old.writeInt(r.value().length);
            old.write(r.value());
        }

        assertArrayEquals(bytes.toByteArray(), Commands.produce("orders.eu-1_v2", 3, 1_700_000_000_123L, RECORDS));
    }

    @Test
    void produceRoundTrips() {
        Recording target = new Recording();
        Object result = Commands.apply(Commands.produce("orders", 2, 99L, RECORDS), target);

        assertEquals(77L, result);
        assertEquals("orders", target.topic);
        assertEquals(2, target.partition);
        assertEquals(99L, target.timestamp);
        assertEquals(RECORDS, target.records);
        assertNull(target.records.get(1).key());
    }

    @Test
    void aCommandClaimingMoreBytesThanItHasIsRejected() {
        byte[] command = Commands.produce("orders", 0, 1L, List.of(new ProducedRecord(null, new byte[10])));
        // The value's length field sits 4 + 10 bytes from the end; claim 1,000 bytes instead of 10.
        java.nio.ByteBuffer.wrap(command).putInt(command.length - 14, 1_000);

        assertThrows(java.io.UncheckedIOException.class, () -> Commands.apply(command, new Recording()));
    }
}
