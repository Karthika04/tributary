package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.storage.NewRecord;
import io.github.karthika04.tributary.storage.Record;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer groups' committed offsets, stored in the internal topic
 * {@code __consumer_offsets} (the same name Kafka uses).
 *
 * <p>Every commit is appended to that topic as records of
 * {@code key = "group/topic/partition"}, {@code value = offset}. Using our own log
 * means commits get the same durability and crash recovery as messages, for free.
 * On startup the whole topic is replayed; the last commit for each key wins.
 *
 * <p>Known limitation: the topic only grows. Kafka keeps it small with log compaction
 * (dropping all but the newest record per key), which Tributary does not have yet.
 */
final class OffsetStore {

    static final String TOPIC = "__consumer_offsets";
    private static final int REPLAY_BATCH = 10_000;

    private final Partition partition;
    private final Clock clock;
    private final Map<String, Map<TopicPartition, Long>> committed = new ConcurrentHashMap<>();

    OffsetStore(Partition partition, Clock clock) {
        this.partition = partition;
        this.clock = clock;
        replay();
    }

    private void replay() {
        long offset = partition.startOffset();
        long end = partition.endOffset();
        try {
            while (offset < end) {
                List<Record> records = partition.read(offset, REPLAY_BATCH, 0);
                for (Record r : records) {
                    apply(new String(r.key(), StandardCharsets.UTF_8), ByteBuffer.wrap(r.value()).getLong());
                    offset = r.offset() + 1;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while loading committed offsets", e);
        }
    }

    private void apply(String key, long offset) {
        int second = key.lastIndexOf('/');
        int first = key.lastIndexOf('/', second - 1);
        String group = key.substring(0, first);
        TopicPartition tp = new TopicPartition(key.substring(first + 1, second),
                Integer.parseInt(key.substring(second + 1)));
        committed.computeIfAbsent(group, g -> new ConcurrentHashMap<>()).put(tp, offset);
    }

    /**
     * Saves offsets durably, then makes them visible. Synchronized so the in-memory
     * view always matches the order of records in the log.
     */
    synchronized void commit(String group, List<PartitionOffset> offsets) {
        if (offsets.isEmpty()) {
            return;
        }
        long now = clock.millis();
        List<NewRecord> records = new ArrayList<>(offsets.size());
        for (PartitionOffset po : offsets) {
            byte[] key = (group + "/" + po.topic() + "/" + po.partition()).getBytes(StandardCharsets.UTF_8);
            byte[] value = ByteBuffer.allocate(8).putLong(po.offset()).array();
            records.add(new NewRecord(now, key, value));
        }
        partition.append(records);
        for (PartitionOffset po : offsets) {
            committed.computeIfAbsent(group, g -> new ConcurrentHashMap<>()).put(po.topicPartition(), po.offset());
        }
    }

    /** The group's committed offset for the partition, or -1 if it never committed one. */
    long committed(String group, TopicPartition tp) {
        Map<TopicPartition, Long> offsets = committed.get(group);
        if (offsets == null) {
            return -1;
        }
        return offsets.getOrDefault(tp, -1L);
    }
}
