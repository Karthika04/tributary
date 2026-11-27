package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import java.util.List;

/**
 * Every operation that changes the broker's data. A single broker applies them directly
 * ({@link LocalWrites}); a cluster member first replicates them through Raft
 * ({@link RaftWrites}), so every broker applies the same writes in the same order.
 */
interface Writes {

    void createTopic(String topic, int partitions);

    /** @return the offset of the first record */
    long produce(String topic, int partition, long timestamp, List<ProducedRecord> records);

    void commitOffsets(String group, List<PartitionOffset> offsets);
}
