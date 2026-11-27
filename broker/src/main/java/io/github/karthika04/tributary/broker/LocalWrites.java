package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.storage.NewRecord;
import java.util.ArrayList;
import java.util.List;

/** Applies writes straight to this broker's storage. */
final class LocalWrites implements Writes {

    private final TopicManager topics;
    private final OffsetStore offsets;

    LocalWrites(TopicManager topics, OffsetStore offsets) {
        this.topics = topics;
        this.offsets = offsets;
    }

    @Override
    public void createTopic(String topic, int partitions) {
        topics.createTopic(topic, partitions);
    }

    @Override
    public long produce(String topic, int partition, long timestamp, List<ProducedRecord> records) {
        List<NewRecord> stored = new ArrayList<>(records.size());
        for (ProducedRecord r : records) {
            stored.add(new NewRecord(timestamp, r.key(), r.value()));
        }
        return topics.partition(topic, partition).append(stored);
    }

    @Override
    public void commitOffsets(String group, List<PartitionOffset> commits) {
        offsets.commit(group, commits);
    }
}
