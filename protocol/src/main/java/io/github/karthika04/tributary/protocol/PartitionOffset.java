package io.github.karthika04.tributary.protocol;

/**
 * An offset for one partition, used when a consumer group commits its progress.
 * By convention a committed offset is the <em>next</em> offset to read, not the last
 * one read. -1 means "nothing committed yet".
 */
public record PartitionOffset(String topic, int partition, long offset) {

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}
