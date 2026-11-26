package io.github.karthika04.tributary.protocol;

/** One partition of one topic, for example {@code orders-2}. */
public record TopicPartition(String topic, int partition) {

    public TopicPartition {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, got " + partition);
        }
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
