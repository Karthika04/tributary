package io.github.karthika04.tributary.protocol;

/** Where a consumer wants to read next in one partition: part of a {@link Request.MultiFetch}. */
public record FetchPosition(String topic, int partition, long offset) {

    public FetchPosition {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is required");
        }
    }
}
