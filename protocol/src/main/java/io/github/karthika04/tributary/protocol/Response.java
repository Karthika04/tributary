package io.github.karthika04.tributary.protocol;

import java.util.List;

/** The successful answer to each {@link Request}. Failures are an {@link ErrorCode} instead. */
public sealed interface Response {

    ApiKey apiKey();

    record CreateTopic() implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.CREATE_TOPIC;
        }
    }

    record TopicMetadata(String name, int partitions) {
    }

    record Metadata(List<TopicMetadata> topics) implements Response {
        public Metadata {
            topics = List.copyOf(topics);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.METADATA;
        }
    }

    /** {@code baseOffset} is the offset of the first record; the rest follow in order. */
    record Produce(long baseOffset) implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.PRODUCE;
        }
    }

    /**
     * @param startOffset the oldest offset still stored in the partition
     * @param endOffset   the offset the next record will get; a consumer at this
     *                    offset is caught up
     */
    record Fetch(long startOffset, long endOffset, List<FetchedRecord> records) implements Response {
        public Fetch {
            records = List.copyOf(records);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.FETCH;
        }
    }

    record ListOffsets(long startOffset, long endOffset) implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.LIST_OFFSETS;
        }
    }
}
