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

    /**
     * @param generation increases every time the group's partitions are reassigned
     * @param assignment the partitions this member now owns
     */
    record JoinGroup(String memberId, int generation, List<TopicPartition> assignment) implements Response {
        public JoinGroup {
            assignment = List.copyOf(assignment);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.JOIN_GROUP;
        }
    }

    record Heartbeat() implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.HEARTBEAT;
        }
    }

    record LeaveGroup() implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.LEAVE_GROUP;
        }
    }

    /**
     * @param rejected partitions that were not saved because this member does not own
     *                 them (any more). The rest were saved.
     */
    record CommitOffsets(List<TopicPartition> rejected) implements Response {
        public CommitOffsets {
            rejected = List.copyOf(rejected);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.COMMIT_OFFSETS;
        }
    }

    /** One entry per requested partition; offset -1 if the group never committed it. */
    record FetchOffsets(List<PartitionOffset> offsets) implements Response {
        public FetchOffsets {
            offsets = List.copyOf(offsets);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.FETCH_OFFSETS;
        }
    }

    record RaftVote(long term, boolean granted) implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.RAFT_VOTE;
        }
    }

    record RaftAppend(long term, boolean success, long matchIndex) implements Response {
        @Override
        public ApiKey apiKey() {
            return ApiKey.RAFT_APPEND;
        }
    }
}
