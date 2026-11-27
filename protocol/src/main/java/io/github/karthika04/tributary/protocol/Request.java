package io.github.karthika04.tributary.protocol;

import java.util.List;

/**
 * Every request a client can send. A sealed interface lists all its implementations,
 * so a {@code switch} over a request is checked by the compiler: add a new request
 * type and every switch that forgets it stops compiling.
 */
public sealed interface Request {

    ApiKey apiKey();

    /** Create a topic with this many partitions. */
    record CreateTopic(String topic, int partitions) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.CREATE_TOPIC;
        }
    }

    /** List every topic and its partition count. */
    record Metadata() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.METADATA;
        }
    }

    /** Append records to one partition. The broker assigns consecutive offsets. */
    record Produce(String topic, int partition, Compression compression, List<ProducedRecord> records)
            implements Request {
        public Produce {
            records = List.copyOf(records);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.PRODUCE;
        }
    }

    /**
     * Read records from one partition, starting at {@code offset}.
     *
     * @param maxWaitMs if there is nothing new yet, the broker may hold the request
     *                  open up to this long and answer as soon as data arrives
     *                  ("long polling"). 0 answers immediately.
     * @param compression how the broker should compress the records it sends back
     */
    record Fetch(String topic, int partition, long offset, int maxRecords, int maxWaitMs, Compression compression)
            implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.FETCH;
        }
    }

    /** Ask for the oldest and next offsets of a partition. */
    record ListOffsets(String topic, int partition) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.LIST_OFFSETS;
        }
    }

    /**
     * Join a consumer group (or re-join after a rebalance).
     *
     * @param memberId          null for a brand-new member; the id the broker gave
     *                          earlier when re-joining
     * @param topics            the topics this member wants to read
     * @param sessionTimeoutMs  if no heartbeat arrives for this long, the member is
     *                          considered dead and its partitions are reassigned
     * @param assignor          "range" or "roundrobin": how partitions are divided
     */
    record JoinGroup(String groupId, String memberId, List<String> topics, int sessionTimeoutMs, String assignor)
            implements Request {
        public JoinGroup {
            topics = List.copyOf(topics);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.JOIN_GROUP;
        }
    }

    /** "I am still alive." Also how a member learns that a rebalance happened. */
    record Heartbeat(String groupId, String memberId, int generation) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.HEARTBEAT;
        }
    }

    /** Leave the group now, so its partitions move to other members right away. */
    record LeaveGroup(String groupId, String memberId) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.LEAVE_GROUP;
        }
    }

    /** Save the group's progress. Only the member that owns a partition may commit it. */
    record CommitOffsets(String groupId, String memberId, List<PartitionOffset> offsets) implements Request {
        public CommitOffsets {
            offsets = List.copyOf(offsets);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.COMMIT_OFFSETS;
        }
    }

    /** Read a group's committed offsets. */
    record FetchOffsets(String groupId, List<TopicPartition> partitions) implements Request {
        public FetchOffsets {
            partitions = List.copyOf(partitions);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.FETCH_OFFSETS;
        }
    }

    /** Broker to broker: Raft RequestVote. */
    record RaftVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.RAFT_VOTE;
        }
    }

    /** Broker to broker: Raft AppendEntries. */
    record RaftAppend(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<RaftEntry> entries,
                      long leaderCommit) implements Request {
        public RaftAppend {
            entries = List.copyOf(entries);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.RAFT_APPEND;
        }
    }
}
