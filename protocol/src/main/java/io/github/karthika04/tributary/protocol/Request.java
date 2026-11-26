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
}
