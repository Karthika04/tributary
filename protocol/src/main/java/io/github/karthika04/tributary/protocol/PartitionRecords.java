package io.github.karthika04.tributary.protocol;

import java.util.List;

/**
 * One partition's part of a {@link Response.MultiFetch}. Each partition has its own
 * error, so one bad partition (say, a position deleted by retention) does not fail the
 * whole fetch.
 *
 * @param error       {@link ErrorCode#NONE}, or why this partition could not be read
 * @param startOffset the partition's oldest offset (0 if {@code error} is set)
 * @param endOffset   the offset the next record will get (0 if {@code error} is set)
 */
public record PartitionRecords(String topic, int partition, ErrorCode error, long startOffset, long endOffset,
                               List<FetchedRecord> records) {

    public PartitionRecords {
        if (topic == null || topic.isEmpty() || error == null) {
            throw new IllegalArgumentException("topic and error are required");
        }
        records = List.copyOf(records);
    }
}
