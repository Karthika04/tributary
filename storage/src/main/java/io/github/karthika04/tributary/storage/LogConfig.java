package io.github.karthika04.tributary.storage;

/**
 * Settings for one {@link Log}. Start from {@link #defaults()} and change what you need:
 *
 * <pre>{@code
 * LogConfig config = LogConfig.defaults()
 *         .withSegmentBytes(16 * 1024 * 1024)
 *         .withRetentionMs(Duration.ofDays(7).toMillis());
 * }</pre>
 *
 * @param segmentBytes     roll to a new segment once the active one reaches this size
 * @param indexBytes       size of each segment's index file; caps records per segment
 *                         at {@code indexBytes / 8}
 * @param maxRecordBytes   largest single record frame (header + key + value) accepted
 * @param retentionMs      delete segments whose newest record is older than this;
 *                         -1 keeps data forever
 * @param retentionBytes   delete oldest segments while the log is bigger than this;
 *                         -1 means no size limit
 * @param flushEveryAppend force data to the physical disk after every append. Safest,
 *                         but much slower; by default the OS decides when to write
 */
public record LogConfig(
        int segmentBytes,
        int indexBytes,
        int maxRecordBytes,
        long retentionMs,
        long retentionBytes,
        boolean flushEveryAppend) {

    public static final int DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024;
    public static final int DEFAULT_INDEX_BYTES = 10 * 1024 * 1024;
    public static final int DEFAULT_MAX_RECORD_BYTES = 1024 * 1024;

    public LogConfig {
        if (maxRecordBytes < RecordCodec.FRAME_OVERHEAD) {
            throw new IllegalArgumentException("maxRecordBytes must be at least "
                    + RecordCodec.FRAME_OVERHEAD + ", got " + maxRecordBytes);
        }
        if (segmentBytes < maxRecordBytes) {
            throw new IllegalArgumentException("segmentBytes (" + segmentBytes
                    + ") must be at least maxRecordBytes (" + maxRecordBytes + ")");
        }
        if (indexBytes < Index.ENTRY_SIZE) {
            throw new IllegalArgumentException("indexBytes must be at least " + Index.ENTRY_SIZE);
        }
        if (retentionMs < -1 || retentionBytes < -1) {
            throw new IllegalArgumentException("retention settings must be -1 (unlimited) or >= 0");
        }
    }

    public static LogConfig defaults() {
        return new LogConfig(DEFAULT_SEGMENT_BYTES, DEFAULT_INDEX_BYTES, DEFAULT_MAX_RECORD_BYTES,
                -1, -1, false);
    }

    public LogConfig withSegmentBytes(int value) {
        return new LogConfig(value, indexBytes, maxRecordBytes, retentionMs, retentionBytes, flushEveryAppend);
    }

    public LogConfig withIndexBytes(int value) {
        return new LogConfig(segmentBytes, value, maxRecordBytes, retentionMs, retentionBytes, flushEveryAppend);
    }

    public LogConfig withMaxRecordBytes(int value) {
        return new LogConfig(segmentBytes, indexBytes, value, retentionMs, retentionBytes, flushEveryAppend);
    }

    public LogConfig withRetentionMs(long value) {
        return new LogConfig(segmentBytes, indexBytes, maxRecordBytes, value, retentionBytes, flushEveryAppend);
    }

    public LogConfig withRetentionBytes(long value) {
        return new LogConfig(segmentBytes, indexBytes, maxRecordBytes, retentionMs, value, flushEveryAppend);
    }

    public LogConfig withFlushEveryAppend(boolean value) {
        return new LogConfig(segmentBytes, indexBytes, maxRecordBytes, retentionMs, retentionBytes, value);
    }

    int maxRecordsPerSegment() {
        return indexBytes / Index.ENTRY_SIZE;
    }
}
