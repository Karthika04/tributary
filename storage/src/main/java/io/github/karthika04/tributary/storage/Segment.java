package io.github.karthika04.tributary.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * One piece of the log: a ".log" file of record frames plus a ".index" file that
 * points into it. Both are named after the segment's base offset (its first record),
 * padded to 20 digits so they sort correctly, e.g. {@code 00000000000000001000.log}.
 *
 * <p>Why split the log into segments? Deleting old data becomes deleting whole files,
 * which is instant, instead of rewriting one giant file. Only the newest segment, the
 * "active" one, is ever written to. Older segments are read-only.
 */
final class Segment implements Closeable {

    static final String LOG_SUFFIX = ".log";
    static final String INDEX_SUFFIX = ".index";

    private final long baseOffset;
    private final LogConfig config;
    private final Store store;
    private final Path indexPath;
    private Index index;
    private boolean active;
    private volatile long nextOffset;
    private volatile long maxTimestamp = -1;
    private long lastRecoveryDroppedBytes;

    private Segment(long baseOffset, LogConfig config, Store store, Path indexPath) {
        this.baseOffset = baseOffset;
        this.config = config;
        this.store = store;
        this.indexPath = indexPath;
        this.nextOffset = baseOffset;
    }

    static String fileName(long baseOffset, String suffix) {
        return String.format("%020d%s", baseOffset, suffix);
    }

    /** Creates a brand-new, empty segment to append to. */
    static Segment createActive(Path dir, long baseOffset, LogConfig config) throws IOException {
        Path logPath = dir.resolve(fileName(baseOffset, LOG_SUFFIX));
        Path indexPath = dir.resolve(fileName(baseOffset, INDEX_SUFFIX));
        Files.deleteIfExists(logPath);
        Files.deleteIfExists(indexPath);
        Segment segment = new Segment(baseOffset, config, Store.open(logPath), indexPath);
        try {
            segment.index = Index.openForAppend(indexPath, config.indexBytes());
            segment.active = true;
            return segment;
        } catch (IOException | RuntimeException e) {
            segment.closeQuietly();
            throw e;
        }
    }

    /**
     * Opens a segment that already exists on disk.
     *
     * @param active  true for the newest segment, which will be appended to
     * @param recover true to rebuild the index by scanning the log file, dropping any
     *                half-written frames at the end. Needed after a crash.
     */
    static Segment open(Path dir, long baseOffset, LogConfig config, boolean active, boolean recover)
            throws IOException {
        Path logPath = dir.resolve(fileName(baseOffset, LOG_SUFFIX));
        Path indexPath = dir.resolve(fileName(baseOffset, INDEX_SUFFIX));
        Segment segment = new Segment(baseOffset, config, Store.open(logPath), indexPath);
        try {
            if (!recover && looksLikeValidIndex(indexPath)) {
                segment.index = active
                        ? Index.openForAppend(indexPath, config.indexBytes())
                        : Index.openReadOnly(indexPath);
                if (segment.loadStateFromIndex()) {
                    segment.active = active;
                    return segment;
                }
                // The index does not match the log file; fall through and rebuild it.
                segment.index.close();
            }
            segment.index = Index.openForAppend(indexPath, config.indexBytes());
            segment.recover();
            if (!active) {
                segment.index.close();
                segment.index = Index.openReadOnly(indexPath);
            }
            segment.active = active;
            return segment;
        } catch (IOException | RuntimeException e) {
            segment.closeQuietly();
            throw e;
        }
    }

    private static boolean looksLikeValidIndex(Path indexPath) throws IOException {
        return Files.exists(indexPath) && Files.size(indexPath) % Index.ENTRY_SIZE == 0;
    }

    /**
     * Trusts the index, but checks that its last entry points at the last frame in the
     * log file. Returns false if they disagree, so the caller can rebuild instead.
     */
    private boolean loadStateFromIndex() throws IOException {
        int count = index.entries();
        nextOffset = baseOffset + count;
        if (count == 0) {
            maxTimestamp = -1;
            return store.size() == 0;
        }
        try {
            int lastPosition = index.positionOf(count - 1);
            ByteBuffer frame = store.readFrame(lastPosition, config.maxRecordBytes());
            if (lastPosition + frame.remaining() != store.size()) {
                return false;
            }
            Record last = RecordCodec.decode(frame);
            if (last.offset() != nextOffset - 1) {
                return false;
            }
            // The broker stamps records with its own clock, so the last record is the newest.
            maxTimestamp = last.timestamp();
            return true;
        } catch (CorruptRecordException | IndexOutOfBoundsException e) {
            return false;
        }
    }

    /**
     * Crash recovery: reads every frame from the start of the log file, checking each
     * checksum and offset, and rebuilds the index as it goes. At the first frame that
     * is cut off or damaged, everything from there to the end is truncated, because a
     * record that was not fully written was never acknowledged as stored.
     *
     * @return how many bytes were dropped from the end of the file
     */
    long recover() throws IOException {
        index.reset();
        long fileSize = store.size();
        long position = 0;
        long expectedOffset = baseOffset;
        long newestTimestamp = -1;
        while (position < fileSize && !index.isFull()) {
            Record record;
            int frameSize;
            try {
                ByteBuffer frame = store.readFrame(position, config.maxRecordBytes());
                frameSize = frame.remaining();
                record = RecordCodec.decode(frame);
            } catch (CorruptRecordException e) {
                break;
            }
            if (record.offset() != expectedOffset) {
                break;
            }
            index.append((int) (expectedOffset - baseOffset), (int) position);
            newestTimestamp = Math.max(newestTimestamp, record.timestamp());
            expectedOffset++;
            position += frameSize;
        }
        long dropped = fileSize - position;
        if (dropped > 0) {
            store.truncateTo(position);
        }
        nextOffset = expectedOffset;
        maxTimestamp = newestTimestamp;
        lastRecoveryDroppedBytes = dropped;
        return dropped;
    }

    long lastRecoveryDroppedBytes() {
        return lastRecoveryDroppedBytes;
    }

    /** True if a batch of this many records and bytes fits without rolling. */
    boolean canFit(long batchBytes, int batchCount) {
        return store.size() + batchBytes <= config.segmentBytes()
                && index.entries() + batchCount <= config.maxRecordsPerSegment();
    }

    /**
     * Appends a batch with a single write to the log file.
     *
     * <p>Order matters for crash safety: the frames are written to the log file first
     * and the index second. If we crash in between, recovery rebuilds the index from
     * the log file, so the two can never permanently disagree.
     *
     * @return the offset given to the first record of the batch
     */
    long append(List<NewRecord> batch) throws IOException {
        if (!active) {
            throw new IllegalStateException("segment " + baseOffset + " is read-only");
        }
        int totalBytes = 0;
        for (NewRecord r : batch) {
            totalBytes += r.frameSize();
        }
        long startPosition = store.size();
        if (startPosition + totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("segment " + baseOffset + " would exceed 2 GiB");
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
        int[] positions = new int[batch.size()];
        long firstOffset = nextOffset;
        long offset = firstOffset;
        long newest = maxTimestamp;
        for (int i = 0; i < batch.size(); i++) {
            NewRecord r = batch.get(i);
            positions[i] = (int) (startPosition + buffer.position());
            RecordCodec.encode(new Record(offset, r.timestamp(), r.key(), r.value()), buffer);
            newest = Math.max(newest, r.timestamp());
            offset++;
        }
        buffer.flip();
        store.append(buffer);

        for (int i = 0; i < positions.length; i++) {
            index.append((int) (firstOffset + i - baseOffset), positions[i]);
        }
        nextOffset = offset;
        maxTimestamp = newest;
        if (config.flushEveryAppend()) {
            flush();
        }
        return firstOffset;
    }

    Record read(long offset) throws IOException {
        if (offset < baseOffset || offset >= nextOffset) {
            throw new IllegalArgumentException("offset " + offset + " is not in segment ["
                    + baseOffset + ", " + nextOffset + ")");
        }
        int position = index.positionOf((int) (offset - baseOffset));
        Record record = RecordCodec.decode(store.readFrame(position, config.maxRecordBytes()));
        if (record.offset() != offset) {
            throw new CorruptRecordException("expected offset " + offset + " at position "
                    + position + " but found " + record.offset());
        }
        return record;
    }

    /** Reopens a sealed segment for writing, so its end can be truncated. */
    void makeActive() throws IOException {
        if (active) {
            return;
        }
        index.close();
        index = Index.openForAppend(indexPath, config.indexBytes());
        active = true;
    }

    /** Removes every record with an offset of {@code offset} or more. */
    void truncateTo(long offset) throws IOException {
        if (!active) {
            throw new IllegalStateException("segment " + baseOffset + " is read-only");
        }
        if (offset >= nextOffset) {
            return;
        }
        if (offset < baseOffset) {
            throw new IllegalArgumentException("offset " + offset + " is before segment " + baseOffset);
        }
        int keep = (int) (offset - baseOffset);
        long position = keep < index.entries() ? index.positionOf(keep) : store.size();
        store.truncateTo(position);
        index.truncate(keep);
        nextOffset = offset;
        maxTimestamp = keep == 0 ? -1 : read(offset - 1).timestamp();
    }

    /** Called when a newer segment takes over: flush everything and reopen read-only. */
    void makeReadOnly() throws IOException {
        if (!active) {
            return;
        }
        store.flush();
        index.close();
        index = Index.openReadOnly(indexPath);
        active = false;
    }

    void flush() throws IOException {
        store.flush();
        index.flush();
    }

    long baseOffset() {
        return baseOffset;
    }

    long nextOffset() {
        return nextOffset;
    }

    long recordCount() {
        return nextOffset - baseOffset;
    }

    long sizeInBytes() {
        return store.size();
    }

    /** Timestamp of the newest record, or -1 if the segment is empty. */
    long maxTimestamp() {
        return maxTimestamp;
    }

    boolean isActive() {
        return active;
    }

    @Override
    public void close() throws IOException {
        try {
            index.close();
        } finally {
            store.close();
        }
    }

    /** Closes the segment and removes its files from disk. */
    void delete() throws IOException {
        close();
        Files.deleteIfExists(store.path());
        Files.deleteIfExists(indexPath);
    }

    private void closeQuietly() {
        try {
            if (index != null) {
                index.close();
            }
        } catch (IOException | RuntimeException ignored) {
            // already failing; keep the original exception
        }
        try {
            store.close();
        } catch (IOException | RuntimeException ignored) {
            // already failing; keep the original exception
        }
    }
}
