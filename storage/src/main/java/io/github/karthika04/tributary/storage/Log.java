package io.github.karthika04.tributary.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An append-only, segmented commit log: the storage engine at the heart of Tributary.
 * Each topic partition gets one {@code Log} in its own directory.
 *
 * <pre>
 *   data/orders-0/
 *     00000000000000000000.log    &lt;- records 0..999   (read-only)
 *     00000000000000000000.index
 *     00000000000000001000.log    &lt;- records 1000..   (active, appended to)
 *     00000000000000001000.index
 * </pre>
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Offsets start at 0, go up by exactly 1 per record, and never repeat.</li>
 *   <li>Records come back exactly as written, or a {@link CorruptRecordException}
 *       says they were damaged. Bad bytes are never returned silently.</li>
 *   <li>After a crash, reopening keeps every complete record and drops a half-written
 *       one at the end.</li>
 * </ul>
 *
 * <p>Threading: safe to share between threads. Appends take an exclusive write lock,
 * so offsets are handed out in order. Reads take a shared read lock, so many readers
 * run at the same time.
 *
 * <p>Errors from the file system are thrown as {@link UncheckedIOException}.
 */
public final class Log implements Closeable {

    static final String CLEAN_SHUTDOWN_FILE = ".clean-shutdown";
    static final String LOCK_FILE = ".lock";

    private final Path dir;
    private final LogConfig config;
    private final Clock clock;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    /** Segments by base offset, oldest first. TreeMap.floorEntry finds the right one fast. */
    private final TreeMap<Long, Segment> segments = new TreeMap<>();
    private final FileChannel lockChannel;
    private final FileLock dirLock;
    private Segment active;
    private boolean closed;
    private long recoveredBytesDropped;

    private Log(Path dir, LogConfig config, Clock clock, FileChannel lockChannel, FileLock dirLock) {
        this.dir = dir;
        this.config = config;
        this.clock = clock;
        this.lockChannel = lockChannel;
        this.dirLock = dirLock;
    }

    public static Log open(Path dir, LogConfig config) {
        return open(dir, config, Clock.systemUTC());
    }

    /** Opens the log in {@code dir}, creating it if needed. The clock is replaceable for tests. */
    public static Log open(Path dir, LogConfig config, Clock clock) {
        try {
            Files.createDirectories(dir);
            FileChannel lockChannel = FileChannel.open(dir.resolve(LOCK_FILE),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock dirLock;
            try {
                dirLock = lockChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                dirLock = null;
            }
            if (dirLock == null) {
                lockChannel.close();
                throw new IllegalStateException("log directory " + dir + " is already open");
            }
            Log log = new Log(dir, config, clock, lockChannel, dirLock);
            try {
                log.load();
            } catch (IOException | RuntimeException e) {
                log.closeSegmentsQuietly();
                log.releaseDirLock();
                throw e;
            }
            return log;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open log in " + dir, e);
        }
    }

    private void load() throws IOException {
        // The marker exists only if the last close() finished. If it is missing, the
        // process crashed or was killed, so the newest segment must be checked.
        boolean cleanShutdown = Files.deleteIfExists(dir.resolve(CLEAN_SHUTDOWN_FILE));

        List<Long> baseOffsets = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + Segment.LOG_SUFFIX)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                baseOffsets.add(Long.parseLong(name.substring(0, name.length() - Segment.LOG_SUFFIX.length())));
            }
        }
        baseOffsets.sort(null);
        deleteOrphanIndexes(baseOffsets);

        if (baseOffsets.isEmpty()) {
            active = Segment.createActive(dir, 0, config);
            segments.put(0L, active);
            return;
        }

        Segment previous = null;
        for (int i = 0; i < baseOffsets.size(); i++) {
            boolean newest = i == baseOffsets.size() - 1;
            long base = baseOffsets.get(i);
            Segment segment;
            if (newest && !cleanShutdown) {
                // After a crash, never trust the newest index: rebuild it from the log file.
                segment = Segment.open(dir, base, config, true, true);
                recoveredBytesDropped = segment.lastRecoveryDroppedBytes();
            } else {
                segment = Segment.open(dir, base, config, newest, false);
            }
            segments.put(base, segment);
            if (previous != null && previous.nextOffset() != segment.baseOffset()) {
                throw new CorruptRecordException("gap in log " + dir + ": segment ending at offset "
                        + previous.nextOffset() + " is followed by one starting at " + base);
            }
            previous = segment;
        }
        active = previous;
    }

    private void deleteOrphanIndexes(List<Long> baseOffsets) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + Segment.INDEX_SUFFIX)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                long base = Long.parseLong(name.substring(0, name.length() - Segment.INDEX_SUFFIX.length()));
                if (!baseOffsets.contains(base)) {
                    Files.deleteIfExists(file);
                }
            }
        }
    }

    /** Appends one record stamped with the current time. Returns its offset. */
    public long append(byte[] key, byte[] value) {
        return append(new NewRecord(clock.millis(), key, value));
    }

    /** Appends one record. Returns its offset. */
    public long append(NewRecord record) {
        return appendAll(List.of(record));
    }

    /**
     * Appends a batch as one write. Records get consecutive offsets.
     * Either the whole batch is accepted or, on a validation error, none of it.
     *
     * @return the offset of the first record in the batch
     */
    public long appendAll(List<NewRecord> batch) {
        if (batch.isEmpty()) {
            throw new IllegalArgumentException("batch must not be empty");
        }
        long totalBytes = 0;
        for (NewRecord record : batch) {
            int size = record.frameSize();
            if (size > config.maxRecordBytes()) {
                throw new IllegalArgumentException("record of " + size
                        + " bytes is larger than maxRecordBytes (" + config.maxRecordBytes() + ")");
            }
            totalBytes += size;
        }
        if (totalBytes > config.segmentBytes() || batch.size() > config.maxRecordsPerSegment()) {
            throw new IllegalArgumentException("batch of " + batch.size() + " records / " + totalBytes
                    + " bytes does not fit in one segment; send smaller batches");
        }

        lock.writeLock().lock();
        try {
            ensureOpen();
            if (!active.canFit(totalBytes, batch.size()) && active.recordCount() > 0) {
                roll();
            }
            return active.append(batch);
        } catch (IOException e) {
            throw new UncheckedIOException("append failed in " + dir, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Seals the active segment and starts a new one at the next offset. */
    private void roll() throws IOException {
        long base = active.nextOffset();
        active.makeReadOnly();
        Segment next = Segment.createActive(dir, base, config);
        segments.put(base, next);
        active = next;
    }

    /** Reads one record. */
    public Record read(long offset) {
        lock.readLock().lock();
        try {
            ensureOpen();
            checkInRange(offset);
            return segments.floorEntry(offset).getValue().read(offset);
        } catch (IOException e) {
            throw new UncheckedIOException("read failed in " + dir, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Reads up to {@code maxRecords} records starting at {@code fromOffset}, crossing
     * segment boundaries as needed. Asking for {@link #endOffset()} returns an empty
     * list: the reader is caught up and should try again later.
     */
    public List<Record> read(long fromOffset, int maxRecords) {
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive");
        }
        lock.readLock().lock();
        try {
            ensureOpen();
            long end = active.nextOffset();
            if (fromOffset == end) {
                return List.of();
            }
            checkInRange(fromOffset);
            List<Record> result = new ArrayList<>(Math.min(maxRecords, 1024));
            long offset = fromOffset;
            for (Segment segment : segments.tailMap(segments.floorKey(fromOffset), true).values()) {
                while (offset < segment.nextOffset() && result.size() < maxRecords) {
                    result.add(segment.read(offset++));
                }
                if (result.size() == maxRecords) {
                    break;
                }
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("read failed in " + dir, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void checkInRange(long offset) {
        long start = segments.firstKey();
        long end = active.nextOffset();
        if (offset < start || offset >= end) {
            throw new OffsetOutOfRangeException(offset, start, end);
        }
    }

    /** The oldest offset still stored. Equals {@link #endOffset()} when the log is empty. */
    public long startOffset() {
        lock.readLock().lock();
        try {
            return segments.firstKey();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The offset the next appended record will get (one past the newest record). */
    public long endOffset() {
        lock.readLock().lock();
        try {
            return active.nextOffset();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long total = 0;
            for (Segment segment : segments.values()) {
                total += segment.sizeInBytes();
            }
            return total;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int segmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Bytes dropped from a half-written tail when this log was opened after a crash. */
    public long recoveredBytesDropped() {
        return recoveredBytesDropped;
    }

    public Path directory() {
        return dir;
    }

    public LogConfig config() {
        return config;
    }

    /** Forces everything written so far onto the physical disk. */
    public void flush() {
        lock.writeLock().lock();
        try {
            ensureOpen();
            active.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("flush failed in " + dir, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deletes old segments according to {@link LogConfig#retentionMs()} and
     * {@link LogConfig#retentionBytes()}. Always deletes oldest first, never the active
     * segment, and only whole segments. A broker calls this on a timer.
     *
     * @return how many segments were deleted
     */
    public int applyRetention() {
        lock.writeLock().lock();
        try {
            ensureOpen();
            int deleted = 0;
            if (config.retentionMs() >= 0) {
                long cutoff = clock.millis() - config.retentionMs();
                while (segments.size() > 1) {
                    Segment oldest = segments.firstEntry().getValue();
                    if (oldest.isActive() || oldest.maxTimestamp() >= cutoff) {
                        break;
                    }
                    deleteOldest();
                    deleted++;
                }
            }
            if (config.retentionBytes() >= 0) {
                long total = 0;
                for (Segment segment : segments.values()) {
                    total += segment.sizeInBytes();
                }
                while (segments.size() > 1 && total > config.retentionBytes()) {
                    Segment oldest = segments.firstEntry().getValue();
                    if (oldest.isActive()) {
                        break;
                    }
                    total -= oldest.sizeInBytes();
                    deleteOldest();
                    deleted++;
                }
            }
            return deleted;
        } catch (IOException e) {
            throw new UncheckedIOException("retention failed in " + dir, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteOldest() throws IOException {
        Map.Entry<Long, Segment> oldest = segments.pollFirstEntry();
        oldest.getValue().delete();
    }

    /**
     * Flushes and closes every segment, then writes the clean-shutdown marker so the
     * next open can skip crash recovery.
     */
    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            for (Segment segment : segments.values()) {
                segment.close();
            }
            Files.write(dir.resolve(CLEAN_SHUTDOWN_FILE), new byte[0]);
        } catch (IOException e) {
            throw new UncheckedIOException("close failed in " + dir, e);
        } finally {
            releaseDirLock();
            lock.writeLock().unlock();
        }
    }

    private void closeSegmentsQuietly() {
        for (Segment segment : segments.values()) {
            try {
                segment.close();
            } catch (IOException | RuntimeException ignored) {
                // already failing; keep the original exception
            }
        }
    }

    private void releaseDirLock() {
        try {
            if (dirLock.isValid()) {
                dirLock.release();
            }
            lockChannel.close();
        } catch (IOException ignored) {
            // nothing useful to do; the OS releases the lock when the process exits
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("log " + dir + " is closed");
        }
    }
}
