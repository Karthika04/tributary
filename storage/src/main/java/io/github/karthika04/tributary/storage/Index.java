package io.github.karthika04.tributary.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * The ".index" file of a segment: for each record, where its frame starts in the
 * ".log" file. Without it, finding offset N would mean reading every frame before it.
 *
 * <p>Each entry is 8 bytes: {@code [relativeOffset: int][position: int]}.
 * "Relative" means offset minus the segment's base offset, so it fits in an int.
 *
 * <p>This is a <em>dense</em> index: one entry per record, in order. Entry N is always
 * for relative offset N, so a lookup is a single multiplication, O(1). (Kafka uses a
 * <em>sparse</em> index with an entry every few KB plus a binary search and short
 * scan, trading a little lookup speed for a smaller file.)
 *
 * <p>The file is memory-mapped: the operating system makes the file look like an
 * array in memory, so reading an entry is a memory read with no system call.
 *
 * <p>While a segment is active the file is pre-sized to its full capacity so it can be
 * mapped once. When the segment is closed, the file is truncated to the entries
 * actually written, which is how a cleanly closed index knows its own entry count.
 */
final class Index implements Closeable {

    static final int ENTRY_SIZE = 8;

    private final Path path;
    private final RandomAccessFile file;
    private final boolean writable;
    private MappedByteBuffer mmap;
    private final int capacityEntries;
    private volatile int entries;

    private Index(Path path, RandomAccessFile file, MappedByteBuffer mmap,
                  boolean writable, int capacityEntries, int entries) {
        this.path = path;
        this.file = file;
        this.mmap = mmap;
        this.writable = writable;
        this.capacityEntries = capacityEntries;
        this.entries = entries;
    }

    /**
     * Opens (or creates) an index that new entries can be added to.
     * Existing entries are kept: the count is taken from the file size, which is exact
     * for a cleanly closed index. After a crash, call {@link #reset()} and rebuild.
     */
    static Index openForAppend(Path path, int maxIndexBytes) throws IOException {
        int capacityEntries = maxIndexBytes / ENTRY_SIZE;
        long capacityBytes = (long) capacityEntries * ENTRY_SIZE;
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
        try {
            long existingBytes = file.length();
            int existing = (int) Math.min(existingBytes / ENTRY_SIZE, capacityEntries);
            file.setLength(capacityBytes);
            MappedByteBuffer mmap = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacityBytes);
            return new Index(path, file, mmap, true, capacityEntries, existing);
        } catch (IOException | RuntimeException e) {
            file.close();
            throw e;
        }
    }

    /** Opens a cleanly closed index of an old segment. No entries can be added. */
    static Index openReadOnly(Path path) throws IOException {
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
        try {
            long bytes = file.length();
            if (bytes % ENTRY_SIZE != 0) {
                throw new CorruptRecordException("index " + path + " has " + bytes
                        + " bytes, not a multiple of " + ENTRY_SIZE);
            }
            int count = (int) (bytes / ENTRY_SIZE);
            MappedByteBuffer mmap = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, bytes);
            return new Index(path, file, mmap, false, count, count);
        } catch (IOException | RuntimeException e) {
            file.close();
            throw e;
        }
    }

    void append(int relativeOffset, int position) {
        if (!writable) {
            throw new IllegalStateException("index " + path + " is read-only");
        }
        if (relativeOffset != entries) {
            throw new IllegalArgumentException("dense index expects relative offset " + entries
                    + " next, got " + relativeOffset);
        }
        if (isFull()) {
            throw new IllegalStateException("index " + path + " is full");
        }
        int at = entries * ENTRY_SIZE;
        mmap.putInt(at, relativeOffset);
        mmap.putInt(at + 4, position);
        entries++; // published last, so a reader never sees a half-written entry
    }

    /** File position of the record with this relative offset. */
    int positionOf(int relativeOffset) {
        if (relativeOffset < 0 || relativeOffset >= entries) {
            throw new IndexOutOfBoundsException("relative offset " + relativeOffset
                    + " not in index of " + entries + " entries");
        }
        int at = relativeOffset * ENTRY_SIZE;
        int stored = mmap.getInt(at);
        if (stored != relativeOffset) {
            throw new CorruptRecordException("index " + path + " entry " + relativeOffset
                    + " says relative offset " + stored);
        }
        return mmap.getInt(at + 4);
    }

    int entries() {
        return entries;
    }

    boolean isFull() {
        return entries >= capacityEntries;
    }

    /** Forgets all entries so they can be rebuilt from the log file. */
    void reset() {
        entries = 0;
    }

    /** Keeps only the first {@code count} entries. */
    void truncate(int count) {
        if (!writable) {
            throw new IllegalStateException("index " + path + " is read-only");
        }
        if (count < 0 || count > entries) {
            throw new IllegalArgumentException("cannot truncate " + entries + " entries to " + count);
        }
        entries = count;
    }

    void flush() {
        if (writable) {
            mmap.force();
        }
    }

    /** Flushes, then shrinks the file to exactly the entries written. */
    @Override
    public void close() throws IOException {
        if (mmap == null) {
            return;
        }
        if (writable) {
            mmap.force();
        }
        // Java 21 has no public way to unmap a MappedByteBuffer; we drop our reference
        // and never touch it again. Truncating a mapped file is safe on Linux and macOS.
        mmap = null;
        if (writable) {
            file.setLength((long) entries * ENTRY_SIZE);
        }
        file.close();
    }
}
