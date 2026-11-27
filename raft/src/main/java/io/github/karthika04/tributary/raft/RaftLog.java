package io.github.karthika04.tributary.raft;

import io.github.karthika04.tributary.storage.Log;
import io.github.karthika04.tributary.storage.LogConfig;
import io.github.karthika04.tributary.storage.NewRecord;
import io.github.karthika04.tributary.storage.Record;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The replicated log, stored in Tributary's own storage engine. Raft numbers entries
 * from 1; storage offsets start at 0, so entry N is stored at offset N - 1. Each
 * record's value is {@code [term: 8 bytes][command]}.
 *
 * <p>Appends are not forced to disk on their own: call {@link #flush()}. That lets
 * {@link RaftNode} force a whole batch of entries with one {@code fsync}, and do it
 * without holding its lock (group commit). A node still never reports an entry as
 * stored, to itself or to a leader, before it has been flushed.
 *
 * <p>The term of every entry is also kept in memory, because Raft asks "what is the
 * term at index N?" constantly.
 */
final class RaftLog implements Closeable {

    private static final int MAX_COMMAND_BYTES = 17 * 1024 * 1024;

    private final Log log;
    private long[] terms = new long[1024];
    private int size;

    RaftLog(Path dir) {
        this.log = Log.open(dir, LogConfig.defaults()
                .withMaxRecordBytes(MAX_COMMAND_BYTES + 64)
                .withSegmentBytes(Math.max(LogConfig.DEFAULT_SEGMENT_BYTES, MAX_COMMAND_BYTES + 64))
                .withFlushEveryAppend(false));
        long offset = 0;
        while (offset < log.endOffset()) {
            for (Record r : log.read(offset, 10_000)) {
                addTerm(ByteBuffer.wrap(r.value()).getLong());
                offset = r.offset() + 1;
            }
        }
    }

    long lastIndex() {
        return size;
    }

    long lastTerm() {
        return size == 0 ? 0 : terms[size - 1];
    }

    /** Term of the entry at {@code index}; 0 for index 0 (the empty start of the log). */
    long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("no entry " + index + "; last is " + size);
        }
        return terms[(int) index - 1];
    }

    /** The first index that has the same term as {@code index}. */
    long firstIndexOfTerm(long index) {
        long term = termAt(index);
        long i = index;
        while (i > 1 && terms[(int) i - 2] == term) {
            i--;
        }
        return i;
    }

    /** Appends entries; they are durable only after {@link #flush()}. @return the index of the last one */
    long append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return size;
        }
        List<NewRecord> records = new ArrayList<>(entries.size());
        for (LogEntry e : entries) {
            if (e.command().length > MAX_COMMAND_BYTES) {
                throw new IllegalArgumentException("command of " + e.command().length + " bytes is too large");
            }
            ByteBuffer value = ByteBuffer.allocate(8 + e.command().length);
            value.putLong(e.term()).put(e.command());
            records.add(new NewRecord(0, null, value.array()));
        }
        log.appendAll(records);
        for (LogEntry e : entries) {
            addTerm(e.term());
        }
        return size;
    }

    LogEntry entry(long index) {
        return toEntry(log.read(index - 1));
    }

    /**
     * Entries starting at {@code from}: at most {@code maxCount}, and stopping before
     * {@code maxBytes} of commands, except that the first entry is always included.
     */
    List<LogEntry> entries(long from, int maxCount, int maxBytes) {
        if (from > size) {
            return List.of();
        }
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;
        for (Record r : log.read(from - 1, maxCount)) {
            LogEntry entry = toEntry(r);
            bytes += entry.command().length;
            if (!result.isEmpty() && bytes > maxBytes) {
                break;
            }
            result.add(entry);
        }
        return result;
    }

    /**
     * Forces every appended entry to disk. Safe to call without the node's lock: the
     * storage log has its own.
     */
    void flush() {
        log.flush();
    }

    /** Deletes the entry at {@code index} and everything after it. */
    void truncateFrom(long index) {
        log.truncateFrom(index - 1);
        size = (int) Math.min(size, index - 1);
    }

    private static LogEntry toEntry(Record r) {
        ByteBuffer value = ByteBuffer.wrap(r.value());
        long term = value.getLong();
        return new LogEntry(term, Arrays.copyOfRange(r.value(), 8, r.value().length));
    }

    private void addTerm(long term) {
        if (size == terms.length) {
            terms = Arrays.copyOf(terms, size * 2);
        }
        terms[size++] = term;
    }

    @Override
    public void close() {
        log.close();
    }
}
