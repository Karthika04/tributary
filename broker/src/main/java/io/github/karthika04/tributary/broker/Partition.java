package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.storage.Log;
import io.github.karthika04.tributary.storage.NewRecord;
import io.github.karthika04.tributary.storage.Record;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One topic partition on this broker: its {@link Log} plus the machinery for
 * long-polling fetches.
 *
 * <p>Long polling: a consumer that is caught up asks "anything after offset 500? I'll
 * wait up to 1 second." Instead of answering "no" and making it ask again and again,
 * the broker parks the request and wakes it the moment a producer appends. Consumers
 * see new messages within milliseconds, without hammering the broker.
 */
final class Partition implements Closeable {

    private final TopicPartition id;
    private final Log log;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition newData = lock.newCondition();
    private volatile boolean stopping;

    Partition(TopicPartition id, Log log) {
        this.id = id;
        this.log = log;
    }

    TopicPartition id() {
        return id;
    }

    long append(List<NewRecord> records) {
        long baseOffset = log.appendAll(records);
        lock.lock();
        try {
            newData.signalAll();
        } finally {
            lock.unlock();
        }
        return baseOffset;
    }

    /**
     * Reads from {@code offset}. If the reader is caught up ({@code offset} equals the
     * end offset) and {@code maxWaitMs > 0}, waits until a producer appends, the time
     * runs out, or the broker starts shutting down.
     */
    List<Record> read(long offset, int maxRecords, long maxWaitMs) throws InterruptedException {
        if (maxWaitMs > 0 && offset == log.endOffset()) {
            long remaining = TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
            lock.lock();
            try {
                // Checked under the lock, and append() signals under the same lock, so
                // a record appended between the check and the wait cannot be missed.
                while (!stopping && log.endOffset() <= offset && remaining > 0) {
                    remaining = newData.awaitNanos(remaining);
                }
            } finally {
                lock.unlock();
            }
        }
        return log.read(offset, maxRecords);
    }

    long startOffset() {
        return log.startOffset();
    }

    long endOffset() {
        return log.endOffset();
    }

    int applyRetention() {
        return log.applyRetention();
    }

    /** Wakes every waiting fetch so shutdown does not have to wait for their timeouts. */
    void stopWaiting() {
        stopping = true;
        lock.lock();
        try {
            newData.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        stopWaiting();
        log.close();
    }
}
