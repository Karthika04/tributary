package io.github.karthika04.tributary.broker;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A broker-wide signal that new records were appended somewhere. A multi-partition
 * fetch waits on it, then checks its own partitions again; that way one waiting
 * request covers any number of partitions.
 *
 * <p>The version counter prevents lost wake-ups: a fetch reads the version
 * <em>before</em> checking its partitions, and only waits while the version is still
 * the same, so an append that lands between the check and the wait is never missed.
 *
 * <p>Every append wakes every waiting multi-fetch, which then re-checks its partitions.
 * That costs little at this scale; a broker with thousands of idle consumers would want
 * per-partition waiter lists instead.
 */
final class DataArrivals {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition arrived = lock.newCondition();
    private long version;
    private boolean stopping;

    long version() {
        lock.lock();
        try {
            return version;
        } finally {
            lock.unlock();
        }
    }

    void signal() {
        lock.lock();
        try {
            version++;
            arrived.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until the version moves past {@code seen}, the time runs out, or the broker
     * starts shutting down.
     *
     * @return the nanoseconds left to wait (0 or less if the time ran out)
     */
    long awaitChange(long seen, long nanos) throws InterruptedException {
        lock.lock();
        try {
            while (!stopping && version == seen && nanos > 0) {
                nanos = arrived.awaitNanos(nanos);
            }
            return stopping ? 0 : nanos;
        } finally {
            lock.unlock();
        }
    }

    /** Wakes every waiting fetch so shutdown does not have to wait for their timeouts. */
    void stop() {
        lock.lock();
        try {
            stopping = true;
            arrived.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
