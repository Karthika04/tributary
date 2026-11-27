package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.Partitioning;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sends records to topics, batching them for throughput.
 *
 * <pre>{@code
 * try (Producer producer = new Producer(ProducerConfig.of(ClientConfig.of("localhost", 9292)))) {
 *     producer.send("orders", "user-42".getBytes(), "clicked checkout".getBytes())
 *             .thenAccept(meta -> System.out.println("stored at offset " + meta.offset()));
 * }
 * }</pre>
 *
 * <p>How it works: {@link #send} only adds the record to an in-memory batch for its
 * partition and returns a future right away. Each partition has a queue of batches:
 * when the newest is full ({@code batchBytes}), a new one is started, so no request
 * grows past that size however fast records arrive. A background "sender" thread ships
 * a batch when it is full or has waited {@code lingerMs}, then completes each record's
 * future with its offset. If more than {@code bufferBytes} are waiting, {@code send}
 * blocks until the sender catches up (backpressure), for at most {@code maxBlockMs}.
 *
 * <p>Partitioning: records with a key always go to the same partition (so they stay
 * in order). Records without a key stick to one partition until its batch is sent,
 * then move to the next, which keeps batches large while still spreading load.
 *
 * <p>Delivery: at-least-once. A batch that hits a retriable error (such as a dropped
 * connection) is retried. If the broker had stored it before the connection dropped,
 * the retry stores it again, so rare duplicates are possible; nothing acknowledged is
 * lost. Order within a partition is kept, because batches are sent one at a time and
 * a batch is retried before the next one is sent.
 *
 * <p>Thread-safe: many threads may call {@link #send} at once.
 */
public final class Producer implements AutoCloseable {

    /** Rough per-record overhead used when sizing batches. */
    private static final int RECORD_OVERHEAD_BYTES = 8;
    private static final long MAX_BACKOFF_MS = 5_000;

    private final ProducerConfig config;
    private final TributaryClient client;
    private final Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> stickyPartitions = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workAvailable = lock.newCondition();
    private final Condition idle = lock.newCondition();
    private final Condition spaceAvailable = lock.newCondition();
    /** Batches waiting to be sent, per partition, oldest first. Guarded by lock. */
    private final Map<TopicPartition, ArrayDeque<Batch>> queues = new LinkedHashMap<>();
    /** Bytes of records queued or being sent; send() blocks above bufferBytes. Guarded by lock. */
    private long bufferedBytes;
    private int inFlight;
    private int flushRequests;
    private boolean closing;
    private final Thread sender;

    public Producer(ProducerConfig config) {
        this.config = config;
        this.client = new TributaryClient(config.client());
        this.sender = Thread.ofPlatform().name("tributary-producer-sender").daemon(true).start(this::senderLoop);
    }

    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        return send(topic, -1, key, value);
    }

    /**
     * Queues a record for sending.
     *
     * @param partition a specific partition, or -1 to choose one from the key
     * @return completes with where the record was stored, or exceptionally with a
     *         {@link TributaryException} if it could not be sent
     */
    public CompletableFuture<RecordMetadata> send(String topic, int partition, byte[] key, byte[] value) {
        ProducedRecord record = new ProducedRecord(key, value);
        int target;
        try {
            target = partition >= 0 ? partition : choosePartition(topic, key);
        } catch (TributaryException e) {
            return CompletableFuture.failedFuture(e); // e.g. the topic does not exist
        }
        TopicPartition tp = new TopicPartition(topic, target);
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        int size = RECORD_OVERHEAD_BYTES + (key == null ? 0 : key.length) + value.length;

        lock.lock();
        try {
            if (closing) {
                throw new IllegalStateException("producer is closed");
            }
            if (!awaitBufferSpace(size)) {
                future.completeExceptionally(new TributaryException(ErrorCode.PRODUCER_BUFFER_FULL,
                        "the producer's buffer of " + config.bufferBytes() + " bytes stayed full for "
                                + config.maxBlockMs() + " ms: records are produced faster than they can be sent"));
                return future;
            }
            ArrayDeque<Batch> queue = queues.computeIfAbsent(tp, k -> new ArrayDeque<>());
            Batch batch = queue.peekLast();
            boolean newBatch = batch == null || batch.bytes + size > config.batchBytes();
            if (newBatch) {
                batch = new Batch(tp);
                queue.addLast(batch);
            }
            batch.add(record, future, size);
            bufferedBytes += size;
            if (newBatch || batch.bytes >= config.batchBytes()) {
                workAvailable.signal(); // a batch is full, or a new one's linger timer must start
            }
        } finally {
            lock.unlock();
        }
        return future;
    }

    /**
     * Waits (holding the lock, released while waiting) until {@code size} more bytes fit
     * in the buffer. A record larger than the whole buffer is let through once the buffer
     * is empty, so it can still be sent.
     *
     * @return false if the wait timed out
     */
    private boolean awaitBufferSpace(int size) {
        long remaining = TimeUnit.MILLISECONDS.toNanos(config.maxBlockMs());
        while (bufferedBytes > 0 && bufferedBytes + size > config.bufferBytes()) {
            if (remaining <= 0 || closing) {
                return false;
            }
            try {
                remaining = spaceAvailable.awaitNanos(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    private int choosePartition(String topic, byte[] key) {
        int count = partitionCounts.computeIfAbsent(topic, client::partitionCount);
        if (key != null) {
            return Partitioning.partitionForKey(key, count);
        }
        return stickyPartitions.computeIfAbsent(topic, t -> ThreadLocalRandom.current().nextInt(count)) % count;
    }

    /** Sends everything queued so far and waits until each record has a result. */
    public void flush() {
        lock.lock();
        try {
            flushRequests++;
            workAvailable.signal();
            while (!queues.isEmpty() || inFlight > 0) {
                idle.awaitUninterruptibly();
            }
            flushRequests--;
        } finally {
            lock.unlock();
        }
    }

    /** Sends what is queued, then stops. Records sent after close are rejected. */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closing) {
                return;
            }
            closing = true;
            workAvailable.signal();
            spaceAvailable.signalAll(); // a send() waiting for buffer space gives up
        } finally {
            lock.unlock();
        }
        try {
            sender.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        client.close();
    }

    // ---- the sender thread -------------------------------------------------------

    private void senderLoop() {
        while (true) {
            List<Batch> ready;
            lock.lock();
            try {
                ready = awaitReadyBatches();
                if (ready == null) {
                    return; // closing and nothing left to send
                }
                inFlight += ready.size();
            } finally {
                lock.unlock();
            }
            for (Batch batch : ready) {
                try {
                    sendWithRetries(batch);
                } catch (RuntimeException e) {
                    batch.fail(new TributaryException(ErrorCode.INTERNAL_ERROR, "send failed: " + e, e));
                }
                lock.lock();
                try {
                    inFlight--;
                    bufferedBytes -= batch.bytes;
                    spaceAvailable.signalAll();
                    idle.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /** Waits (holding the lock) until some batches are ready, and removes them. */
    private List<Batch> awaitReadyBatches() {
        while (true) {
            boolean sendAll = closing || flushRequests > 0;
            long now = System.nanoTime();
            long lingerNanos = TimeUnit.MILLISECONDS.toNanos(config.lingerMs());
            long nextDeadline = Long.MAX_VALUE;
            List<Batch> ready = new ArrayList<>();
            Iterator<ArrayDeque<Batch>> it = queues.values().iterator();
            while (it.hasNext()) {
                ArrayDeque<Batch> queue = it.next();
                Batch batch = queue.peekFirst(); // one batch per partition per round keeps them in order
                long age = now - batch.createdNanos;
                boolean full = queue.size() > 1 || batch.bytes >= config.batchBytes();
                if (sendAll || full || age >= lingerNanos) {
                    ready.add(queue.pollFirst());
                    if (queue.isEmpty()) {
                        it.remove();
                    }
                    rotateSticky(batch.partition);
                } else {
                    nextDeadline = Math.min(nextDeadline, lingerNanos - age);
                }
            }
            if (!ready.isEmpty()) {
                return ready;
            }
            if (closing) {
                return null;
            }
            idle.signalAll(); // nothing queued: wake any flush() that is waiting
            try {
                if (nextDeadline == Long.MAX_VALUE) {
                    workAvailable.await();
                } else {
                    workAvailable.awaitNanos(nextDeadline);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closing = true;
            }
        }
    }

    /** After a keyless batch is sent, move keyless records on to the next partition. */
    private void rotateSticky(TopicPartition tp) {
        Integer count = partitionCounts.get(tp.topic());
        if (count != null) {
            stickyPartitions.computeIfPresent(tp.topic(),
                    (t, current) -> current == tp.partition() ? (current + 1) % count : current);
        }
    }

    private void sendWithRetries(Batch batch) {
        int attempt = 0;
        while (true) {
            try {
                long baseOffset = client.produce(batch.partition.topic(), batch.partition.partition(),
                        batch.records, config.compression());
                batch.complete(baseOffset);
                return;
            } catch (TributaryException e) {
                if (!e.retriable() || attempt >= config.maxRetries()) {
                    batch.fail(e);
                    return;
                }
                long backoff = Math.min(MAX_BACKOFF_MS, config.retryBackoffMs() << Math.min(attempt, 20));
                attempt++;
                try {
                    Thread.sleep(Duration.ofMillis(backoff));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    batch.fail(e);
                    return;
                }
            }
        }
    }

    private static final class Batch {
        final TopicPartition partition;
        final List<ProducedRecord> records = new ArrayList<>();
        final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        final long createdNanos = System.nanoTime();
        int bytes;

        Batch(TopicPartition partition) {
            this.partition = partition;
        }

        void add(ProducedRecord record, CompletableFuture<RecordMetadata> future, int size) {
            records.add(record);
            futures.add(future);
            bytes += size;
        }

        void complete(long baseOffset) {
            for (int i = 0; i < futures.size(); i++) {
                futures.get(i).complete(new RecordMetadata(partition.topic(), partition.partition(), baseOffset + i));
            }
        }

        void fail(Throwable error) {
            futures.forEach(f -> f.completeExceptionally(error));
        }
    }
}
