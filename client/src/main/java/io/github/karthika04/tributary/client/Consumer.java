package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads records from topic partitions.
 *
 * <pre>{@code
 * try (Consumer consumer = new Consumer(ConsumerConfig.of(ClientConfig.of("localhost", 9292)))) {
 *     consumer.subscribe("orders");
 *     while (running) {
 *         for (ConsumerRecord r : consumer.poll(Duration.ofSeconds(1))) {
 *             System.out.println(r.offset() + ": " + r.valueAsString());
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>The consumer keeps a <em>position</em> per partition: the next offset to read.
 * Positions live in memory only for now; consumer groups that save positions on the
 * broker come in the next phase.
 *
 * <p>Not thread-safe: use one consumer per thread.
 */
public final class Consumer implements AutoCloseable {

    private final ConsumerConfig config;
    private final TributaryClient client;
    private final Set<TopicPartition> assignment = new LinkedHashSet<>();
    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private int nextPartitionIndex;

    public Consumer(ConsumerConfig config) {
        this.config = config;
        this.client = new TributaryClient(config.client());
    }

    /** Adds every partition of {@code topic} to this consumer's assignment. */
    public void subscribe(String topic) {
        int count = client.partitionCount(topic);
        for (int p = 0; p < count; p++) {
            assignment.add(new TopicPartition(topic, p));
        }
    }

    /** Replaces the assignment with exactly these partitions. */
    public void assign(Collection<TopicPartition> partitions) {
        assignment.clear();
        assignment.addAll(partitions);
        positions.keySet().retainAll(assignment);
    }

    public Set<TopicPartition> assignment() {
        return Set.copyOf(assignment);
    }

    /** The next offset this consumer will read from the partition. */
    public long position(TopicPartition partition) {
        checkAssigned(partition);
        return positions.computeIfAbsent(partition, this::resetPosition);
    }

    public void seek(TopicPartition partition, long offset) {
        checkAssigned(partition);
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        positions.put(partition, offset);
    }

    public void seekToBeginning(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            checkAssigned(tp);
            positions.put(tp, client.listOffsets(tp.topic(), tp.partition()).startOffset());
        }
    }

    public void seekToEnd(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            checkAssigned(tp);
            positions.put(tp, client.listOffsets(tp.topic(), tp.partition()).endOffset());
        }
    }

    /**
     * Returns the next records, waiting up to {@code timeout} if none are available.
     * Returns an empty list if nothing arrives in time.
     *
     * <p>First it asks every assigned partition for data without waiting, taking turns so
     * a busy partition cannot starve the others. If all are caught up, it long-polls one
     * partition (rotating each time) until data arrives or the time runs out.
     */
    public List<ConsumerRecord> poll(Duration timeout) {
        if (assignment.isEmpty()) {
            throw new IllegalStateException("subscribe to a topic or assign partitions before polling");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        List<TopicPartition> partitions = new ArrayList<>(assignment);
        while (true) {
            List<ConsumerRecord> records = new ArrayList<>();
            for (int i = 0; i < partitions.size() && records.size() < config.maxPollRecords(); i++) {
                TopicPartition tp = partitions.get((nextPartitionIndex + i) % partitions.size());
                fetchInto(tp, 0, records);
            }
            nextPartitionIndex = (nextPartitionIndex + 1) % partitions.size();
            if (!records.isEmpty()) {
                return records;
            }
            long remainingMs = Duration.ofNanos(deadline - System.nanoTime()).toMillis();
            if (remainingMs <= 0) {
                return records;
            }
            TopicPartition waitOn = partitions.get(nextPartitionIndex % partitions.size());
            fetchInto(waitOn, (int) Math.min(remainingMs, config.fetchMaxWaitMs()), records);
            if (!records.isEmpty()) {
                return records;
            }
        }
    }

    private void fetchInto(TopicPartition tp, int maxWaitMs, List<ConsumerRecord> out) {
        long position = position(tp);
        int room = config.maxPollRecords() - out.size();
        Response.Fetch response;
        try {
            response = client.fetch(tp.topic(), tp.partition(), position, room, maxWaitMs,
                    config.fetchCompression());
        } catch (TributaryException e) {
            if (e.error() == ErrorCode.OFFSET_OUT_OF_RANGE) {
                // Our position was deleted by retention (or is past the end): start over
                // according to the reset policy.
                positions.put(tp, resetPosition(tp));
                return;
            }
            throw e;
        }
        for (FetchedRecord r : response.records()) {
            out.add(new ConsumerRecord(tp.topic(), tp.partition(), r.offset(), r.timestamp(), r.key(), r.value()));
            position = r.offset() + 1;
        }
        positions.put(tp, position);
    }

    private long resetPosition(TopicPartition tp) {
        Response.ListOffsets offsets = client.listOffsets(tp.topic(), tp.partition());
        return config.autoOffsetReset() == ConsumerConfig.AutoOffsetReset.EARLIEST
                ? offsets.startOffset()
                : offsets.endOffset();
    }

    private void checkAssigned(TopicPartition tp) {
        if (!assignment.contains(tp)) {
            throw new IllegalStateException(tp + " is not assigned to this consumer");
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
