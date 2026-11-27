package io.github.karthika04.tributary.client;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.FetchPosition;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.PartitionRecords;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Reads records from topic partitions, on its own or as part of a consumer group.
 *
 * <pre>{@code
 * ConsumerConfig config = ConsumerConfig.of(ClientConfig.of("localhost", 9292)).withGroup("billing");
 * try (Consumer consumer = new Consumer(config)) {
 *     consumer.subscribe("orders");
 *     while (running) {
 *         for (ConsumerRecord r : consumer.poll(Duration.ofSeconds(1))) {
 *             process(r);
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p><b>Without a group</b> you choose partitions yourself ({@link #assign}, or
 * {@link #subscribe} for all of a topic's partitions), and positions are kept in memory.
 *
 * <p><b>With a group</b> ({@link ConsumerConfig#withGroup}), the broker divides the
 * partitions between all consumers sharing the group id, and moves them when consumers
 * join or leave. Progress is <em>committed</em> to the broker, so a restarted consumer
 * carries on where its group left off. Everything happens inside {@link #poll}:
 * joining, heartbeats, re-joining after a rebalance, and (by default) committing
 * positions every second. Call {@code poll} at least once per session timeout, or the
 * broker assumes this consumer died.
 *
 * <p>A committed offset means "everything before this was processed". Auto-commit
 * commits the position reached by previous polls, so a record is committed only after
 * the next {@code poll} call, meaning after you have processed it. If a consumer
 * crashes after processing but before committing, those records are delivered again:
 * at-least-once.
 *
 * <p>Not thread-safe: use one consumer per thread.
 */
public final class Consumer implements AutoCloseable {

    private final ConsumerConfig config;
    private final TributaryClient client;
    private final Set<TopicPartition> assignment = new LinkedHashSet<>();
    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private int nextPartitionIndex;

    // Group state (unused without a group).
    private final List<String> subscriptions = new ArrayList<>();
    private final Map<TopicPartition, Long> lastCommitted = new HashMap<>();
    private String memberId;
    private int generation;
    private boolean needsJoin = true;
    private long lastHeartbeatMs;
    private long lastAutoCommitMs;

    public Consumer(ConsumerConfig config) {
        this.config = config;
        this.client = new TributaryClient(config.client());
    }

    private boolean inGroup() {
        return config.groupId() != null;
    }

    // ---- choosing what to read ---------------------------------------------------

    /**
     * Reads {@code topic}. Without a group: assigns all its partitions to this consumer.
     * With a group: asks to share the topic with the group; the partitions arrive at the
     * next {@link #poll}.
     */
    public void subscribe(String topic) {
        int count = client.partitionCount(topic); // fails fast if the topic does not exist
        if (inGroup()) {
            if (!subscriptions.contains(topic)) {
                subscriptions.add(topic);
                needsJoin = true;
            }
            return;
        }
        for (int p = 0; p < count; p++) {
            assignment.add(new TopicPartition(topic, p));
        }
    }

    /** Replaces the assignment with exactly these partitions. Not for group members. */
    public void assign(Collection<TopicPartition> partitions) {
        if (inGroup()) {
            throw new IllegalStateException("a group member's partitions are chosen by the group; use subscribe()");
        }
        assignment.clear();
        assignment.addAll(partitions);
        positions.keySet().retainAll(assignment);
    }

    /** The partitions this consumer reads right now. For a group member this can change at every poll. */
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

    // ---- polling -----------------------------------------------------------------

    /** Most bytes of keys and values one fetch brings back, like Kafka's fetch.max.bytes. */
    static final int FETCH_MAX_BYTES = 4 * 1024 * 1024;

    /**
     * Returns the next records, waiting up to {@code timeout} if none are available.
     * Returns an empty list if nothing arrives in time.
     *
     * <p>One multi-partition fetch asks every assigned partition at once, and the broker
     * answers as soon as any of them has records. (An earlier version long-polled one
     * partition at a time, so a record arriving on another partition could wait out the
     * whole poll; the benchmarks showed it as ~200 ms latency spikes.) The starting
     * partition rotates on each fetch, so a busy partition cannot starve the others when
     * {@code maxPollRecords} is reached. Group members also heartbeat, re-join and
     * auto-commit here.
     */
    public List<ConsumerRecord> poll(Duration timeout) {
        if (inGroup() ? subscriptions.isEmpty() : assignment.isEmpty()) {
            throw new IllegalStateException("subscribe to a topic or assign partitions before polling");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (inGroup()) {
                maintainGroupMembership();
                maybeAutoCommit();
            }
            long remainingMs = Math.max(0, Duration.ofNanos(deadline - System.nanoTime()).toMillis());
            List<TopicPartition> partitions = new ArrayList<>(assignment);
            if (partitions.isEmpty()) {
                // A group can have more members than partitions; idle members just keep heartbeating.
                if (remainingMs <= 0) {
                    return List.of();
                }
                sleep(Math.min(remainingMs, config.heartbeatIntervalMs()));
                continue;
            }

            long wait = Math.min(remainingMs, config.fetchMaxWaitMs());
            if (inGroup()) {
                wait = Math.min(wait, config.heartbeatIntervalMs()); // wake up in time to heartbeat
            }
            List<ConsumerRecord> records = fetch(partitions, (int) wait);
            if (!records.isEmpty() || System.nanoTime() >= deadline) {
                return records;
            }
        }
    }

    private List<ConsumerRecord> fetch(List<TopicPartition> partitions, int maxWaitMs) {
        List<FetchPosition> wanted = new ArrayList<>(partitions.size());
        for (int i = 0; i < partitions.size(); i++) {
            TopicPartition tp = partitions.get((nextPartitionIndex + i) % partitions.size());
            wanted.add(new FetchPosition(tp.topic(), tp.partition(), position(tp)));
        }
        nextPartitionIndex = (nextPartitionIndex + 1) % partitions.size();

        Response.MultiFetch response = client.multiFetch(wanted, config.maxPollRecords(), FETCH_MAX_BYTES, maxWaitMs,
                config.fetchCompression());
        List<ConsumerRecord> out = new ArrayList<>();
        for (PartitionRecords p : response.partitions()) {
            TopicPartition tp = new TopicPartition(p.topic(), p.partition());
            if (!assignment.contains(tp)) {
                continue; // reassigned while the fetch was out
            }
            if (p.error() == ErrorCode.OFFSET_OUT_OF_RANGE) {
                // Our position was deleted by retention (or is past the end): start over
                // according to the reset policy.
                positions.put(tp, resetPosition(tp));
                continue;
            }
            if (p.error() != ErrorCode.NONE) {
                throw new TributaryException(p.error(), "cannot fetch " + tp + ": " + p.error());
            }
            long position = position(tp);
            for (FetchedRecord r : p.records()) {
                out.add(new ConsumerRecord(tp.topic(), tp.partition(), r.offset(), r.timestamp(), r.key(), r.value()));
                position = r.offset() + 1;
            }
            positions.put(tp, position);
        }
        return out;
    }

    private long resetPosition(TopicPartition tp) {
        Response.ListOffsets offsets = client.listOffsets(tp.topic(), tp.partition());
        return config.autoOffsetReset() == ConsumerConfig.AutoOffsetReset.EARLIEST
                ? offsets.startOffset()
                : offsets.endOffset();
    }

    // ---- group membership --------------------------------------------------------

    private void maintainGroupMembership() {
        if (needsJoin || memberId == null) {
            joinGroup();
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastHeartbeatMs < config.heartbeatIntervalMs()) {
            return;
        }
        try {
            client.heartbeat(config.groupId(), memberId, generation);
            lastHeartbeatMs = now;
        } catch (TributaryException e) {
            if (e.error() == ErrorCode.REBALANCE_IN_PROGRESS) {
                // Save progress on partitions we may be about to lose, then get the new assignment.
                if (config.autoCommit()) {
                    commitQuietly();
                }
                joinGroup();
            } else if (e.error() == ErrorCode.UNKNOWN_MEMBER) {
                memberId = null; // session expired or the broker restarted: start over as a new member
                joinGroup();
            } else {
                throw e;
            }
        }
    }

    private void joinGroup() {
        Response.JoinGroup joined;
        try {
            joined = client.joinGroup(config.groupId(), memberId, subscriptions, config.sessionTimeoutMs(),
                    config.assignor());
        } catch (TributaryException e) {
            if (e.error() != ErrorCode.UNKNOWN_MEMBER || memberId == null) {
                throw e;
            }
            memberId = null;
            joined = client.joinGroup(config.groupId(), null, subscriptions, config.sessionTimeoutMs(),
                    config.assignor());
        }
        memberId = joined.memberId();
        generation = joined.generation();
        needsJoin = false;
        lastHeartbeatMs = System.currentTimeMillis();

        Set<TopicPartition> newAssignment = new LinkedHashSet<>(joined.assignment());
        // Partitions we keep continue from where we are; lost ones are forgotten.
        positions.keySet().retainAll(newAssignment);
        lastCommitted.keySet().retainAll(newAssignment);
        // New partitions start where the group last committed (or per autoOffsetReset).
        List<TopicPartition> fresh = new ArrayList<>();
        for (TopicPartition tp : newAssignment) {
            if (!positions.containsKey(tp)) {
                fresh.add(tp);
            }
        }
        if (!fresh.isEmpty()) {
            for (PartitionOffset po : client.fetchOffsets(config.groupId(), fresh)) {
                if (po.offset() >= 0) {
                    positions.put(po.topicPartition(), po.offset());
                    lastCommitted.put(po.topicPartition(), po.offset());
                }
            }
        }
        assignment.clear();
        assignment.addAll(newAssignment);
        nextPartitionIndex = 0;
    }

    // ---- committing --------------------------------------------------------------

    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        if (config.autoCommit() && now - lastAutoCommitMs >= config.autoCommitIntervalMs()) {
            commitQuietly();
            lastAutoCommitMs = now;
        }
    }

    /**
     * Commits this consumer's current positions, meaning "every record returned so far
     * has been processed". Only partitions whose position moved since the last commit
     * are sent.
     *
     * @return partitions the broker refused because they were reassigned to another
     *         member in a rebalance (their new owner continues from the last commit)
     */
    public Set<TopicPartition> commitSync() {
        if (!inGroup()) {
            throw new IllegalStateException("committing needs a consumer group (ConsumerConfig.withGroup)");
        }
        if (memberId == null) {
            return Set.of(); // not joined yet, so nothing has been read
        }
        List<PartitionOffset> changed = new ArrayList<>();
        for (TopicPartition tp : assignment) {
            Long position = positions.get(tp);
            if (position != null && !Objects.equals(lastCommitted.get(tp), position)) {
                changed.add(new PartitionOffset(tp.topic(), tp.partition(), position));
            }
        }
        if (changed.isEmpty()) {
            return Set.of();
        }
        Set<TopicPartition> rejected = Set.copyOf(client.commitOffsets(config.groupId(), memberId, changed));
        for (PartitionOffset po : changed) {
            if (!rejected.contains(po.topicPartition())) {
                lastCommitted.put(po.topicPartition(), po.offset());
            }
        }
        return rejected;
    }

    private void commitQuietly() {
        try {
            commitSync();
        } catch (TributaryException e) {
            // A failed auto-commit is retried at the next interval; the worst case is
            // re-delivering a few records after a crash, which at-least-once allows.
        }
    }

    /** The group's committed offset for a partition, or -1 if nothing was committed. */
    public long committed(TopicPartition partition) {
        if (!inGroup()) {
            throw new IllegalStateException("committed offsets need a consumer group (ConsumerConfig.withGroup)");
        }
        return client.fetchOffsets(config.groupId(), List.of(partition)).get(0).offset();
    }

    private void checkAssigned(TopicPartition tp) {
        if (!assignment.contains(tp)) {
            throw new IllegalStateException(tp + " is not assigned to this consumer");
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TributaryException(ErrorCode.NETWORK_ERROR, "interrupted while polling");
        }
    }

    /**
     * Closes the consumer. A group member commits its positions (if auto-commit is on)
     * and leaves the group, so its partitions move to the other members right away
     * instead of after its session times out.
     */
    @Override
    public void close() {
        if (inGroup() && memberId != null) {
            try {
                if (config.autoCommit()) {
                    commitSync();
                }
                client.leaveGroup(config.groupId(), memberId);
            } catch (TributaryException e) {
                // The broker will notice when the session times out.
            }
        }
        client.close();
    }
}
