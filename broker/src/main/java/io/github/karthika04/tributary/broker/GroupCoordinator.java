package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.lang.System.Logger.Level;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Manages consumer groups: who is in each group, which partitions each member owns,
 * and where each group has got to.
 *
 * <p><b>How a group works.</b> Consumers that share a group id split the partitions of
 * the topics they read, so each record is processed by one member of the group.
 * (Different groups each get every record.)
 *
 * <ol>
 *   <li>A consumer <em>joins</em>. The broker gives it a member id, bumps the group's
 *       <em>generation</em> and reassigns all partitions among the members.</li>
 *   <li>Members send <em>heartbeats</em>. A member whose generation is older than the
 *       group's gets {@code REBALANCE_IN_PROGRESS} and joins again to learn its new
 *       partitions.</li>
 *   <li>A member that leaves, or sends no heartbeat within its session timeout, is
 *       removed and its partitions are reassigned.</li>
 * </ol>
 *
 * <p><b>Assignment happens on the broker</b>, in one step. Kafka's original protocol
 * had a consumer compute it; Kafka's newer protocol (KIP-848) also moved it to the
 * broker, because one source of truth is simpler and faster.
 *
 * <p><b>Fencing.</b> Only the member that currently owns a partition may commit its
 * offset. A slow "zombie" consumer that lost its partition in a rebalance cannot
 * overwrite the new owner's progress.
 *
 * <p><b>Delivery stays at-least-once.</b> Right after a rebalance a partition's old
 * owner may still be finishing a batch while the new owner starts from the last commit,
 * so a few records can be processed twice. None are skipped.
 *
 * <p>Membership lives in memory; committed offsets are durable ({@link OffsetStore}). After
 * a broker restart, members get {@code UNKNOWN_MEMBER} and simply join again.
 */
final class GroupCoordinator {

    private static final System.Logger LOG = System.getLogger(GroupCoordinator.class.getName());
    static final int MIN_SESSION_TIMEOUT_MS = 1_000;
    static final int MAX_SESSION_TIMEOUT_MS = 300_000;

    private final TopicManager topics;
    private final OffsetStore offsets;
    private final Clock clock;
    private final Map<String, Group> groups = new HashMap<>();

    GroupCoordinator(TopicManager topics, OffsetStore offsets, Clock clock) {
        this.topics = topics;
        this.offsets = offsets;
        this.clock = clock;
    }

    synchronized Response.JoinGroup join(String groupId, String memberId, List<String> subscribedTopics,
                                         int sessionTimeoutMs, String assignor) {
        checkGroupId(groupId);
        if (!Assignors.isKnown(assignor)) {
            throw new BrokerException(ErrorCode.INVALID_GROUP, "unknown assignor '" + assignor
                    + "'; use '" + Assignors.RANGE + "' or '" + Assignors.ROUND_ROBIN + "'");
        }
        if (sessionTimeoutMs < MIN_SESSION_TIMEOUT_MS || sessionTimeoutMs > MAX_SESSION_TIMEOUT_MS) {
            throw new BrokerException(ErrorCode.INVALID_GROUP, "session timeout must be between "
                    + MIN_SESSION_TIMEOUT_MS + " and " + MAX_SESSION_TIMEOUT_MS + " ms");
        }
        if (subscribedTopics.isEmpty()) {
            throw new BrokerException(ErrorCode.INVALID_GROUP, "subscribe to at least one topic");
        }
        for (String topic : subscribedTopics) {
            topics.partitionCount(topic); // throws UNKNOWN_TOPIC
        }

        Group group = groups.computeIfAbsent(groupId, id -> new Group(assignor));
        if (group.members.isEmpty()) {
            group.assignor = assignor; // an empty group adopts whatever the first member asks for
        } else if (!group.assignor.equals(assignor)) {
            throw new BrokerException(ErrorCode.INVALID_GROUP, "group '" + groupId + "' uses the '"
                    + group.assignor + "' assignor, not '" + assignor + "'");
        }

        long now = clock.millis();
        boolean rebalance;
        Member member;
        if (memberId == null || memberId.isEmpty()) {
            member = new Member(groupId + "-" + UUID.randomUUID().toString().substring(0, 8));
            group.members.put(member.id, member);
            rebalance = true;
        } else {
            member = group.members.get(memberId);
            if (member == null) {
                throw new BrokerException(ErrorCode.UNKNOWN_MEMBER, "member " + memberId
                        + " is not in group " + groupId + "; join again without a member id");
            }
            rebalance = !member.topics.equals(List.copyOf(subscribedTopics));
        }
        member.topics = List.copyOf(subscribedTopics);
        member.sessionTimeoutMs = sessionTimeoutMs;
        member.lastHeartbeatMs = now;
        if (rebalance) {
            rebalance(groupId, group, "member " + member.id + " joined");
        }
        member.generation = group.generation;
        return new Response.JoinGroup(member.id, group.generation, member.assignment);
    }

    synchronized void heartbeat(String groupId, String memberId, int generation) {
        Member member = member(groupId, memberId);
        member.lastHeartbeatMs = clock.millis();
        Group group = groups.get(groupId);
        if (generation != group.generation) {
            throw new BrokerException(ErrorCode.REBALANCE_IN_PROGRESS, "group " + groupId
                    + " was rebalanced (generation " + group.generation + "); join again");
        }
    }

    synchronized void leave(String groupId, String memberId) {
        Group group = groups.get(groupId);
        if (group != null && group.members.remove(memberId) != null) {
            rebalance(groupId, group, "member " + memberId + " left");
        }
    }

    /**
     * Saves the offsets for partitions this member owns. Offsets for partitions it does
     * not own are not saved and are returned, so a stale member cannot overwrite the
     * progress of the partition's new owner.
     */
    synchronized List<TopicPartition> commit(String groupId, String memberId, List<PartitionOffset> commits) {
        Member member = member(groupId, memberId);
        member.lastHeartbeatMs = clock.millis(); // committing proves the member is alive
        List<PartitionOffset> accepted = new ArrayList<>();
        List<TopicPartition> rejected = new ArrayList<>();
        for (PartitionOffset po : commits) {
            if (po.offset() < 0) {
                throw new BrokerException(ErrorCode.INVALID_REQUEST, "committed offsets must be >= 0");
            }
            if (member.assignment.contains(po.topicPartition())) {
                accepted.add(po);
            } else {
                rejected.add(po.topicPartition());
            }
        }
        offsets.commit(groupId, accepted);
        return rejected;
    }

    List<PartitionOffset> fetchOffsets(String groupId, List<TopicPartition> partitions) {
        checkGroupId(groupId);
        List<PartitionOffset> result = new ArrayList<>(partitions.size());
        for (TopicPartition tp : partitions) {
            result.add(new PartitionOffset(tp.topic(), tp.partition(), offsets.committed(groupId, tp)));
        }
        return result;
    }

    /** Removes members whose session timed out. Called on a timer. @return members removed */
    synchronized int expireMembers() {
        long now = clock.millis();
        int removed = 0;
        for (Map.Entry<String, Group> entry : groups.entrySet()) {
            Group group = entry.getValue();
            List<String> expired = new ArrayList<>();
            group.members.values().forEach(m -> {
                if (now - m.lastHeartbeatMs > m.sessionTimeoutMs) {
                    expired.add(m.id);
                }
            });
            if (!expired.isEmpty()) {
                expired.forEach(group.members::remove);
                removed += expired.size();
                rebalance(entry.getKey(), group, "session expired for " + expired);
            }
        }
        return removed;
    }

    /** For tests and tooling: who owns what right now. */
    synchronized Map<String, List<TopicPartition>> assignments(String groupId) {
        Group group = groups.get(groupId);
        Map<String, List<TopicPartition>> result = new TreeMap<>();
        if (group != null) {
            group.members.values().forEach(m -> result.put(m.id, List.copyOf(m.assignment)));
        }
        return result;
    }

    synchronized int generation(String groupId) {
        Group group = groups.get(groupId);
        return group == null ? 0 : group.generation;
    }

    private void rebalance(String groupId, Group group, String reason) {
        group.generation++;
        SortedMap<String, List<String>> subscriptions = new TreeMap<>();
        Map<String, Integer> partitionCounts = new HashMap<>();
        for (Member m : group.members.values()) {
            subscriptions.put(m.id, m.topics);
            for (String topic : m.topics) {
                partitionCounts.computeIfAbsent(topic, topics::partitionCount);
            }
        }
        Map<String, List<TopicPartition>> assignment = Assignors.assign(group.assignor, subscriptions, partitionCounts);
        for (Member m : group.members.values()) {
            m.assignment = List.copyOf(assignment.get(m.id));
        }
        LOG.log(Level.INFO, "group " + groupId + " rebalanced to generation " + group.generation + " ("
                + reason + "): " + group.members.size() + " member(s)");
    }

    private Member member(String groupId, String memberId) {
        Group group = groups.get(groupId);
        Member member = group == null ? null : group.members.get(memberId);
        if (member == null) {
            throw new BrokerException(ErrorCode.UNKNOWN_MEMBER, "member " + memberId
                    + " is not in group " + groupId + "; join again without a member id");
        }
        return member;
    }

    private static void checkGroupId(String groupId) {
        if (!TopicManager.isValidName(groupId)) {
            throw new BrokerException(ErrorCode.INVALID_GROUP, "invalid group id '" + groupId
                    + "': use 1-200 letters, digits, '.', '_' or '-'");
        }
    }

    private static final class Group {
        String assignor;
        int generation;
        /** Sorted by member id so assignments are deterministic. */
        final Map<String, Member> members = new TreeMap<>();

        Group(String assignor) {
            this.assignor = assignor;
        }
    }

    private static final class Member {
        final String id;
        List<String> topics = List.of();
        int sessionTimeoutMs;
        long lastHeartbeatMs;
        int generation;
        List<TopicPartition> assignment = List.of();

        Member(String id) {
            this.id = id;
        }
    }
}
