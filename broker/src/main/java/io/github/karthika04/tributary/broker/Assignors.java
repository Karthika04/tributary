package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.TopicPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Ways to divide a group's partitions between its members. Both are deterministic:
 * the same members and topics always give the same result, and every partition goes
 * to exactly one member that subscribed to its topic.
 */
final class Assignors {

    static final String RANGE = "range";
    static final String ROUND_ROBIN = "roundrobin";

    private Assignors() {
    }

    static boolean isKnown(String name) {
        return RANGE.equals(name) || ROUND_ROBIN.equals(name);
    }

    /**
     * @param subscriptions   member id to the topics it wants, for every member
     * @param partitionCounts topic to partition count, for every subscribed topic
     * @return member id to its partitions; every member appears, possibly with none
     */
    static Map<String, List<TopicPartition>> assign(String assignor, SortedMap<String, List<String>> subscriptions,
                                                     Map<String, Integer> partitionCounts) {
        return switch (assignor) {
            case RANGE -> range(subscriptions, partitionCounts);
            case ROUND_ROBIN -> roundRobin(subscriptions, partitionCounts);
            default -> throw new IllegalArgumentException("unknown assignor " + assignor);
        };
    }

    /**
     * Topic by topic, split the partitions into contiguous ranges, one per subscribed
     * member in id order. When it does not divide evenly the first members get one
     * extra. Example, 3 partitions and 2 members: A gets 0-1, B gets 2.
     *
     * <p>Good when members join two topics on the same key: partition N of each topic
     * lands on the same member. Can be uneven across many topics, because the first
     * members get the extra partition of every topic.
     */
    static Map<String, List<TopicPartition>> range(SortedMap<String, List<String>> subscriptions,
                                                    Map<String, Integer> partitionCounts) {
        Map<String, List<TopicPartition>> result = emptyResult(subscriptions);
        for (String topic : new TreeSet<>(partitionCounts.keySet())) {
            List<String> members = new ArrayList<>();
            subscriptions.forEach((member, topics) -> {
                if (topics.contains(topic)) {
                    members.add(member);
                }
            });
            if (members.isEmpty()) {
                continue;
            }
            int partitions = partitionCounts.get(topic);
            int perMember = partitions / members.size();
            int extra = partitions % members.size();
            int next = 0;
            for (int i = 0; i < members.size(); i++) {
                int count = perMember + (i < extra ? 1 : 0);
                for (int p = next; p < next + count; p++) {
                    result.get(members.get(i)).add(new TopicPartition(topic, p));
                }
                next += count;
            }
        }
        return result;
    }

    /**
     * Lay out every partition of every topic in order and deal them out like cards, one
     * to each member in turn, skipping members not subscribed to that topic. Spreads
     * partitions as evenly as possible across all topics.
     */
    static Map<String, List<TopicPartition>> roundRobin(SortedMap<String, List<String>> subscriptions,
                                                         Map<String, Integer> partitionCounts) {
        Map<String, List<TopicPartition>> result = emptyResult(subscriptions);
        List<String> members = new ArrayList<>(subscriptions.keySet());
        int turn = 0;
        for (String topic : new TreeSet<>(partitionCounts.keySet())) {
            for (int p = 0; p < partitionCounts.get(topic); p++) {
                for (int tries = 0; tries < members.size(); tries++) {
                    String member = members.get(turn % members.size());
                    turn++;
                    if (subscriptions.get(member).contains(topic)) {
                        result.get(member).add(new TopicPartition(topic, p));
                        break;
                    }
                }
            }
        }
        return result;
    }

    private static Map<String, List<TopicPartition>> emptyResult(SortedMap<String, List<String>> subscriptions) {
        Map<String, List<TopicPartition>> result = new TreeMap<>();
        subscriptions.keySet().forEach(member -> result.put(member, new ArrayList<>()));
        return result;
    }
}
