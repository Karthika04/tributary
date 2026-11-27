package io.github.karthika04.tributary.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.protocol.TopicPartition;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class AssignorsTest {

    private static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    private static SortedMap<String, List<String>> members(String... namesAndTopics) {
        SortedMap<String, List<String>> result = new TreeMap<>();
        for (String entry : namesAndTopics) {
            String[] parts = entry.split(":");
            result.put(parts[0], List.of(parts[1].split(",")));
        }
        return result;
    }

    /** The example from Kafka's RangeAssignor documentation: 2 topics x 3 partitions, 2 consumers. */
    @Test
    void rangeMatchesKafkasExample() {
        Map<String, List<TopicPartition>> result = Assignors.range(
                members("C0:t0,t1", "C1:t0,t1"), Map.of("t0", 3, "t1", 3));

        assertEquals(List.of(tp("t0", 0), tp("t0", 1), tp("t1", 0), tp("t1", 1)), result.get("C0"));
        assertEquals(List.of(tp("t0", 2), tp("t1", 2)), result.get("C1"));
    }

    /** The example from Kafka's RoundRobinAssignor documentation, same setup. */
    @Test
    void roundRobinMatchesKafkasExample() {
        Map<String, List<TopicPartition>> result = Assignors.roundRobin(
                members("C0:t0,t1", "C1:t0,t1"), Map.of("t0", 3, "t1", 3));

        assertEquals(List.of(tp("t0", 0), tp("t0", 2), tp("t1", 1)), result.get("C0"));
        assertEquals(List.of(tp("t0", 1), tp("t1", 0), tp("t1", 2)), result.get("C1"));
    }

    @Test
    void membersOnlyGetTopicsTheySubscribedTo() {
        for (String assignor : List.of(Assignors.RANGE, Assignors.ROUND_ROBIN)) {
            Map<String, List<TopicPartition>> result = Assignors.assign(assignor,
                    members("A:orders", "B:orders,payments", "C:payments"), Map.of("orders", 4, "payments", 2));

            result.get("A").forEach(p -> assertEquals("orders", p.topic()));
            result.get("C").forEach(p -> assertEquals("payments", p.topic()));
            assertEveryPartitionOnce(result, Map.of("orders", 4, "payments", 2));
        }
    }

    @Test
    void moreMembersThanPartitionsLeavesSomeIdle() {
        for (String assignor : List.of(Assignors.RANGE, Assignors.ROUND_ROBIN)) {
            Map<String, List<TopicPartition>> result = Assignors.assign(assignor,
                    members("A:t", "B:t", "C:t"), Map.of("t", 2));

            assertEquals(3, result.size()); // every member is listed, even with nothing
            assertEquals(1, result.get("A").size());
            assertEquals(1, result.get("B").size());
            assertEquals(0, result.get("C").size());
        }
    }

    @Test
    void spreadsEvenlyWhenItDivides() {
        for (String assignor : List.of(Assignors.RANGE, Assignors.ROUND_ROBIN)) {
            Map<String, List<TopicPartition>> result = Assignors.assign(assignor,
                    members("A:t", "B:t", "C:t"), Map.of("t", 9));
            result.values().forEach(list -> assertEquals(3, list.size()));
            assertEveryPartitionOnce(result, Map.of("t", 9));
        }
    }

    private static void assertEveryPartitionOnce(Map<String, List<TopicPartition>> result,
                                                 Map<String, Integer> partitionCounts) {
        Set<TopicPartition> seen = new HashSet<>();
        result.values().forEach(list -> list.forEach(p -> assertTrue(seen.add(p), "assigned twice: " + p)));
        int total = partitionCounts.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(total, seen.size());
    }
}
