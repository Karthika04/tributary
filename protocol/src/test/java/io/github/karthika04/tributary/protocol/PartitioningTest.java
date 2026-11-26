package io.github.karthika04.tributary.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PartitioningTest {

    /** Expected values from Kafka's own test suite for Utils.murmur2. */
    @Test
    void murmur2MatchesKafka() {
        assertEquals(-973932308, Partitioning.murmur2("21".getBytes(UTF_8)));
        assertEquals(-790332482, Partitioning.murmur2("foobar".getBytes(UTF_8)));
        assertEquals(-985981536, Partitioning.murmur2("a-little-bit-long-string".getBytes(UTF_8)));
        assertEquals(-1486304829, Partitioning.murmur2("a-little-bit-longer-string".getBytes(UTF_8)));
        assertEquals(-58897971,
                Partitioning.murmur2("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8".getBytes(UTF_8)));
        assertEquals(479470107, Partitioning.murmur2("abc".getBytes(UTF_8)));
    }

    @Test
    void sameKeyAlwaysGetsSamePartition() {
        byte[] key = "user-42".getBytes(UTF_8);
        int first = Partitioning.partitionForKey(key, 8);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, Partitioning.partitionForKey(key, 8));
        }
    }

    @Test
    void keysSpreadAcrossAllPartitions() {
        int partitions = 8;
        int[] counts = new int[partitions];
        for (int i = 0; i < 8000; i++) {
            counts[Partitioning.partitionForKey(("key-" + i).getBytes(UTF_8), partitions)]++;
        }
        for (int count : counts) {
            assertTrue(count > 800 && count < 1200, "uneven spread: " + java.util.Arrays.toString(counts));
        }
    }

    @Test
    void rejectsZeroPartitions() {
        assertThrows(IllegalArgumentException.class, () -> Partitioning.partitionForKey(new byte[1], 0));
    }
}
