package io.github.karthika04.tributary.protocol;

/**
 * Chooses a partition for a record key.
 *
 * <p>Records with the same key always go to the same partition, and a partition keeps
 * its records in order, so all events for one key (one user, one order...) are read in
 * the order they were written. This uses the same murmur2 hash as Kafka's default
 * partitioner, a fast hash that spreads keys evenly.
 */
public final class Partitioning {

    private Partitioning() {
    }

    public static int partitionForKey(byte[] key, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        return toPositive(murmur2(key)) % partitionCount;
    }

    /** Clears the sign bit. (Math.abs would fail for Integer.MIN_VALUE.) */
    static int toPositive(int n) {
        return n & 0x7fffffff;
    }

    /** 32-bit murmur2, identical to Kafka's {@code Utils.murmur2}. */
    public static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int tail = length & ~3;
        int remaining = length % 4;
        if (remaining == 3) {
            h ^= (data[tail + 2] & 0xff) << 16;
        }
        if (remaining >= 2) {
            h ^= (data[tail + 1] & 0xff) << 8;
        }
        if (remaining >= 1) {
            h ^= data[tail] & 0xff;
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
}
