package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The Raft produce command (1,000 records of 1 KB, about 1 MB), which every produced
 * byte passes through in a cluster. Compares the original encoder (a growing
 * {@code ByteArrayOutputStream}) with the exact-size buffer that replaced it after
 * profiling, and measures decoding.
 *
 * <p>It sits in the broker's package (in this separate module) because
 * {@code Commands} is package-private.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class CommandsBenchmark {

    private List<ProducedRecord> records;
    private byte[] command;
    private final Writes discard = new Writes() {
        @Override
        public void createTopic(String topic, int partitions) {
        }

        @Override
        public long produce(String topic, int partition, long timestamp, List<ProducedRecord> rs) {
            return rs.size();
        }

        @Override
        public void commitOffsets(String group, List<PartitionOffset> offsets) {
        }
    };

    @Setup
    public void prepare() {
        Random random = new Random(42);
        records = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            byte[] value = new byte[1_000];
            random.nextBytes(value);
            records.add(new ProducedRecord(null, value));
        }
        command = Commands.produce("orders", 1, 42L, records);
    }

    /** The encoder before profiling: a stream whose buffer doubles (and is copied) as it grows. */
    @Benchmark
    public byte[] encodeWithGrowingStream() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(2);
        out.writeUTF("orders");
        out.writeInt(1);
        out.writeLong(42L);
        out.writeInt(records.size());
        for (ProducedRecord r : records) {
            out.writeInt(-1);
            out.writeInt(r.value().length);
            out.write(r.value());
        }
        return bytes.toByteArray();
    }

    /** The encoder now: one buffer of exactly the right size. */
    @Benchmark
    public byte[] encodeExactSize() {
        return Commands.produce("orders", 1, 42L, records);
    }

    @Benchmark
    public Object decode() {
        return Commands.apply(command, discard);
    }
}
