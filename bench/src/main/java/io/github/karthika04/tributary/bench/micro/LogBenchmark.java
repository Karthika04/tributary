package io.github.karthika04.tributary.bench.micro;

import io.github.karthika04.tributary.storage.Log;
import io.github.karthika04.tributary.storage.LogConfig;
import io.github.karthika04.tributary.storage.NewRecord;
import io.github.karthika04.tributary.storage.Record;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * The storage engine on its own: appending batches of 100 records, with and without an
 * fsync per batch ({@link Append}), and reading batches back by offset ({@link Read}).
 *
 * <p>Why JMH and not a timing loop: the JVM compiles hot code while it runs, so early
 * iterations are slow; it can also delete work whose result nobody uses. JMH warms up
 * first, runs each benchmark in a fresh JVM (fork), and feeds results to a
 * {@link Blackhole} so nothing is optimized away.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class LogBenchmark {

    private static final int BATCH = 100;

    private static List<NewRecord> batch(int recordSize) {
        byte[] value = new byte[recordSize];
        new Random(42).nextBytes(value);
        List<NewRecord> batch = new ArrayList<>(BATCH);
        for (int i = 0; i < BATCH; i++) {
            batch.add(new NewRecord(System.currentTimeMillis(), null, value));
        }
        return batch;
    }

    private static void delete(Path dir) throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path p : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }

    /** A fresh, empty log for every measurement iteration, so the disk does not fill up. */
    @State(Scope.Benchmark)
    public static class Append {
        @Param({"100", "1000"})
        public int recordSize;

        /** false: rely on the OS page cache (the default); true: fsync every batch. */
        @Param({"false", "true"})
        public boolean flushEveryAppend;

        Path dir;
        Log log;
        List<NewRecord> batch;

        @Setup(Level.Iteration)
        public void open() throws IOException {
            dir = Files.createTempDirectory("log-bench");
            log = Log.open(dir, LogConfig.defaults().withFlushEveryAppend(flushEveryAppend));
            batch = batch(recordSize);
        }

        @TearDown(Level.Iteration)
        public void close() throws IOException {
            log.close();
            delete(dir);
        }
    }

    /** A log of 100,000 records, written once. */
    @State(Scope.Benchmark)
    public static class Read {
        @Param({"100", "1000"})
        public int recordSize;

        Path dir;
        Log log;
        long offset;

        @Setup(Level.Trial)
        public void fill() throws IOException {
            dir = Files.createTempDirectory("log-bench");
            log = Log.open(dir, LogConfig.defaults());
            List<NewRecord> batch = batch(recordSize);
            for (int i = 0; i < 1_000; i++) {
                log.appendAll(batch);
            }
        }

        @TearDown(Level.Trial)
        public void close() throws IOException {
            log.close();
            delete(dir);
        }
    }

    /** Batches of 100 records appended per second (multiply by 100 for records per second). */
    @Benchmark
    public long appendBatchOf100(Append state) {
        return state.log.appendAll(state.batch);
    }

    /** Batches of 100 records read per second, from offsets spread over the log. */
    @Benchmark
    public void readBatchOf100(Read state, Blackhole hole) {
        state.offset = (state.offset + 7_919) % (state.log.endOffset() - BATCH);
        List<Record> records = state.log.read(state.offset, BATCH);
        hole.consume(records);
    }
}
