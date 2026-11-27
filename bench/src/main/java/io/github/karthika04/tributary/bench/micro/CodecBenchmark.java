package io.github.karthika04.tributary.bench.micro;

import io.github.karthika04.tributary.protocol.Codec;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.RequestEnvelope;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The wire format: encoding and decoding a produce request of 100 records of 1 KB of
 * JSON-like text, with and without GZIP. Shows what compression costs in CPU.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class CodecBenchmark {

    @Param({"NONE", "GZIP"})
    public String compression;

    private Request.Produce request;
    private byte[] encoded;

    @Setup
    public void prepare() {
        byte[] text = "{\"user\":\"u-1042\",\"event\":\"page_view\",\"path\":\"/checkout\",\"ms\":184}\n"
                .getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1_000];
        for (int i = 0; i < value.length; i++) {
            value[i] = text[i % text.length];
        }
        List<ProducedRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(new ProducedRecord(null, value));
        }
        request = new Request.Produce("orders", 0, Compression.valueOf(compression), records);
        encoded = Codec.encodeRequest(1, "bench", request);
    }

    @Benchmark
    public byte[] encodeProduce() {
        return Codec.encodeRequest(1, "bench", request);
    }

    @Benchmark
    public RequestEnvelope decodeProduce() {
        return Codec.decodeRequest(encoded);
    }
}
