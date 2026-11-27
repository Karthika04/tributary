package io.github.karthika04.tributary.bench;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.Producer;
import io.github.karthika04.tributary.client.ProducerConfig;
import io.github.karthika04.tributary.protocol.Compression;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Measures producer throughput and latency, like Kafka's {@code producer-perf-test}.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>Max rate</b> ({@code ratePerSec == 0}): send as fast as the cluster accepts,
 *       with at most {@code maxOutstanding} unacknowledged records. This measures
 *       throughput; the latencies are those of a saturated system.</li>
 *   <li><b>Fixed rate</b>: send on a fixed schedule and measure each record's latency
 *       from when it was <em>scheduled</em> to be sent, not from when it actually was.
 *       This corrects for <b>coordinated omission</b>: if the system stalls for a second,
 *       a naive benchmark simply sends nothing during that second and records one slow
 *       request, hiding that every request a real client would have made in that second
 *       waited too. Measuring from the schedule counts all of them.</li>
 * </ul>
 */
public final class ProducePerf {

    /**
     * @param ratePerSec         records per second across all producers, or 0 for max rate
     * @param compressible       payloads of repeated JSON-like text instead of random bytes
     * @param maxOutstanding     cap on unacknowledged records, across all producers
     */
    public record Settings(String topic, int recordSize, int ratePerSec, int producers, int seconds,
                           int warmupSeconds, int lingerMs, int batchBytes, Compression compression,
                           boolean compressible, int maxOutstanding) {

        public Settings {
            if (recordSize < 8 || ratePerSec < 0 || producers < 1 || seconds < 1 || warmupSeconds < 0
                    || maxOutstanding < producers) {
                throw new IllegalArgumentException("invalid benchmark settings");
            }
        }

        public static Settings defaults(String topic) {
            ProducerConfig producer = ProducerConfig.of(ClientConfig.of("localhost", 9292)); // just for its defaults
            return new Settings(topic, 1_000, 0, 1, 10, 3, producer.lingerMs(), producer.batchBytes(), Compression.NONE,
                    false, 20_000);
        }
    }

    private ProducePerf() {
    }

    public static PerfResult run(String scenario, String deployment, List<String> brokers, Settings s)
            throws InterruptedException {
        long start = System.nanoTime();
        long measureFrom = start + TimeUnit.SECONDS.toNanos(s.warmupSeconds());
        long end = measureFrom + TimeUnit.SECONDS.toNanos(s.seconds());
        LatencyRecorder latencies = new LatencyRecorder();
        AtomicLong measured = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        AtomicLong lastCompletion = new AtomicLong(measureFrom);

        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < s.producers(); p++) {
            int index = p;
            threads.add(Thread.ofPlatform().name("perf-producer-" + p).start(() ->
                    produce(index, brokers, s, start, measureFrom, end, latencies, measured, errors, lastCompletion)));
        }
        for (Thread t : threads) {
            t.join();
        }

        double seconds = Math.max(1e-9, (lastCompletion.get() - measureFrom) / 1e9);
        long records = measured.get();
        return new PerfResult(scenario, deployment, s.recordSize(), s.compression().name().toLowerCase(),
                s.producers(), s.ratePerSec(), records, records / seconds,
                records * (double) s.recordSize() / seconds / (1024 * 1024),
                latencies.millisAt(50), latencies.millisAt(95), latencies.millisAt(99), latencies.millisAt(99.9),
                latencies.maxMillis(), errors.get());
    }

    private static void produce(int index, List<String> brokers, Settings s, long start, long measureFrom, long end,
                                LatencyRecorder latencies, AtomicLong measured, AtomicLong errors,
                                AtomicLong lastCompletion) {
        byte[] payload = payload(s.recordSize(), s.compressible(), index);
        Semaphore outstanding = new Semaphore(s.maxOutstanding() / s.producers());
        long intervalNanos = s.ratePerSec() == 0 ? 0 : 1_000_000_000L * s.producers() / s.ratePerSec();
        ProducerConfig config = ProducerConfig.of(ClientConfig.of(brokers).withClientId("perf-" + index))
                .withLingerMs(s.lingerMs()).withBatchBytes(s.batchBytes()).withCompression(s.compression());

        try (Producer producer = new Producer(config)) {
            for (long i = 0; ; i++) {
                long scheduled;
                if (intervalNanos > 0) {
                    scheduled = start + i * intervalNanos;
                    long wait = scheduled - System.nanoTime();
                    if (wait > 0) {
                        LockSupport.parkNanos(wait);
                    }
                } else {
                    scheduled = System.nanoTime();
                }
                if (scheduled >= end) {
                    break;
                }
                outstanding.acquireUninterruptibly();
                // Fixed rate: latency counts from the schedule (coordinated omission).
                long from = intervalNanos > 0 ? scheduled : System.nanoTime();
                boolean measure = scheduled >= measureFrom;
                producer.send(s.topic(), -1, null, payload).whenComplete((meta, error) -> {
                    long now = System.nanoTime();
                    outstanding.release();
                    if (!measure) {
                        return;
                    }
                    if (error != null) {
                        errors.incrementAndGet();
                        return;
                    }
                    latencies.recordNanos(now - from);
                    measured.incrementAndGet();
                    lastCompletion.accumulateAndGet(now, Math::max);
                });
            }
            producer.flush();
        }
    }

    /**
     * Random bytes by default: compression can do nothing with them, so throughput is
     * not flattered. With {@code compressible}, repeated JSON-like text, as real events often are.
     */
    static byte[] payload(int size, boolean compressible, int seed) {
        byte[] payload = new byte[size];
        if (compressible) {
            byte[] text = "{\"user\":\"u-1042\",\"event\":\"page_view\",\"path\":\"/checkout\",\"ms\":184}\n"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            for (int i = 0; i < size; i++) {
                payload[i] = text[i % text.length];
            }
        } else {
            new Random(seed).nextBytes(payload);
        }
        return payload;
    }
}
