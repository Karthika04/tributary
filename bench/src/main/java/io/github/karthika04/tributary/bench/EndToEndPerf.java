package io.github.karthika04.tributary.bench;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.Consumer;
import io.github.karthika04.tributary.client.ConsumerConfig;
import io.github.karthika04.tributary.client.ConsumerRecord;
import io.github.karthika04.tributary.client.Producer;
import io.github.karthika04.tributary.client.ProducerConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.protocol.TopicPartition;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * End-to-end latency: the time from a record's scheduled send until a consumer in the
 * same JVM receives it. This is what an application feels: batching (linger),
 * replication, the fetch long-poll waking up, and delivery, all together.
 *
 * <p>Each record carries its scheduled send time ({@code System.nanoTime()}) in its first
 * 8 bytes. Producer and consumer share one JVM, so the clocks are the same clock.
 */
public final class EndToEndPerf {

    private EndToEndPerf() {
    }

    public static PerfResult run(String deployment, List<String> brokers, String topic, int recordSize,
                                 int ratePerSec, int seconds, int warmupSeconds, int lingerMs)
            throws InterruptedException {
        if (ratePerSec <= 0 || recordSize < 8) {
            throw new IllegalArgumentException("end-to-end needs a fixed rate and records of at least 8 bytes");
        }
        int partitions;
        try (TributaryClient client = new TributaryClient(ClientConfig.of(brokers))) {
            partitions = client.partitionCount(topic);
        }
        LatencyRecorder latencies = new LatencyRecorder();
        AtomicLong received = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();
        long start = System.nanoTime();
        long measureFrom = start + TimeUnit.SECONDS.toNanos(warmupSeconds);
        long end = measureFrom + TimeUnit.SECONDS.toNanos(seconds);

        Consumer consumer = new Consumer(ConsumerConfig.of(ClientConfig.of(brokers).withClientId("perf-e2e-consumer")));
        List<TopicPartition> all = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            all.add(new TopicPartition(topic, p));
        }
        consumer.assign(all);
        consumer.seekToEnd(all);
        Thread reader = Thread.ofPlatform().name("perf-e2e-consumer").start(() -> {
            try (consumer) {
                while (!stop.get()) {
                    for (ConsumerRecord r : consumer.poll(Duration.ofMillis(200))) {
                        long now = System.nanoTime();
                        long scheduled = ByteBuffer.wrap(r.value()).getLong();
                        if (scheduled >= measureFrom && scheduled < end) {
                            latencies.recordNanos(now - scheduled);
                            received.incrementAndGet();
                        }
                    }
                }
            }
        });

        long sent = 0;
        long intervalNanos = 1_000_000_000L / ratePerSec;
        ProducerConfig config = ProducerConfig.of(ClientConfig.of(brokers).withClientId("perf-e2e-producer"))
                .withLingerMs(lingerMs);
        try (Producer producer = new Producer(config)) {
            for (long i = 0; ; i++) {
                long scheduled = start + i * intervalNanos;
                if (scheduled >= end) {
                    break;
                }
                long wait = scheduled - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
                byte[] value = new byte[recordSize];
                ByteBuffer.wrap(value).putLong(scheduled);
                producer.send(topic, -1, null, value);
                if (scheduled >= measureFrom) {
                    sent++;
                }
            }
            producer.flush();
        }
        // Give the consumer time to receive the last records.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (received.get() < sent && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        stop.set(true);
        reader.join();

        long missing = sent - received.get();
        return new PerfResult("end-to-end", deployment, recordSize, "none", 1, ratePerSec, received.get(),
                received.get() / (double) seconds, received.get() * (double) recordSize / seconds / (1024 * 1024),
                latencies.millisAt(50), latencies.millisAt(95), latencies.millisAt(99), latencies.millisAt(99.9),
                latencies.maxMillis(), Math.max(0, missing));
    }
}
