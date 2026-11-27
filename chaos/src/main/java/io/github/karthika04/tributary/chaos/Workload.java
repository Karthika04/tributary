package io.github.karthika04.tributary.chaos;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Producers that write numbered values and remember exactly which writes the cluster
 * acknowledged, and at which offset. Producer {@code k} writes {@code "p<k>-<seq>"} to
 * partition {@code k}, one record per request and one request at a time, so for every
 * value we know whether it was acknowledged (and where) or failed.
 */
public final class Workload {

    /** What happened to one write. {@code offset} is -1 if it failed. */
    public record Attempt(int producer, long seq, long offset, long startedNanos, long endedNanos) {
        public boolean acknowledged() {
            return offset >= 0;
        }
    }

    public static String value(int producer, long seq) {
        return "p" + producer + "-" + seq;
    }

    private final String topic;
    private final int producers;
    private final List<String> brokers;
    private final Map<Integer, List<Attempt>> attempts = new ConcurrentHashMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running = true;

    public Workload(String topic, int producers, List<String> brokers) {
        this.topic = topic;
        this.producers = producers;
        this.brokers = brokers;
    }

    public void start() {
        for (int p = 0; p < producers; p++) {
            int producer = p;
            attempts.put(producer, new ArrayList<>());
            threads.add(Thread.ofPlatform().name("producer-" + p).start(() -> run(producer)));
        }
    }

    private void run(int producer) {
        ClientConfig config = ClientConfig.of(brokers).withClientId("chaos-producer-" + producer)
                .withConnectTimeoutMs(1_000).withRequestTimeoutMs(5_000).withMaxAttempts(5);
        List<Attempt> log = attempts.get(producer);
        try (TributaryClient client = new TributaryClient(config)) {
            for (long seq = 0; running; seq++) {
                long started = System.nanoTime();
                long offset = -1;
                try {
                    offset = client.produce(topic, producer,
                            List.of(new ProducedRecord(null, value(producer, seq).getBytes(UTF_8))), Compression.NONE);
                } catch (TributaryException e) {
                    // not acknowledged: it may or may not have been stored
                }
                synchronized (log) {
                    log.add(new Attempt(producer, seq, offset, started, System.nanoTime()));
                }
            }
        }
    }

    /** Stops writing and waits for in-flight requests to finish. */
    public void stop() throws InterruptedException {
        running = false;
        for (Thread t : threads) {
            t.join(60_000);
            if (t.isAlive()) {
                throw new IllegalStateException(t.getName() + " is stuck");
            }
        }
    }

    public List<Attempt> attempts(int producer) {
        List<Attempt> log = attempts.get(producer);
        synchronized (log) {
            return List.copyOf(log);
        }
    }

    public int producers() {
        return producers;
    }
}
