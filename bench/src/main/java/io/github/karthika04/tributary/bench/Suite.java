package io.github.karthika04.tributary.bench;

import io.github.karthika04.tributary.chaos.BrokerProcess;
import io.github.karthika04.tributary.chaos.ChaosCluster;
import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.protocol.Compression;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The standard benchmark run: starts its own brokers (one, then a 3-broker cluster),
 * runs every scenario against each, and writes a CSV and a Markdown table.
 *
 * <ul>
 *   <li><b>throughput</b>: max rate with 100 B, 1 KB and 10 KB records.</li>
 *   <li><b>latency</b>: fixed 1,000 and 10,000 records/s of 1 KB, latency measured from
 *       the schedule (coordinated omission corrected).</li>
 *   <li><b>compression</b> (cluster): 1 KB of JSON-like text, with and without GZIP.</li>
 *   <li><b>end-to-end</b>: producer to consumer at 1,000 records/s.</li>
 * </ul>
 */
public final class Suite {

    /** Ordinary broker JVMs: the optimizing JIT on, unlike the chaos tests' quick-start settings. */
    static final List<String> BROKER_JVM_OPTIONS = List.of("-Xmx1g");

    private final Path dir;
    private final int seconds;
    private final int warmupSeconds;
    private final List<String> brokerJvmOptions;
    private final PrintStream out;
    private final Set<String> scenarios;
    private int topicNumber;

    /** The scenarios run when none are chosen. */
    public static final List<String> ALL_SCENARIOS = List.of("throughput", "latency", "compression", "end-to-end");

    public Suite(Path dir, int seconds, int warmupSeconds, List<String> brokerJvmOptions, List<String> scenarios,
                 PrintStream out) {
        for (String s : scenarios) {
            if (!ALL_SCENARIOS.contains(s)) {
                throw new IllegalArgumentException("unknown scenario " + s + "; choose from " + ALL_SCENARIOS);
            }
        }
        this.scenarios = Set.copyOf(scenarios);
        this.dir = dir;
        this.seconds = seconds;
        this.warmupSeconds = warmupSeconds;
        this.brokerJvmOptions = List.copyOf(brokerJvmOptions);
        this.out = out;
    }

    public List<PerfResult> run(List<String> deployments) throws Exception {
        List<PerfResult> results = new ArrayList<>();
        for (String deployment : deployments) {
            Path runDir = dir.resolve(deployment);
            switch (deployment) {
                case "single" -> {
                    int port = freePort();
                    Files.createDirectories(runDir);
                    BrokerProcess broker = new BrokerProcess("single", brokerJvmOptions,
                            List.of("--port", Integer.toString(port), "--data-dir", runDir.resolve("data").toString()),
                            runDir.resolve("broker.log"));
                    broker.start();
                    try {
                        List<String> addresses = List.of("localhost:" + port);
                        awaitReady(addresses);
                        results.addAll(scenarios(deployment, addresses));
                    } finally {
                        broker.stop();
                    }
                }
                case "cluster" -> {
                    try (ChaosCluster cluster = new ChaosCluster(runDir, false, brokerJvmOptions)) {
                        cluster.start();
                        cluster.awaitLeader(30_000);
                        results.addAll(scenarios(deployment, cluster.addresses()));
                    }
                }
                default -> throw new IllegalArgumentException("unknown deployment " + deployment
                        + "; use single or cluster");
            }
        }
        return results;
    }

    private List<PerfResult> scenarios(String deployment, List<String> brokers) throws InterruptedException {
        List<PerfResult> results = new ArrayList<>();
        if (scenarios.contains("throughput")) {
            for (int size : List.of(100, 1_000, 10_000)) {
                ProducePerf.Settings s = settings(brokers).withSize(size);
                results.add(report(ProducePerf.run("throughput", deployment, brokers, s)));
            }
        }
        if (scenarios.contains("latency")) {
            for (int rate : List.of(1_000, 10_000)) {
                ProducePerf.Settings s = settings(brokers).withRate(rate);
                results.add(report(ProducePerf.run("latency", deployment, brokers, s)));
            }
        }
        if (scenarios.contains("compression") && deployment.equals("cluster")) {
            for (Compression c : Compression.values()) {
                ProducePerf.Settings s = settings(brokers).withCompression(c);
                results.add(report(ProducePerf.run("compression", deployment, brokers, s)));
            }
        }
        if (scenarios.contains("end-to-end")) {
            String topic = newTopic(brokers);
            results.add(report(EndToEndPerf.run(deployment, brokers, topic, 1_000, 1_000, seconds, warmupSeconds, 5)));
        }
        return results;
    }

    /** A fresh 3-partition topic for each run, so runs do not affect each other. */
    private ProducePerfSettings settings(List<String> brokers) {
        return new ProducePerfSettings(ProducePerf.Settings.defaults(newTopic(brokers)), seconds, warmupSeconds);
    }

    private String newTopic(List<String> brokers) {
        String topic = "perf-" + (topicNumber++);
        try (TributaryClient client = new TributaryClient(ClientConfig.of(brokers).withMaxAttempts(20))) {
            client.createTopic(topic, 3);
        }
        return topic;
    }

    private PerfResult report(PerfResult r) throws InterruptedException {
        out.println(r.summary());
        // Let the brokers finish replicating and applying the last run's backlog first;
        // without this pause, a latency run right after a max-rate run measured the backlog.
        Thread.sleep(3_000);
        return r;
    }

    /** Small builder over {@link ProducePerf.Settings} for the suite's variations. */
    private record ProducePerfSettings(ProducePerf.Settings base, int seconds, int warmupSeconds) {

        private ProducePerf.Settings with(int size, int rate, Compression c, boolean compressible) {
            ProducePerf.Settings b = base;
            return new ProducePerf.Settings(b.topic(), size, rate, b.producers(), seconds, warmupSeconds,
                    b.lingerMs(), b.batchBytes(), c, compressible, b.maxOutstanding());
        }

        ProducePerf.Settings withSize(int size) {
            return with(size, 0, Compression.NONE, false);
        }

        ProducePerf.Settings withRate(int rate) {
            return with(1_000, rate, Compression.NONE, false);
        }

        ProducePerf.Settings withCompression(Compression c) {
            return with(1_000, 0, c, true);
        }
    }

    private static void awaitReady(List<String> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try (TributaryClient probe = new TributaryClient(ClientConfig.of(brokers).withMaxAttempts(1)
                    .withConnectTimeoutMs(500).withRequestTimeoutMs(1_000))) {
                probe.listTopics();
                return;
            } catch (TributaryException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** The results as a Markdown table, for the README. */
    public static String markdown(List<PerfResult> results) {
        StringBuilder md = new StringBuilder(
                "| Scenario | Deployment | Record | Compression | Rate | Records/s | MB/s | p50 ms | p99 ms | p99.9 ms |\n"
                        + "|---|---|---:|---|---:|---:|---:|---:|---:|---:|\n");
        for (PerfResult r : results) {
            md.append(String.format(Locale.ROOT, "| %s | %s | %,d B | %s | %s | %,.0f | %.1f | %.2f | %.2f | %.2f |%n",
                    r.scenario(), r.deployment(), r.recordSize(), r.compression(),
                    r.targetRate() == 0 ? "max" : String.format(Locale.ROOT, "%,d/s", r.targetRate()),
                    r.recordsPerSec(), r.megabytesPerSec(), r.p50(), r.p99(), r.p999()));
        }
        return md.toString();
    }

    static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(path)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
