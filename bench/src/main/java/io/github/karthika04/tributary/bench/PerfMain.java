package io.github.karthika04.tributary.bench;

import io.github.karthika04.tributary.protocol.Compression;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code tributary-perf} command.
 *
 * <pre>
 * tributary-perf suite [--seconds 10] [--warmup 3] [--deployments single,cluster] [--out perf-results]
 *                      [--scenarios throughput,latency,compression,end-to-end]
 *                      [--broker-jvm "-Xmx1g -XX:StartFlightRecording=..."]
 * tributary-perf produce --bootstrap host:port[,...] --topic T [--record-size 1000] [--rate 0]
 *                        [--producers 1] [--seconds 10] [--warmup 3] [--linger-ms 5]
 *                        [--batch-bytes 65536] [--compression none|gzip] [--compressible]
 * tributary-perf e2e --bootstrap host:port[,...] --topic T [--record-size 1000] [--rate 1000]
 *                    [--seconds 10] [--warmup 3] [--linger-ms 5]
 * </pre>
 */
public final class PerfMain {

    private PerfMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].equals("--help")) {
            System.out.println(usage());
            return;
        }
        Map<String, String> options = parse(args);
        PrintStream out = System.out;
        switch (args[0]) {
            case "suite" -> suite(options, out);
            case "produce" -> {
                List<String> brokers = brokers(options);
                ProducePerf.Settings d = ProducePerf.Settings.defaults(required(options, "topic"));
                ProducePerf.Settings s = new ProducePerf.Settings(d.topic(),
                        integer(options, "record-size", d.recordSize()), integer(options, "rate", 0),
                        integer(options, "producers", 1), integer(options, "seconds", 10),
                        integer(options, "warmup", 3), integer(options, "linger-ms", d.lingerMs()),
                        integer(options, "batch-bytes", d.batchBytes()),
                        Compression.valueOf(options.getOrDefault("compression", "none").toUpperCase()),
                        options.containsKey("compressible"), d.maxOutstanding());
                out.println(ProducePerf.run("produce", "custom", brokers, s).summary());
            }
            case "e2e" -> out.println(EndToEndPerf.run("custom", brokers(options), required(options, "topic"),
                    integer(options, "record-size", 1_000), integer(options, "rate", 1_000),
                    integer(options, "seconds", 10), integer(options, "warmup", 3),
                    integer(options, "linger-ms", 5)).summary());
            default -> {
                System.err.println("unknown command " + args[0] + "\n" + usage());
                System.exit(2);
            }
        }
    }

    private static void suite(Map<String, String> options, PrintStream out) throws Exception {
        Path dir = Path.of(options.getOrDefault("out", "perf-results"));
        Files.createDirectories(dir);
        Path work = Files.createTempDirectory("tributary-perf");
        List<String> jvm = options.containsKey("broker-jvm")
                ? List.of(options.get("broker-jvm").trim().split("\\s+")) : Suite.BROKER_JVM_OPTIONS;
        out.printf("Tributary benchmark suite on %s %s, %d CPUs, Java %s%n%n", System.getProperty("os.name"),
                System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(),
                System.getProperty("java.version"));
        List<String> scenarios = options.containsKey("scenarios")
                ? List.of(options.get("scenarios").split(",")) : Suite.ALL_SCENARIOS;
        Suite suite = new Suite(work, integer(options, "seconds", 10), integer(options, "warmup", 3), jvm, scenarios,
                out);
        List<PerfResult> results;
        try {
            results = suite.run(List.of(options.getOrDefault("deployments", "single,cluster").split(",")));
        } finally {
            Suite.deleteRecursively(work);
        }
        StringBuilder csv = new StringBuilder(PerfResult.CSV_HEADER).append('\n');
        for (PerfResult r : results) {
            csv.append(r.csv()).append('\n');
        }
        Files.writeString(dir.resolve("results.csv"), csv);
        String md = Suite.markdown(results);
        Files.writeString(dir.resolve("results.md"), md);
        out.println();
        out.println(md);
        out.println("Wrote " + dir.resolve("results.csv") + " and " + dir.resolve("results.md"));
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument " + arg);
            }
            String key = arg.substring(2);
            if (key.equals("compressible")) {
                options.put(key, "true");
            } else if (i + 1 < args.length) {
                options.put(key, args[++i]);
            } else {
                throw new IllegalArgumentException(arg + " needs a value");
            }
        }
        return options;
    }

    private static List<String> brokers(Map<String, String> options) {
        return List.of(required(options, "bootstrap").split(","));
    }

    private static String required(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null) {
            throw new IllegalArgumentException("--" + key + " is required");
        }
        return value;
    }

    private static int integer(Map<String, String> options, String key, int fallback) {
        return options.containsKey(key) ? Integer.parseInt(options.get(key)) : fallback;
    }

    static String usage() {
        return """
                usage:
                  tributary-perf suite   [--seconds 10] [--warmup 3] [--deployments single,cluster] [--out perf-results]
                                         [--scenarios throughput,latency,compression,end-to-end]
                                         [--broker-jvm "-Xmx1g ..."]
                  tributary-perf produce --bootstrap HOST:PORT[,...] --topic T [--record-size 1000] [--rate 0 (max)]
                                         [--producers 1] [--seconds 10] [--warmup 3] [--linger-ms 5]
                                         [--batch-bytes 65536] [--compression none|gzip] [--compressible]
                  tributary-perf e2e     --bootstrap HOST:PORT[,...] --topic T [--record-size 1000] [--rate 1000]
                                         [--seconds 10] [--warmup 3] [--linger-ms 5]""";
    }
}
