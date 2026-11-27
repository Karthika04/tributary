package io.github.karthika04.tributary.broker;

import io.github.karthika04.tributary.storage.LogConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Starts a broker from the command line:
 *
 * <pre>
 * tributary-broker [--port 9292] [--data-dir ./data] [--retention-hours 168]
 *                  [--retention-bytes N] [--segment-bytes N] [--flush-every-append]
 *                  [--node-id ID --cluster ID=host:port,ID=host:port,...]
 *                  [--peer-addresses ID=host:port,...]
 * </pre>
 *
 * Ctrl+C shuts it down gracefully.
 */
public final class BrokerMain {

    private BrokerMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerConfig config;
        try {
            config = parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(usage());
            System.exit(2);
            return;
        }
        if (config == null) {
            System.out.println(usage());
            return;
        }

        BrokerServer broker = BrokerServer.start(config);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            broker.close();
            stopped.countDown();
        }, "tributary-shutdown"));
        System.out.println("Tributary broker listening on port " + broker.port()
                + ", data in " + config.dataDir().toAbsolutePath().normalize()
                + (config.isClustered() ? ", node " + config.nodeId() + " of " + config.clusterNodes().size() : ""));
        stopped.await();
    }

    /** @return the config, or null if help was requested */
    static BrokerConfig parse(String[] args) {
        Integer port = null;
        Path dataDir = Path.of("data");
        LogConfig log = LogConfig.defaults();
        String nodeId = null;
        Map<String, String> cluster = Map.of();
        Map<String, String> peerAddresses = Map.of();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help", "-h" -> {
                    return null;
                }
                case "--port" -> port = Integer.parseInt(value(args, ++i, arg));
                case "--data-dir" -> dataDir = Path.of(value(args, ++i, arg));
                case "--retention-hours" -> log = log.withRetentionMs(
                        Duration.ofHours(Long.parseLong(value(args, ++i, arg))).toMillis());
                case "--retention-bytes" -> log = log.withRetentionBytes(Long.parseLong(value(args, ++i, arg)));
                case "--segment-bytes" -> log = log.withSegmentBytes(Integer.parseInt(value(args, ++i, arg)));
                case "--flush-every-append" -> log = log.withFlushEveryAppend(true);
                case "--node-id" -> nodeId = value(args, ++i, arg);
                case "--cluster" -> cluster = parseCluster(value(args, ++i, arg));
                case "--peer-addresses" -> peerAddresses = parseAddresses(value(args, ++i, arg));
                default -> throw new IllegalArgumentException("unknown option " + arg);
            }
        }
        if (cluster.isEmpty() != (nodeId == null)) {
            throw new IllegalArgumentException("--node-id and --cluster go together");
        }
        if (!cluster.isEmpty() && !cluster.containsKey(nodeId)) {
            throw new IllegalArgumentException("--node-id " + nodeId + " is not in --cluster");
        }
        if (port == null) {
            // A cluster member listens on the port the others know it by.
            port = cluster.isEmpty() ? BrokerConfig.DEFAULT_PORT : portOf(cluster.get(nodeId));
        }
        BrokerConfig config = BrokerConfig.defaults(dataDir).withPort(port).withLogConfig(log);
        if (cluster.isEmpty()) {
            if (!peerAddresses.isEmpty()) {
                throw new IllegalArgumentException("--peer-addresses needs --cluster");
            }
            return config;
        }
        return config.withCluster(nodeId, cluster).withPeerAddresses(peerAddresses);
    }

    /** Parses {@code 1=localhost:9292,2=localhost:9293,3=localhost:9294}: at least 3 nodes. */
    static Map<String, String> parseCluster(String spec) {
        Map<String, String> nodes = parseAddresses(spec);
        if (nodes.size() < 3) {
            // With two nodes a majority is both, so losing either stops the cluster:
            // no better than one broker, and easy to misread as fault tolerant.
            throw new IllegalArgumentException("a cluster needs at least 3 nodes, got " + nodes.size());
        }
        return nodes;
    }

    /** Parses {@code ID=host:port,ID=host:port}. */
    static Map<String, String> parseAddresses(String spec) {
        Map<String, String> nodes = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String entry = part.strip();
            int eq = entry.indexOf('=');
            if (eq <= 0 || eq == entry.length() - 1) {
                throw new IllegalArgumentException("cluster entries look like ID=host:port, got '" + entry + "'");
            }
            String id = entry.substring(0, eq).strip();
            String address = entry.substring(eq + 1).strip();
            portOf(address); // validates it
            if (nodes.put(id, address) != null) {
                throw new IllegalArgumentException("node " + id + " is listed twice");
            }
        }
        return nodes;
    }

    private static int portOf(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("address must be host:port, got '" + address + "'");
        }
        try {
            int port = Integer.parseInt(address.substring(colon + 1));
            if (port < 1 || port > 65535) {
                throw new NumberFormatException();
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad port in '" + address + "'");
        }
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " needs a value");
        }
        return args[index];
    }

    static String usage() {
        return """
                usage: tributary-broker [options]
                  --port N               TCP port (default 9292)
                  --data-dir PATH        where topic data is stored (default ./data)
                  --retention-hours N    delete data older than N hours (default: keep forever)
                  --retention-bytes N    keep at most N bytes per partition (default: no limit)
                  --segment-bytes N      segment file size (default 64 MiB)
                  --flush-every-append   force every write to disk (slower, safest)
                  --node-id ID           this broker's id in the cluster
                  --cluster LIST         run as a replicated cluster, e.g.
                                         1=localhost:9292,2=localhost:9293,3=localhost:9294
                                         (the port defaults to this node's entry)
                  --peer-addresses LIST  reach these peers at other addresses than --cluster
                                         gives (another network, or a test proxy), e.g.
                                         2=10.0.0.2:9292,3=10.0.0.3:9292
                  --help                 show this message""";
    }
}
