package io.github.karthika04.tributary.chaos;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.storage.Log;
import io.github.karthika04.tributary.storage.LogConfig;
import io.github.karthika04.tributary.storage.Record;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Three real broker processes with a {@link ChaosProxy} on each of the six one-way
 * links between them.
 *
 * <pre>
 *   broker 1 --proxy(1->2)--> broker 2        clients connect to brokers directly,
 *   broker 1 --proxy(1->3)--> broker 3        so a leader cut off from its peers can
 *   broker 2 --proxy(2->1)--> broker 1 ...    still be reached by clients
 * </pre>
 *
 * Each broker is told to reach its peers through its own proxies
 * ({@code --peer-addresses}), so cutting proxy(1->2) and proxy(2->1) partitions
 * exactly brokers 1 and 2 from each other.
 */
public final class ChaosCluster implements AutoCloseable {

    public static final List<String> IDS = List.of("1", "2", "3");

    private final Path dir;
    private final Map<String, Integer> ports = new LinkedHashMap<>();
    private final Map<String, ChaosProxy> proxies = new LinkedHashMap<>(); // "1->2"
    private final Map<String, BrokerProcess> brokers = new LinkedHashMap<>();

    /** A chaos cluster: proxies on every link, small quick-starting JVMs. */
    public ChaosCluster(Path dir) throws IOException {
        this(dir, true, BrokerProcess.CHAOS_JVM_OPTIONS);
    }

    /**
     * @param proxies    false to connect brokers directly (for benchmarks: no proxy in the
     *                   replication path, and the network faults do nothing)
     * @param jvmOptions options for each broker's JVM
     */
    public ChaosCluster(Path dir, boolean proxies, List<String> jvmOptions) throws IOException {
        this.dir = dir;
        for (String id : IDS) {
            ports.put(id, freePort());
        }
        if (proxies) {
            for (String from : IDS) {
                for (String to : IDS) {
                    if (!from.equals(to)) {
                        this.proxies.put(from + "->" + to, new ChaosProxy(ports.get(to)));
                    }
                }
            }
        }
        StringJoiner cluster = new StringJoiner(",");
        for (String id : IDS) {
            cluster.add(id + "=localhost:" + ports.get(id));
        }
        for (String id : IDS) {
            List<String> args = new ArrayList<>(List.of("--node-id", id, "--cluster", cluster.toString(),
                    "--data-dir", dir.resolve("node-" + id).toString()));
            if (proxies) {
                StringJoiner peers = new StringJoiner(",");
                for (String to : IDS) {
                    if (!to.equals(id)) {
                        peers.add(to + "=localhost:" + this.proxies.get(id + "->" + to).port());
                    }
                }
                args.addAll(List.of("--peer-addresses", peers.toString()));
            }
            Files.createDirectories(dir.resolve("node-" + id));
            brokers.put(id, new BrokerProcess(id, jvmOptions, args, dir.resolve("broker-" + id + ".log")));
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    public void start() throws IOException {
        for (BrokerProcess b : brokers.values()) {
            b.start();
        }
    }

    /** Every broker's client address, for a client's bootstrap list. */
    public List<String> addresses() {
        List<String> list = new ArrayList<>();
        for (String id : IDS) {
            list.add("localhost:" + ports.get(id));
        }
        return list;
    }

    public BrokerProcess broker(String id) {
        return brokers.get(id);
    }

    // ---- who leads? ------------------------------------------------------------------

    /** The broker that currently serves clients, or null if none answers as leader. */
    public String leader() {
        for (String id : IDS) {
            BrokerProcess b = brokers.get(id);
            if (!b.isRunning() || b.isPaused()) {
                continue;
            }
            ClientConfig config = ClientConfig.of(List.of("localhost:" + ports.get(id)))
                    .withConnectTimeoutMs(500).withRequestTimeoutMs(1_000).withMaxAttempts(1);
            try (TributaryClient probe = new TributaryClient(config)) {
                probe.listTopics();
                return id;
            } catch (TributaryException e) {
                // not the leader, or not reachable
            }
        }
        return null;
    }

    public String awaitLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            String leader = leader();
            if (leader != null) {
                return leader;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("no leader within " + timeoutMs + " ms\n" + diagnostics());
    }

    /** Which brokers are up, and the end of each one's log: what you need when a run gets stuck. */
    public String diagnostics() {
        StringBuilder out = new StringBuilder();
        for (String id : IDS) {
            BrokerProcess b = brokers.get(id);
            out.append("broker ").append(id).append(b.isRunning() ? (b.isPaused() ? " (paused)" : " (running)") : " (not running)")
                    .append(", log ").append(dir.resolve("broker-" + id + ".log")).append(":\n");
            try {
                List<String> lines = Files.readAllLines(dir.resolve("broker-" + id + ".log"));
                for (String line : lines.subList(Math.max(0, lines.size() - 8), lines.size())) {
                    out.append("    ").append(line).append('\n');
                }
            } catch (IOException e) {
                out.append("    (no log: ").append(e.getMessage()).append(")\n");
            }
        }
        return out.toString();
    }

    // ---- faults ------------------------------------------------------------------------

    /** Cuts every link to and from {@code id}, in both directions. */
    public void isolate(String id) {
        for (Map.Entry<String, ChaosProxy> e : proxies.entrySet()) {
            String[] ends = e.getKey().split("->");
            if (ends[0].equals(id) || ends[1].equals(id)) {
                e.getValue().cut();
            }
        }
    }

    public void setLatencyMs(int ms) {
        for (ChaosProxy p : proxies.values()) {
            p.setLatencyMs(ms);
        }
    }

    /** Heals the network, wakes paused brokers and restarts dead ones. */
    public void healAll() throws IOException, InterruptedException {
        for (ChaosProxy p : proxies.values()) {
            p.setLatencyMs(0);
            if (p.isCut()) {
                p.heal();
            }
        }
        for (BrokerProcess b : brokers.values()) {
            if (b.isPaused()) {
                b.resume();
            }
            if (!b.isRunning()) {
                b.start();
            }
        }
    }

    // ---- after the run -----------------------------------------------------------------

    /** Stops every broker cleanly. */
    public void stop() throws InterruptedException, IOException {
        for (BrokerProcess b : brokers.values()) {
            if (b.isPaused()) {
                b.resume();
            }
        }
        for (BrokerProcess b : brokers.values()) {
            b.stop();
        }
    }

    /**
     * Reads each broker's Raft log from disk (call after {@link #stop()}). Each entry
     * is the raw stored bytes: term then command.
     */
    public Map<String, List<byte[]>> raftLogs() {
        Map<String, List<byte[]>> logs = new LinkedHashMap<>();
        for (String id : IDS) {
            int maxRecord = 17 * 1024 * 1024 + 64;
            LogConfig config = LogConfig.defaults().withMaxRecordBytes(maxRecord)
                    .withSegmentBytes(Math.max(LogConfig.DEFAULT_SEGMENT_BYTES, maxRecord));
            List<byte[]> entries = new ArrayList<>();
            try (Log log = Log.open(dir.resolve("node-" + id).resolve("raft").resolve("log"), config)) {
                long offset = log.startOffset();
                while (offset < log.endOffset()) {
                    for (Record r : log.read(offset, 10_000)) {
                        entries.add(r.value());
                        offset = r.offset() + 1;
                    }
                }
            }
            logs.put(id, entries);
        }
        return logs;
    }

    @Override
    public void close() throws IOException {
        try {
            stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            for (ChaosProxy p : proxies.values()) {
                p.close();
            }
        }
    }
}
