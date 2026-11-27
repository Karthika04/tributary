package io.github.karthika04.tributary.chaos;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Runs chaos rounds against real 3-broker clusters and records the results.
 *
 * <pre>
 * chaos-runner [--clusters 50] [--rounds 10] [--seed 1] [--out chaos-results]
 *              [--faults KILL_LEADER,PAUSE_LEADER,...] [--hold-ms 1500-3000] [--keep]
 * </pre>
 *
 * Each cluster lives for {@code rounds} rounds. In each round three producers write
 * to a fresh topic, one {@link Fault} is injected and later healed, and the
 * {@link Checker} compares what was acknowledged with what was stored. After the last
 * round the brokers are stopped and their Raft logs compared on disk. Every round is
 * a line in {@code results.csv}; the cluster directories (data and broker logs) of
 * failed runs are kept for debugging, the rest deleted unless {@code --keep} is given.
 */
public final class ChaosRunner {

    /** One round's outcome. */
    public record RoundResult(int cluster, int round, Fault fault, String injected, Checker.Result check,
                              long longestGapMs) {
    }

    private final Random random;
    private final PrintStream log;
    private final List<Fault> faults;
    private final int holdMinMs;
    private final int holdMaxMs;

    /**
     * @param faults    the faults to cycle through
     * @param holdMinMs shortest time a fault stays in place before the heal
     * @param holdMaxMs longest time
     */
    public ChaosRunner(long seed, PrintStream log, List<Fault> faults, int holdMinMs, int holdMaxMs) {
        if (faults.isEmpty() || holdMinMs < 0 || holdMaxMs < holdMinMs) {
            throw new IllegalArgumentException("need at least one fault and 0 <= hold min <= hold max");
        }
        this.random = new Random(seed);
        this.log = log;
        this.faults = List.copyOf(faults);
        this.holdMinMs = holdMinMs;
        this.holdMaxMs = holdMaxMs;
    }

    /** Runs one cluster for {@code rounds} rounds, cycling through the faults. */
    public List<RoundResult> runCluster(int clusterNumber, Path dir, int rounds, int firstFault)
            throws IOException, InterruptedException {
        List<RoundResult> results = new ArrayList<>();
        try (ChaosCluster cluster = new ChaosCluster(dir)) {
            cluster.start();
            cluster.awaitLeader(30_000);
            for (int round = 0; round < rounds; round++) {
                Fault fault = faults.get((firstFault + round) % faults.size());
                RoundResult r = runRound(cluster, clusterNumber, round, fault);
                results.add(r);
                log.printf("cluster %d round %d %-18s %-28s acked %4d failed %3d lost %d dup %d gap %5d ms %s%n",
                        clusterNumber, round, fault, r.injected(), r.check().acknowledged(), r.check().failed(),
                        r.check().lost(), r.check().duplicates(), r.longestGapMs(), r.check().ok() ? "ok" : "VIOLATION");
                for (String d : r.check().details()) {
                    log.println("    " + d);
                }
            }
            Thread.sleep(1_500); // let followers receive the last entries before stopping
            cluster.stop();
            String raft = Checker.checkRaftLogs(cluster.raftLogs());
            log.printf("cluster %d raft logs: %s%n", clusterNumber, raft == null ? "consistent" : "VIOLATION " + raft);
            if (raft != null) {
                results.add(new RoundResult(clusterNumber, -1, null, raft,
                        new Checker.Result(0, 0, 0, 0, 0, 1, 0, 0, List.of(raft)), 0));
            }
        }
        return results;
    }

    private RoundResult runRound(ChaosCluster cluster, int clusterNumber, int round, Fault fault)
            throws IOException, InterruptedException {
        String topic = "chaos-" + round;
        try (TributaryClient admin = new TributaryClient(ClientConfig.of(cluster.addresses())
                .withRequestTimeoutMs(5_000).withMaxAttempts(20))) {
            admin.createTopic(topic, 3);
        }
        Workload workload = new Workload(topic, 3, cluster.addresses());
        workload.start();
        Thread.sleep(800);
        String injected = fault.inject(cluster, random);
        Thread.sleep(holdMinMs + random.nextInt(holdMaxMs - holdMinMs + 1));
        cluster.healAll();
        Thread.sleep(2_000);
        workload.stop();
        cluster.awaitLeader(30_000);

        Checker.Result check;
        try (TributaryClient reader = new TributaryClient(ClientConfig.of(cluster.addresses())
                .withRequestTimeoutMs(10_000).withMaxAttempts(20))) {
            check = Checker.check(workload, topic, reader);
        }
        return new RoundResult(clusterNumber, round, fault, injected, check, longestGapMs(workload));
    }

    /** The longest stretch with no acknowledged write from any producer: how long writes were unavailable. */
    static long longestGapMs(Workload workload) {
        List<Long> acks = new ArrayList<>();
        long first = Long.MAX_VALUE;
        long last = Long.MIN_VALUE;
        for (int p = 0; p < workload.producers(); p++) {
            for (Workload.Attempt a : workload.attempts(p)) {
                first = Math.min(first, a.startedNanos());
                last = Math.max(last, a.endedNanos());
                if (a.acknowledged()) {
                    acks.add(a.endedNanos());
                }
            }
        }
        if (first == Long.MAX_VALUE) {
            return 0;
        }
        acks.add(first);
        acks.add(last);
        acks.sort(null);
        long gap = 0;
        for (int i = 1; i < acks.size(); i++) {
            gap = Math.max(gap, acks.get(i) - acks.get(i - 1));
        }
        return gap / 1_000_000;
    }

    // ---- command line ----------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        int clusters = 50;
        int rounds = 10;
        long seed = 1;
        Path out = Path.of("chaos-results");
        List<Fault> faults = List.of(Fault.values());
        int holdMin = 1_500;
        int holdMax = 3_000;
        boolean keep = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--clusters" -> clusters = Integer.parseInt(args[++i]);
                case "--rounds" -> rounds = Integer.parseInt(args[++i]);
                case "--seed" -> seed = Long.parseLong(args[++i]);
                case "--out" -> out = Path.of(args[++i]);
                case "--faults" -> faults = Stream.of(args[++i].split(",")).map(String::strip).map(Fault::valueOf).toList();
                case "--keep" -> keep = true;
                case "--hold-ms" -> {
                    String[] range = args[++i].split("-");
                    holdMin = Integer.parseInt(range[0]);
                    holdMax = Integer.parseInt(range[range.length - 1]);
                }
                default -> {
                    System.err.println("usage: chaos-runner [--clusters N] [--rounds N] [--seed N] [--out DIR]"
                            + " [--faults F1,F2] [--hold-ms MIN-MAX] [--keep]");
                    System.exit(2);
                }
            }
        }
        Files.createDirectories(out);
        Path csv = out.resolve("results.csv");
        boolean newFile = !Files.exists(csv);
        ChaosRunner runner = new ChaosRunner(seed, System.out, faults, holdMin, holdMax);
        Map<Fault, int[]> summary = new EnumMap<>(Fault.class); // rounds, violations
        int violations = 0;
        try (PrintStream rows = new PrintStream(Files.newOutputStream(csv,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND))) {
            if (newFile) {
                rows.println("seed,cluster,round,fault,injected,acknowledged,failed,lost,wrong_offsets,"
                        + "out_of_order,foreign,duplicates,failed_but_stored,longest_gap_ms,ok");
            }
            for (int c = 0; c < clusters; c++) {
                Path dir = out.resolve("seed-" + seed + "-cluster-" + c);
                List<RoundResult> results = runner.runCluster(c, dir, rounds, c);
                boolean clusterOk = true;
                for (RoundResult r : results) {
                    Checker.Result k = r.check();
                    rows.printf("%d,%d,%d,%s,\"%s\",%d,%d,%d,%d,%d,%d,%d,%d,%d,%s%n", seed, r.cluster(), r.round(),
                            r.fault() == null ? "RAFT_LOG_CHECK" : r.fault(), r.injected(), k.acknowledged(),
                            k.failed(), k.lost(), k.wrongOffsets(), k.outOfOrder(), k.foreign(), k.duplicates(),
                            k.failedButStored(), r.longestGapMs(), k.ok());
                    rows.flush();
                    if (r.fault() != null) {
                        int[] s = summary.computeIfAbsent(r.fault(), f -> new int[2]);
                        s[0]++;
                        s[1] += k.ok() ? 0 : 1;
                    }
                    if (!k.ok()) {
                        violations++;
                        clusterOk = false;
                    }
                }
                if (clusterOk && !keep) {
                    deleteRecursively(dir);
                }
            }
        }
        System.out.println();
        summary.forEach((f, s) -> System.out.printf("%-18s %4d rounds, %d violations%n", f, s[0], s[1]));
        System.out.println(violations == 0 ? "ALL ROUNDS PASSED" : violations + " VIOLATIONS");
        System.exit(violations == 0 ? 0 : 1);
    }

    static void deleteRecursively(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
