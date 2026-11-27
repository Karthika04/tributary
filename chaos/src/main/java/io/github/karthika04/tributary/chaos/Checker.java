package io.github.karthika04.tributary.chaos;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks what the cluster stored against what it acknowledged, in the spirit of
 * Jepsen's queue checkers. Violations break a guarantee; the rest is information.
 *
 * <ul>
 *   <li><b>lost</b> (violation): acknowledged, but not in the partition.</li>
 *   <li><b>wrong offset</b> (violation): the offset the producer was given holds a
 *       different record.</li>
 *   <li><b>out of order</b> (violation): one producer's values appear in a different
 *       order than it sent them (counting each value's first copy).</li>
 *   <li><b>foreign</b> (violation): a value in a partition its producer never wrote to.</li>
 *   <li><b>duplicates</b> (allowed: at-least-once): extra copies from retries.</li>
 *   <li><b>failed but stored</b> (allowed): the producer gave up, but the write had
 *       committed anyway, so its retry or its next value came after it.</li>
 * </ul>
 */
public final class Checker {

    public record Result(int acknowledged, int failed, int lost, int wrongOffsets, int outOfOrder,
                         int foreign, int duplicates, int failedButStored, List<String> details) {

        public boolean ok() {
            return lost == 0 && wrongOffsets == 0 && outOfOrder == 0 && foreign == 0;
        }
    }

    private Checker() {
    }

    /** Reads every partition the workload wrote to and checks it. */
    public static Result check(Workload workload, String topic, TributaryClient client) {
        Map<Integer, List<Workload.Attempt>> attempts = new HashMap<>();
        Map<Integer, List<FetchedRecord>> stored = new HashMap<>();
        for (int producer = 0; producer < workload.producers(); producer++) {
            attempts.put(producer, workload.attempts(producer));
            stored.put(producer, readAll(client, topic, producer));
        }
        return check(attempts, stored);
    }

    /**
     * The check itself, on plain data so it can be tested: what each producer
     * attempted, and what each partition holds (producer {@code k} writes partition {@code k}).
     */
    static Result check(Map<Integer, List<Workload.Attempt>> attemptsByProducer,
                        Map<Integer, List<FetchedRecord>> storedByPartition) {
        int acknowledged = 0;
        int failed = 0;
        int lost = 0;
        int wrongOffsets = 0;
        int outOfOrder = 0;
        int foreign = 0;
        int duplicates = 0;
        int failedButStored = 0;
        List<String> details = new ArrayList<>();

        for (int producer : attemptsByProducer.keySet()) {
            List<FetchedRecord> stored = storedByPartition.getOrDefault(producer, List.of());
            Map<Long, String> byOffset = new HashMap<>();
            List<Long> firstSeen = new ArrayList<>(); // seqs in log order, first copies only
            Set<Long> seen = new HashSet<>();
            for (FetchedRecord r : stored) {
                String value = new String(r.value(), UTF_8);
                byOffset.put(r.offset(), value);
                long seq = parseSeq(value, producer);
                if (seq < 0) {
                    foreign++;
                    note(details, "foreign value " + value + " in partition " + producer + " at " + r.offset());
                    continue;
                }
                if (!seen.add(seq)) {
                    duplicates++;
                } else {
                    firstSeen.add(seq);
                }
            }
            for (int i = 1; i < firstSeen.size(); i++) {
                if (firstSeen.get(i) < firstSeen.get(i - 1)) {
                    outOfOrder++;
                    note(details, "partition " + producer + ": " + firstSeen.get(i) + " appears after "
                            + firstSeen.get(i - 1));
                }
            }
            for (Workload.Attempt a : attemptsByProducer.get(producer)) {
                String value = Workload.value(producer, a.seq());
                if (a.acknowledged()) {
                    acknowledged++;
                    if (!seen.contains(a.seq())) {
                        lost++;
                        note(details, "LOST " + value + " (acknowledged at offset " + a.offset() + ")");
                    } else if (!value.equals(byOffset.get(a.offset()))) {
                        wrongOffsets++;
                        note(details, "offset " + a.offset() + " was acknowledged for " + value
                                + " but holds " + byOffset.get(a.offset()));
                    }
                } else {
                    failed++;
                    if (seen.contains(a.seq())) {
                        failedButStored++;
                    }
                }
            }
        }
        return new Result(acknowledged, failed, lost, wrongOffsets, outOfOrder, foreign, duplicates,
                failedButStored, details);
    }

    private static void note(List<String> details, String line) {
        if (details.size() < 20) {
            details.add(line);
        }
    }

    /** The seq of {@code "p<producer>-<seq>"}, or -1 if the value is not that producer's. */
    static long parseSeq(String value, int producer) {
        String prefix = "p" + producer + "-";
        if (!value.startsWith(prefix)) {
            return -1;
        }
        try {
            return Long.parseLong(value.substring(prefix.length()));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static List<FetchedRecord> readAll(TributaryClient client, String topic, int partition) {
        List<FetchedRecord> out = new ArrayList<>();
        long offset = 0;
        while (true) {
            Response.Fetch fetch = client.fetch(topic, partition, offset, 5_000, 0, Compression.GZIP);
            if (fetch.records().isEmpty()) {
                return out;
            }
            out.addAll(fetch.records());
            offset = fetch.records().get(fetch.records().size() - 1).offset() + 1;
        }
    }

    /**
     * Raft's Log Matching property, checked on the brokers' own disks after a run: any
     * two logs agree on every entry up to the shorter one's length. (Logs may differ in
     * length: a broker can stop before receiving the last few entries.)
     *
     * @return a description of the first disagreement, or null if they all agree
     */
    public static String checkRaftLogs(Map<String, List<byte[]>> logs) {
        List<String> ids = new ArrayList<>(logs.keySet());
        for (int i = 0; i < ids.size(); i++) {
            for (int j = i + 1; j < ids.size(); j++) {
                List<byte[]> a = logs.get(ids.get(i));
                List<byte[]> b = logs.get(ids.get(j));
                for (int n = 0; n < Math.min(a.size(), b.size()); n++) {
                    if (!Arrays.equals(a.get(n), b.get(n))) {
                        return "brokers " + ids.get(i) + " and " + ids.get(j) + " disagree at Raft index " + (n + 1);
                    }
                }
            }
        }
        return null;
    }
}
