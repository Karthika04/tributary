package io.github.karthika04.tributary.broker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.karthika04.tributary.client.ClientConfig;
import io.github.karthika04.tributary.client.TributaryClient;
import io.github.karthika04.tributary.client.TributaryException;
import io.github.karthika04.tributary.protocol.Compression;
import io.github.karthika04.tributary.protocol.ErrorCode;
import io.github.karthika04.tributary.protocol.FetchedRecord;
import io.github.karthika04.tributary.protocol.PartitionOffset;
import io.github.karthika04.tributary.protocol.ProducedRecord;
import io.github.karthika04.tributary.protocol.Request;
import io.github.karthika04.tributary.protocol.Response;
import io.github.karthika04.tributary.protocol.TopicPartition;
import io.github.karthika04.tributary.raft.RaftNode;
import io.github.karthika04.tributary.storage.Record;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Three real brokers on localhost, replicating through Raft over the Tributary protocol. */
class ClusterTest {

    @TempDir
    Path dir;

    private final Map<String, String> nodes = new LinkedHashMap<>();
    private final Map<String, BrokerServer> running = new ConcurrentHashMap<>();
    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void stopEverything() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
        for (BrokerServer b : running.values()) {
            b.close();
        }
    }

    // ---- helpers -------------------------------------------------------------------

    private void startCluster() throws IOException {
        for (String id : List.of("1", "2", "3")) {
            nodes.put(id, "localhost:" + freePort());
        }
        for (String id : nodes.keySet()) {
            startBroker(id);
        }
    }

    private BrokerServer startBroker(String id) throws IOException {
        BrokerConfig config = BrokerConfig.defaults(dir.resolve("node-" + id))
                .withPort(Integer.parseInt(nodes.get(id).substring(nodes.get(id).lastIndexOf(':') + 1)))
                .withCluster(id, nodes);
        BrokerServer broker = BrokerServer.start(config);
        running.put(id, broker);
        return broker;
    }

    private void stopBroker(String id) {
        running.remove(id).close();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Waits for exactly one ready leader among the running brokers and returns its id. */
    private String awaitLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            List<String> leaders = running.entrySet().stream()
                    .filter(e -> e.getValue().raft().isReadyLeader())
                    .map(Map.Entry::getKey)
                    .toList();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no single leader elected within 10 s");
    }

    /** Waits until every running broker has applied everything the leader has committed. */
    private void awaitCaughtUp(String leaderId) throws InterruptedException {
        long target = running.get(leaderId).raft().commitIndex();
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            boolean all = running.values().stream().allMatch(b -> b.raft().lastApplied() >= target);
            if (all) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("brokers did not catch up to index " + target);
    }

    private String followerOf(String leaderId) {
        return running.keySet().stream().filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();
    }

    private TributaryClient client(String... ids) {
        List<String> addresses = new ArrayList<>();
        for (String id : ids) {
            addresses.add(nodes.get(id));
        }
        TributaryClient client = new TributaryClient(ClientConfig.of(addresses)
                .withRequestTimeoutMs(15_000).withMaxAttempts(20));
        toClose.add(client);
        return client;
    }

    private TributaryClient clientForAll() {
        return client(nodes.keySet().toArray(String[]::new));
    }

    private static List<ProducedRecord> values(String... values) {
        List<ProducedRecord> list = new ArrayList<>();
        for (String v : values) {
            list.add(new ProducedRecord(null, v.getBytes(UTF_8)));
        }
        return list;
    }

    /** What one broker has on disk for a partition, read directly (followers refuse clients). */
    private static List<String> localValues(BrokerServer broker, String topic, int partition)
            throws InterruptedException {
        Partition p = broker.topics().partition(topic, partition);
        List<String> out = new ArrayList<>();
        long offset = p.startOffset();
        while (offset < p.endOffset()) {
            List<Record> batch = p.read(offset, 1000, 0);
            for (Record r : batch) {
                out.add(new String(r.value(), UTF_8));
            }
            offset += batch.size();
        }
        return out;
    }

    private static List<String> fetchAll(TributaryClient client, String topic, int partition) {
        List<String> out = new ArrayList<>();
        long offset = 0;
        while (true) {
            Response.Fetch fetch = client.fetch(topic, partition, offset, 1000, 0, Compression.NONE);
            if (fetch.records().isEmpty()) {
                return out;
            }
            for (FetchedRecord r : fetch.records()) {
                out.add(new String(r.value(), UTF_8));
                offset = r.offset() + 1;
            }
        }
    }

    // ---- tests -----------------------------------------------------------------------

    @Test
    void everyBrokerEndsUpWithTheSameData() throws Exception {
        startCluster();
        String leader = awaitLeader();
        TributaryClient client = clientForAll();
        client.createTopic("orders", 2);
        for (int i = 0; i < 50; i++) {
            assertEquals(i, client.produce("orders", 0, values("a" + i), Compression.NONE));
            client.produce("orders", 1, values("b" + i, "c" + i), Compression.GZIP);
        }
        awaitCaughtUp(leader);

        List<String> expected0 = fetchAll(client, "orders", 0);
        List<String> expected1 = fetchAll(client, "orders", 1);
        assertEquals(50, expected0.size());
        assertEquals(100, expected1.size());
        for (BrokerServer broker : running.values()) {
            assertEquals(Map.of("orders", 2), toMap(broker.topics().metadata()));
            assertEquals(expected0, localValues(broker, "orders", 0));
            assertEquals(expected1, localValues(broker, "orders", 1));
        }

        // Each broker's metrics agree on who leads and on the term.
        double term = MetricsTest.metric(running.get(leader).scrapeMetrics(), "tributary_raft_term");
        for (Map.Entry<String, BrokerServer> e : running.entrySet()) {
            String text = e.getValue().scrapeMetrics();
            assertEquals(e.getKey().equals(leader) ? 1.0 : 0.0, MetricsTest.metric(text, "tributary_raft_leader"));
            assertEquals(term, MetricsTest.metric(text, "tributary_raft_term"));
        }
    }

    private static Map<String, Integer> toMap(List<Response.TopicMetadata> metadata) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (Response.TopicMetadata m : metadata) {
            map.put(m.name(), m.partitions());
        }
        return map;
    }

    @Test
    void aFollowerSendsClientsToTheLeader() throws Exception {
        startCluster();
        String leader = awaitLeader();
        String follower = followerOf(leader);

        // Talking to the follower directly, with no retries, shows the redirect itself.
        TributaryClient direct = new TributaryClient(ClientConfig.of(List.of(nodes.get(follower)))
                .withMaxAttempts(1));
        toClose.add(direct);
        TributaryException e = assertThrows(TributaryException.class, () -> direct.createTopic("t", 1));
        assertEquals(ErrorCode.NOT_LEADER, e.error());
        assertTrue(e.getMessage().contains("leader is " + nodes.get(leader)), e.getMessage());

        // A normal client that only knows the follower follows the hint and succeeds.
        TributaryClient client = client(follower);
        client.createTopic("t", 1);
        assertEquals(0, client.produce("t", 0, values("x"), Compression.NONE));
        assertEquals(nodes.get(leader), client.currentBroker());
    }

    @Test
    void noAcknowledgedWriteIsLostWhenTheLeaderDies() throws Exception {
        startCluster();
        String leader = awaitLeader();
        TributaryClient admin = clientForAll();
        admin.createTopic("events", 1);

        // One producer writes "0", "1", "2", ... one record at a time and remembers which
        // writes the cluster acknowledged. Halfway through, the leader is killed.
        Set<Integer> acknowledged = ConcurrentHashMap.newKeySet();
        AtomicBoolean failed = new AtomicBoolean();
        TributaryClient producer = clientForAll();
        Thread writer = Thread.ofPlatform().start(() -> {
            for (int i = 0; i < 300; i++) {
                try {
                    producer.produce("events", 0, values(Integer.toString(i)), Compression.NONE);
                    acknowledged.add(i);
                } catch (TributaryException ex) {
                    failed.set(true); // allowed: the write was not acknowledged
                }
            }
        });
        while (acknowledged.size() < 100 && writer.isAlive()) {
            Thread.sleep(5);
        }
        stopBroker(leader);
        writer.join(60_000);
        assertTrue(!writer.isAlive(), "writer did not finish");

        String newLeader = awaitLeader();
        assertNotEquals(leader, newLeader);
        List<String> stored = fetchAll(clientForAll(), "events", 0);
        Set<Integer> storedSet = new TreeSet<>();
        for (String v : stored) {
            storedSet.add(Integer.parseInt(v));
        }
        Set<Integer> lost = new TreeSet<>(acknowledged);
        lost.removeAll(storedSet);
        assertEquals(Set.of(), lost, "acknowledged writes missing after failover");
        assertTrue(acknowledged.size() >= 250, "too few writes succeeded: " + acknowledged.size()
                + (failed.get() ? " (some failed)" : ""));

        // The two surviving brokers hold identical logs.
        awaitCaughtUp(newLeader);
        for (BrokerServer broker : running.values()) {
            assertEquals(stored, localValues(broker, "events", 0));
        }
    }

    @Test
    void aRestartedBrokerCatchesUpOnWhatItMissed() throws Exception {
        startCluster();
        String leader = awaitLeader();
        String follower = followerOf(leader);
        TributaryClient client = clientForAll();
        client.createTopic("logs", 1);
        client.produce("logs", 0, values("before-1", "before-2"), Compression.NONE);
        awaitCaughtUp(leader);

        stopBroker(follower);
        client.produce("logs", 0, values("while-down-1", "while-down-2"), Compression.NONE);
        client.createTopic("created-while-down", 3);

        BrokerServer restarted = startBroker(follower);
        awaitCaughtUp(awaitLeader());
        assertEquals(List.of("before-1", "before-2", "while-down-1", "while-down-2"),
                localValues(restarted, "logs", 0));
        assertEquals(3, restarted.topics().partitionCount("created-while-down"));
    }

    @Test
    void theClusterKeepsWorkingAfterTheLeaderComesBack() throws Exception {
        startCluster();
        String leader = awaitLeader();
        TributaryClient client = clientForAll();
        client.createTopic("t", 1);
        client.produce("t", 0, values("one"), Compression.NONE);

        stopBroker(leader);
        String second = awaitLeader();
        client.produce("t", 0, values("two"), Compression.NONE);
        startBroker(leader); // the old leader rejoins as a follower
        client.produce("t", 0, values("three"), Compression.NONE);

        awaitCaughtUp(awaitLeader());
        assertNotEquals(leader, second);
        for (BrokerServer broker : running.values()) {
            assertEquals(List.of("one", "two", "three"), localValues(broker, "t", 0));
        }
    }

    @Test
    void committedGroupOffsetsSurviveFailover() throws Exception {
        startCluster();
        String leader = awaitLeader();
        TributaryClient client = clientForAll();
        client.createTopic("jobs", 2);
        Response.JoinGroup joined = client.joinGroup("workers", "", List.of("jobs"), 10_000, "range");
        client.commitOffsets("workers", joined.memberId(), List.of(
                new PartitionOffset("jobs", 0, 7),
                new PartitionOffset("jobs", 1, 3)));

        stopBroker(leader);
        awaitLeader();
        List<PartitionOffset> fetched = client.fetchOffsets("workers",
                List.of(new TopicPartition("jobs", 0), new TopicPartition("jobs", 1)));
        assertEquals(List.of(new PartitionOffset("jobs", 0, 7),
                new PartitionOffset("jobs", 1, 3)), fetched);

        // Group membership lives in the leader's memory, so the old member must rejoin
        // before it can commit again. That is the same rule as after a broker restart.
        TributaryException e = assertThrows(TributaryException.class, () -> client.commitOffsets("workers",
                joined.memberId(), List.of(new PartitionOffset("jobs", 0, 8))));
        assertEquals(ErrorCode.UNKNOWN_MEMBER, e.error());
    }

    @Test
    void writesStopWithoutAMajorityAndResumeWhenItReturns() throws Exception {
        startCluster();
        String leader = awaitLeader();
        TributaryClient client = clientForAll();
        client.createTopic("t", 1);

        String f1 = followerOf(leader);
        stopBroker(f1);
        String f2 = followerOf(leader);
        stopBroker(f2);
        // One of three brokers cannot commit anything, so nothing is acknowledged.
        TributaryClient lonely = new TributaryClient(ClientConfig.of(List.of(nodes.get(leader)))
                .withRequestTimeoutMs(15_000).withMaxAttempts(1));
        toClose.add(lonely);
        TributaryException e = assertThrows(TributaryException.class,
                () -> lonely.send(new Request.Produce("t", 0, Compression.NONE, values("lost?"))));
        assertTrue(e.error().retriable(), e.error().toString());

        startBroker(f1);
        awaitLeader();
        // The unacknowledged write may or may not have committed (at-least-once), but the
        // cluster accepts new writes again as soon as a majority is back.
        client.produce("t", 0, values("back"), Compression.NONE);
        List<String> stored = fetchAll(client, "t", 0);
        assertEquals("back", stored.get(stored.size() - 1));
    }
}
