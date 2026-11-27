package io.github.karthika04.tributary.raft;

import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

/** A test cluster of Raft nodes on a {@link SimulatedNetwork}, with fast timeouts. */
final class Cluster implements AutoCloseable {

    final SimulatedNetwork network = new SimulatedNetwork();
    final List<String> ids = new ArrayList<>();
    final Map<String, RaftNode> nodes = new HashMap<>();
    final Map<String, RecordingStateMachine> machines = new HashMap<>();
    private final Path baseDir;

    Cluster(Path baseDir, int size) {
        this.baseDir = baseDir;
        for (int i = 1; i <= size; i++) {
            ids.add("n" + i);
        }
        ids.forEach(this::start);
    }

    /** Starts (or restarts) a node with a fresh state machine and its existing files. */
    void start(String id) {
        List<String> peers = new ArrayList<>(ids);
        peers.remove(id);
        RaftConfig config = RaftConfig.of(id, peers, baseDir.resolve(id)).withTimeouts(150, 300, 30);
        RecordingStateMachine machine = new RecordingStateMachine();
        RaftNode node = RaftNode.start(config, network.transportFor(id), machine);
        nodes.put(id, node);
        machines.put(id, machine);
        network.register(node);
    }

    /** Simulates a crash: the node vanishes from the network and stops. */
    void kill(String id) {
        network.unregister(id);
        RaftNode node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    boolean isUp(String id) {
        return nodes.containsKey(id);
    }

    /** Waits until exactly one live node considers itself a ready leader, and returns it. */
    RaftNode awaitLeader() {
        return awaitLeaderAmong(ids);
    }

    RaftNode awaitLeaderAmong(List<String> candidates) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (String id : candidates) {
                RaftNode node = nodes.get(id);
                if (node != null && node.isReadyLeader()) {
                    return node;
                }
            }
            sleep(10);
        }
        fail("no leader elected among " + candidates);
        return null;
    }

    static CompletableFuture<Object> propose(RaftNode node, String command) {
        return node.propose(command.getBytes(StandardCharsets.UTF_8));
    }

    static void await(BooleanSupplier condition, String what) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for " + what);
            }
            sleep(10);
        }
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        new ArrayList<>(nodes.keySet()).forEach(this::kill);
    }
}
